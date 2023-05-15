# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import copy
from typing import Optional

from common.utils import Utils
from metadata.catalog import Catalog, Index
from metadata.metadata_utils import MetaDataUtils
from optimizer.formatter import format_sql
from optimizer.optimizer_enum import IndexType
from parser.parser_utils import ParserUtils
from .cbo.cbo_optimizer import CBOOptimizer
from .oceanbase_engine import OceanBaseEngine
from .prunning_rule.index_prunning import CompareStat, IndexPrunning
from .rewrite_rule.rewrite_result import RewriteResult


class Optimizer(object):

    def optimize(self, sql, catalog: Optional[Catalog], engine=OceanBaseEngine()):

        index_optimization_recommendation_list = []

        # remove annotation in sql
        sql = Utils.remove_sql_text_affects_parser(sql)

        statement = engine.parse(sql)
        after_sql_rewrite, rewrite_rule_explanation_list = engine.rewrite(statement, catalog)
        development_specification_recommendation_list = engine.pmd(copy.deepcopy(statement))
        after_sql_rewrite_format = format_sql(after_sql_rewrite, 0) if rewrite_rule_explanation_list else None

        rewrite_result = RewriteResult(after_sql_rewrite_format, rewrite_rule_explanation_list)

        visitor = ParserUtils.format_statement(statement)

        table_list = visitor.table_list

        index_list = []
        selectivity_list = []
        min_selectivity = None
        table_rows = 0

        if catalog:
            index_list = self.format_catalog_index_list(catalog, visitor)

        for _table in table_list:
            pruning_index_list = []
            candidate_index_list = []
            _index_list = list(filter(lambda x: (x.table_name == _table['table_name']), index_list))
            for i, _left in enumerate(_index_list):
                if i == len(_index_list):
                    break
                # It has been prunning, no need to judge
                if _left.index_name in pruning_index_list:
                    continue
                for _right in _index_list[i + 1:]:
                    if _right.index_name in pruning_index_list:
                        continue
                    compare_stat = IndexPrunning(_left, _right, len(_table['filter_column_list'])).skyline_compare()
                    if compare_stat == CompareStat.LEFT_DOMINATED:
                        pruning_index_list.append(_right.index_name)
                    elif compare_stat == CompareStat.RIGHT_DOMINATED:
                        pruning_index_list.append(_left.index_name)

            for _index in _index_list:
                index_name = _index.index_name
                if index_name not in pruning_index_list:
                    candidate_index_list.append(_index)

            heuristic_rule_return_result = engine.rbo(statement, candidate_index_list)
            # TODO(tingkai.ztk):返回格式要改
            if heuristic_rule_return_result and heuristic_rule_return_result.index_name:
                index_optimization_recommendation_list.append({
                    'index_recommendation': self.index_optimization_recommendation_return_format(
                        heuristic_rule_return_result.index_name, heuristic_rule_return_result.index_column_list),
                    'diagnosis_reason': self.get_diagnosis_reason_by_index(heuristic_rule_return_result.index_name,
                                                                           index_list)
                })
            else:
                statistics_list = catalog.statistics_list if catalog else []
                table_list = catalog.table_list if catalog else []
                for _statistics in statistics_list:
                    if _statistics.table_name == _table['table_name']:
                        selectivity_list = _statistics.selectivity_list
                        break
                for catalog_table in table_list:
                    if catalog_table.table_name == _table['table_name']:
                        table_rows = catalog_table.table_rows
                        break

                recommend_index, recommend_index_column, min_selectivity = CBOOptimizer().get_cbo_index(
                    candidate_index_list, visitor,
                    _table['filter_column_list'],
                    selectivity_list, table_rows)

                if recommend_index:
                    index_optimization_recommendation_list.append(
                        {'index_recommendation':
                             self.index_optimization_recommendation_return_format(recommend_index,
                                                                                  recommend_index_column),
                         'diagnosis_reason': self.get_diagnosis_reason_by_index(recommend_index, index_list)})

            # Check whether the existing index has index_all_match, if not, need to add a new index
            is_index_all_match = False
            for _index in _index_list:
                if _index.index_all_match:
                    is_index_all_match = True
                    break

            # Need to add a better query range index
            if not is_index_all_match:
                index_name, column_list = self.add_index(_table['filter_column_list'], visitor.order_list)
                index_recommendation = self.add_index_return_format(_table['table_name'], index_name, column_list)

                if not column_list:
                    continue

                if not min_selectivity:
                    diagnosis_reason = 'This is a better query range index'
                    index_optimization_recommendation_list.append(
                        {'index_recommendation': index_recommendation, 'diagnosis_reason': diagnosis_reason})
                else:
                    new_index = Index(index_name, column_list, IndexType.PRIMARY)

                    new_index = self.format_index(new_index, _table['filter_column_list'],
                                                  visitor.projection_column_list,
                                                  visitor.order_list, visitor.min_max_list, _table['table_name'])

                    _selectivity_dict = {}
                    for selectivity in selectivity_list:
                        _selectivity_dict[selectivity.column_name] = selectivity.ndv

                    new_index_selectivity = CBOOptimizer().calculate_selectivity(new_index, visitor,
                                                                                 _table['filter_column_list'],
                                                                                 _selectivity_dict, table_rows)

                    if not new_index_selectivity:
                        diagnosis_reason = 'This is a better query range index , but due to lack of statistics, ' \
                                           'it is not possible to calculate the specific improved performance'
                        index_optimization_recommendation_list.append(
                            {'index_recommendation': index_recommendation, 'diagnosis_reason': diagnosis_reason})
                    elif new_index_selectivity < min_selectivity:
                        diagnosis_reason = 'This new index is expected to improve performance by {:.2%} percent'.format(
                            (min_selectivity - new_index_selectivity) / min_selectivity)
                        index_optimization_recommendation_list.append(
                            {'index_recommendation': index_recommendation, 'diagnosis_reason': diagnosis_reason})

        return index_optimization_recommendation_list, development_specification_recommendation_list, rewrite_result

    def format_catalog_index_list(self, catalog: Catalog, visitor):
        """
        Index formatting, calculating index_back, query_range and others
        :param catalog:
        :param visitor
        :return:
        """
        table_list = visitor.table_list
        projection_column_list = visitor.projection_column_list
        order_list = visitor.order_list
        min_max_list = visitor.min_max_list

        index_list = []
        for _schema in catalog.table_list:
            _table_name = _schema.table_name
            _index_list = _schema.index_list
            for _table in table_list:
                filter_column_list = _table['filter_column_list']
                if _table['table_name'] == _table_name:
                    for _index in _index_list:
                        _index = self.format_index(_index, filter_column_list, projection_column_list, order_list,
                                                   min_max_list,
                                                   _table_name)
                        index_list.append(_index)

        return index_list

    def index_optimization_recommendation_return_format(self, index_name, index_column):
        return 'Among the existing indexes, the optimal index is: {index_name}({index_column})'.format(
            index_name=index_name, index_column=index_column)

    def add_index(self, filter_column_list, order_list):
        column_list = MetaDataUtils.extension_all_match_index(filter_column_list, order_list)
        index_name = 'idx_sqless_{_index_name}'.format(_index_name='_'.join(column_list))
        return index_name, column_list

    def add_index_return_format(self, table_name, index_name, column_list):
        return 'alter table {table_name} add index {index_name}({column_str})'.format(table_name=table_name,
                                                                                      index_name=index_name,
                                                                                      column_str=','.join(
                                                                                          column_list))

    def get_diagnosis_reason_by_index(self, index_name, index_list):
        for _index in index_list:
            if _index.index_name == index_name:
                return 'Query Range : {query_range} , Index Back : {is_index_back} , ' \
                       'Interesting Order : {interesting_order}'.format(query_range=_index.extract_range,
                                                                        is_index_back=_index.index_back,
                                                                        interesting_order=_index.has_interesting_order)

    def format_index(self, index: Index, filter_column_list, projection_column_list, order_list, min_max_list,
                     table_name):

        is_index_back = MetaDataUtils.is_index_back(index.column_list, filter_column_list,
                                                    projection_column_list,
                                                    order_list, index.index_type)
        extract_range = MetaDataUtils.extract_range(index.column_list, filter_column_list)
        index_all_match = MetaDataUtils.index_all_match(index.column_list, filter_column_list)
        has_interesting_order = MetaDataUtils.has_interesting_order(index.column_list, order_list,
                                                                    min_max_list, extract_range,
                                                                    filter_column_list)
        index.index_back = is_index_back
        index.extract_range = extract_range
        index.has_interesting_order = has_interesting_order
        index.table_name = table_name
        index.index_all_match = index_all_match

        return index
