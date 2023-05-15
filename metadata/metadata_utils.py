# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import itertools
from copy import deepcopy
from typing import List

from metadata.catalog import Catalog, Statistics, Selectivity, Table, Index, Column
from optimizer.optimizer_enum import IndexType, OptType
from parser.mysql_parser import parser


class MetaDataUtils(object):

    @staticmethod
    def schema_sql_to_catalog_index(schema_sql):
        catalog_index_list = []
        catalog_column_list = []
        catalog_statistics_list = []
        catalog_table_list = []
        for ddl_sql in schema_sql.split(';'):
            if not ddl_sql:
                break
            res = parser.parse(ddl_sql)
            table_name = res['table_name']
            element_list = res['element_list']
            index_list = res['index_list']
            for (_index_type, _index_name, _index_column_list) in index_list:
                catalog_index_list.append(Index(_index_name, _index_column_list, _index_type))

            for (_column_name, _column_type, _nullable) in element_list:
                catalog_column_list.append(Column(_column_name, _column_type, _nullable))

            catalog_statistics_list.append(Statistics(None, table_name, []))
            catalog_table_list.append(Table(None, table_name, catalog_column_list, catalog_index_list, None))

        return Catalog(catalog_table_list, catalog_statistics_list)

    @staticmethod
    def json_to_catalog(catalog_json, schema_sql=None) -> Catalog:
        """
        convert json to catalog
        :param catalog_json:
        :param schema_sql:
        :return:
        """

        catalog_table_list = []
        catalog_statistics_list = []
        catalog_table_list_manually = []

        # manual input
        if 'manually_statistics' in catalog_json:
            statistics_list = catalog_json['manually_statistics']
            data_list = sorted(statistics_list, key=lambda item: (item['schema'], item['table']))
            for (schema, table), group_data in itertools.groupby(data_list,
                                                                 key=lambda item: (item['schema'], item['table'])):
                catalog_selectivity_list = []
                catalog_column_list = []
                catalog_index_list = []
                for per_plan in group_data:
                    catalog_selectivity_list.append(Selectivity(per_plan['name'], None, None, per_plan['cardinality']))
                    catalog_column_list.append(Column(per_plan['name'], None, None))
                catalog_table_list_manually.append(Table(schema, table, catalog_column_list, catalog_index_list, 100))
                catalog_statistics_list.append(Statistics(schema, table, catalog_selectivity_list))
            if schema_sql:
                catalog_index_list = []
                catalog_column_list = []
                for ddl_sql in schema_sql.split(';'):
                    if not ddl_sql:
                        break
                    res = parser.parse(ddl_sql)
                    table_name = res['table_name']
                    element_list = res['element_list']
                    index_list = res['index_list']
                    for (_index_type, _index_name, _index_column_list) in index_list:
                        catalog_index_list.append(Index(_index_name, _index_column_list, _index_type))

                    for (_column_name, _column_type, _nullable) in element_list:
                        catalog_column_list.append(Column(_column_name, _column_type, _nullable))
                    catalog_table_list.append(Table(None, table_name, catalog_column_list, catalog_index_list, 100))

            else:
                catalog_table_list.extend(catalog_table_list_manually)
        else:
            table_list = catalog_json['tables'] if 'tables' in catalog_json else []
            index_list = catalog_json['indexes'] if 'indexes' in catalog_json else []
            column_list = catalog_json['columns'] if 'columns' in catalog_json else []

            for table in table_list:

                catalog_column_list = []
                catalog_index_list = []
                catalog_selectivity_list = []

                table_name = table['table']
                schema_name = table['schema']
                rows = table['rows']
                schema_index_list = list(filter(lambda x: (x['table'] == table_name), index_list))
                schema_column_list = list(filter(lambda x: (x['table'] == table_name), column_list))
                first_uk = False
                column_set = set()
                index_name_set = set()
                for index in schema_index_list:
                    if index['column'] not in column_set:
                        column_set.add(index['column'])
                        catalog_selectivity_list.append(Selectivity(index['column'], None, None, index['cardinality']))

                    if index['name'] not in index_name_set:
                        index_name_set.add(index['name'])
                        index_list_filter_by_name = list(
                            filter(lambda x: (x['name'] == index['name']), schema_index_list))

                        _index_unique = index_list_filter_by_name[0]['unique']
                        _index_name = index_list_filter_by_name[0]['name']
                        _index_column_list = []
                        _index_type = ''
                        if _index_unique and not first_uk:
                            first_uk = True
                            _index_type = IndexType.PRIMARY
                        elif _index_unique and first_uk:
                            _index_type = IndexType.UNIQUE
                        else:
                            _index_type = IndexType.NORMAL

                        for _index in index_list_filter_by_name:
                            _index_column_list.append(_index['column'])

                        catalog_index_list.append(Index(_index_name, _index_column_list, _index_type))

                for column in schema_column_list:
                    catalog_column_list.append(Column(column['name'], column['type'], column['nullable']))

                catalog_statistics_list.append(Statistics(schema_name, table_name, catalog_selectivity_list))
                catalog_table_list.append(Table(schema_name, table_name, catalog_column_list, catalog_index_list, rows))

        return Catalog(catalog_table_list, catalog_statistics_list)

    @staticmethod
    def extension_all_match_index(filter_column_list, order_list):
        column_list = []
        range_flag = False
        last_column = ''
        for filter_column in filter_column_list:
            column_name = filter_column['column_name']
            opt = filter_column['opt']
            if opt == '=' or opt == 'in' or opt == 'is':
                column_list.append(column_name)
            elif opt == '>' or opt == '<' or opt == '>=' or opt == '<=' or opt == 'between' or opt == 'like_prefix':
                if not range_flag:
                    last_column = column_name
                    range_flag = True

        if range_flag:
            column_list.append(last_column)

        # TODO Statistics need to be added to determine which one comes first when there are range queries and sorting
        if not range_flag:
            ordering = ''
            for _order in order_list:
                _column_name = _order['column_name']
                _ordering = _order['ordering']
                if not ordering:
                    ordering = _ordering
                    if _column_name not in column_list:
                        column_list.append(_column_name)
                else:
                    if ordering == _ordering:
                        if _column_name not in column_list:
                            column_list.append(_column_name)
                    else:
                        break

        # add the remaining columns for not index back
        for filter_column in filter_column_list:
            column_name = filter_column['column_name']
            if column_name not in column_list:
                column_list.append(column_name)

        for _order in order_list:
            _column_name = _order['column_name']
            if _column_name not in column_list:
                column_list.append(_column_name)

        return column_list

    @staticmethod
    def is_index_back(idx_column_list: List, filter_column_list: List, projection_column_list: List,
                      order_list: List, index_type: IndexType) -> bool:
        """
        judging whether to index back, here does not involve the rows of index back
        :param order_list
        :param projection_column_list
        :param idx_column_list
        :param filter_column_list
        :param index_type
        :return: True means index back, otherwise, False.
        """

        if index_type.value == IndexType.PRIMARY.value:
            return False

        # Is it a covering index
        # select *: projection_column_list == ['*']
        # insert/update: projection_column_list == []
        if not set(projection_column_list).issubset(set(idx_column_list)):
            return True

        filter_column_name_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])

        if not set(filter_column_name_list).issubset(set(idx_column_list)):
            return True

        if not set(order_list).issubset(set(idx_column_list)):
            return True

        return False

    @staticmethod
    def extract_range(idx_column_list: List, filter_column_list: List) -> List:
        """
        according to each index, extract the largest query range
        :param idx_column_list:
        :param filter_column_list:
        :return: extract query range，exsample: ['a','b']
        """

        filter_column_name_list = []
        filter_column_opt_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])
            filter_column_opt_list.append(filter_column['opt'])

        _range_list = []
        for index_column in idx_column_list:
            # orderly
            # temporary only the first matched variable a > 0 and a = -1 is considered.
            # in fact, a = -1 is not considered here.
            try:
                i = filter_column_name_list.index(index_column)
            except Exception as e:
                # not found
                return _range_list

            # operator
            # ['=', '!=', '>', '<', '>=', '<=', '<>', 'not', 'in', 'like',  'exists', 'is', 'between']
            opt = filter_column_opt_list[i]
            if opt == '=' or opt == 'in' or opt == 'is':
                _range_list.append(index_column)
                continue
            elif opt == '>' or opt == '<' or opt == '>=' or opt == '<=' or opt == 'between' or opt == 'like':
                _range_list.append(index_column)
                return _range_list
            else:
                return _range_list

        return _range_list

    @staticmethod
    def has_interesting_order(idx_column_list: List, order_list: List, min_max_list: List,
                              extract_range_list: List, filter_column_list: List) -> bool:
        """
        interesting order
        :param idx_column_list:
        :param order_list:
        :param min_max_list
        :param extract_range_list
        :param filter_column_list
        :return:
        """

        filter_column_name_list = []
        filter_column_opt_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])
            filter_column_opt_list.append(filter_column['opt'])

        _order_list = []
        for _order in order_list:
            _order_list.append(_order['column_name'])

        if not idx_column_list and (not _order_list or not min_max_list):
            return False

        remain_list = []

        for column in idx_column_list:
            if column not in extract_range_list:
                remain_list.append(column)

        if min_max_list:
            # max(b) where a = ?          index: a,b
            if remain_list and remain_list[0] in min_max_list:
                return True

            # max(a) where a > ?          index: a
            if extract_range_list and extract_range_list[-1] in min_max_list:
                return True

        if _order_list:
            idx_str = ','.join(idx_column_list)
            order_str = ','.join(_order_list)
            remain_str = ','.join(remain_list)

            # where a = ? order by b,c           index: b,c
            if idx_str.startswith(order_str):
                return True

            # where a = ? and b = ? order by b           index: a,b
            if extract_range_list and extract_range_list[-1] in _order_list:
                flag = True
                for extract_range_column in extract_range_list[:-1]:
                    opt_type = MetaDataUtils.get_column_opt_for_cost(extract_range_column,
                                                                     filter_column_name_list,
                                                                     filter_column_opt_list)
                    if opt_type != OptType.EQUAL:
                        flag = False
                        break
                if flag:
                    return True

            # where a = ? order by b           index: a,b
            # where a > ? order by b           index: a,b
            # where a = ? order by b           index: a,b,c
            if extract_range_list:
                opt_type = MetaDataUtils.get_column_opt_for_cost(extract_range_list[-1],
                                                                 filter_column_name_list,
                                                                 filter_column_opt_list)
                if opt_type == OptType.EQUAL:
                    if remain_str.startswith(order_str):
                        return True
                    if remain_list and remain_list[0] == _order_list[0]:
                        return True

        return False

    @staticmethod
    def index_all_match(idx_column_list: List, filter_column_list: List) -> bool:
        """
        determine whether the columns of the index are all =, in (same variable or), exist
        :param idx_column_list: the index column here cannot be added with the primary key column
        :param filter_column_list:
        :return:
        """

        filter_column_name_list = []
        filter_column_opt_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])
            filter_column_opt_list.append(filter_column['opt'])

        if not filter_column_name_list or not filter_column_opt_list or not idx_column_list:
            return False

        for _index_column in idx_column_list:
            if _index_column not in filter_column_name_list:
                return False
            else:
                for _i, _filter_column in enumerate(filter_column_name_list):
                    if _index_column == _filter_column:
                        # TODO: Consider the case of exist
                        # Determine whether the index column is all =/in/is in the filter
                        if filter_column_opt_list[_i] != '=' and filter_column_opt_list[_i] != 'in' and \
                                filter_column_opt_list[_i] != 'is':
                            return False

        return True

    @staticmethod
    def get_opt_by_column_name(column_name, filter_column_list, filter_column_opt_list):
        """
        find the operator of the first column_name in the where statement by column_name
        :param column_name
        :param filter_column_list:
        :param filter_column_opt_list:
        :return:
        """
        try:
            i = filter_column_list.index(column_name)
            opt = filter_column_opt_list[i]
            return opt
        except:
            pass

        return None

    @staticmethod
    def get_column_opt_for_cost(column_name, filter_column_list, filter_column_opt_list) -> OptType:
        """
        via the column operator. Used to determine the cost calculation method
        :param column_name:
        :param filter_column_list:
        :param filter_column_opt_list:
        :return:
        """
        opt_type = OptType.UNKNOW
        # determine how many times the field appears in the filter field
        cnt = filter_column_list.count(column_name)
        # if the filter column is only 1
        if cnt == 1:
            pos = filter_column_list.index(column_name)
            opt = filter_column_opt_list[pos]
            if opt == 'in':
                opt_type = OptType.IN
            elif opt == '=' or opt == 'is':
                opt_type = OptType.EQUAL
            elif opt == 'like_prefix' or opt == 'between':
                opt_type = OptType.CLOSED_RANGE_EQUAL_ALL
            elif opt == '<=' or opt == '>=':
                opt_type = OptType.HALF_OPEN_RANGE_EQUAL
            elif opt == '<' or opt == '>':
                opt_type = OptType.HALF_OPEN_RANGE
            elif opt == '!=' or opt == 'not':
                opt_type = OptType.NOT_EQUAL
            else:
                opt_type = OptType.OTHER
        elif cnt > 1:
            tmp_cols = deepcopy(filter_column_list)
            tmp_opt = deepcopy(filter_column_opt_list)
            # range operator entered before mark
            lt = False
            le = False
            ge = False
            gt = False
            # loop through fields one by one, judge the same as above
            while cnt > 0:
                pos = tmp_cols.index(column_name)
                opt = tmp_opt[pos]
                # for the same variable, an operator with a smaller range should be selected to calculate the cost
                # for example a > ? and a = ?, calculate according to a =
                if opt == '=':
                    opt_type = OptType.EQUAL
                    return opt_type
                elif opt == 'in':
                    opt_type = OptType.IN
                elif opt == '<' and opt_type.value <= OptType.CLOSED_RANGE.value:
                    lt = True
                    if gt:
                        opt_type = OptType.CLOSED_RANGE
                    elif ge and opt_type.value <= OptType.CLOSED_RANGE_HALF_EQUAL.value:
                        opt_type = OptType.CLOSED_RANGE_HALF_EQUAL
                    else:
                        opt_type = OptType.HALF_OPEN_RANGE
                elif opt == '>' and opt_type.value <= OptType.CLOSED_RANGE.value:
                    gt = True
                    if lt:
                        opt_type = OptType.CLOSED_RANGE
                    elif le and opt_type.value <= OptType.CLOSED_RANGE_HALF_EQUAL.value:
                        opt_type = OptType.CLOSED_RANGE_HALF_EQUAL
                    else:
                        opt_type = OptType.HALF_OPEN_RANGE
                elif opt == '<=' and opt_type.value <= OptType.CLOSED_RANGE_HALF_EQUAL.value:
                    le = True
                    if gt:
                        opt_type = OptType.CLOSED_RANGE_HALF_EQUAL
                    elif ge and opt_type.value <= OptType.CLOSED_RANGE_EQUAL_ALL.value:
                        opt_type = OptType.CLOSED_RANGE_EQUAL_ALL
                    else:
                        opt_type = OptType.HALF_OPEN_RANGE_EQUAL
                elif opt == '>=' and opt_type.value <= OptType.CLOSED_RANGE_HALF_EQUAL.value:
                    ge = True
                    if lt:
                        opt_type = OptType.CLOSED_RANGE_HALF_EQUAL
                    elif le and opt_type.value <= OptType.CLOSED_RANGE_EQUAL_ALL.value:
                        opt_type = OptType.CLOSED_RANGE_EQUAL_ALL
                    else:
                        opt_type = OptType.HALF_OPEN_RANGE_EQUAL
                elif (opt == 'like_prefix' or opt == 'between') \
                        and opt_type.value <= OptType.CLOSED_RANGE_EQUAL_ALL.value:
                    opt_type = OptType.CLOSED_RANGE_EQUAL_ALL
                elif (opt == '!=' or opt == 'not') \
                        and opt_type.value <= OptType.NOT_EQUAL.value:
                    opt_type = OptType.NOT_EQUAL
                else:
                    opt_type = OptType.OTHER

                del tmp_cols[pos]
                del tmp_opt[pos]
                cnt = cnt - 1
        return opt_type
