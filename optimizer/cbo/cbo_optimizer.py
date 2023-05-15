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
import math
import os
import sys
from copy import deepcopy

from common.logger import Logger
from metadata.catalog import Index
from metadata.metadata_utils import MetaDataUtils
from optimizer.optimizer_enum import IndexType, OptType

logfile = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(logfile)


class CBOOptimizer:

    def get_cbo_index(self, candidate_index_list, visitor, filter_column_list, selectivity_list, table_rows):
        """

        :param candidate_index_list:
        :param visitor:
        :param filter_column_list:
        :param selectivity_list:
        :param table_rows:
        :return: index_name, min_selectivity
        """

        recommend_index = None
        min_selectivity = None
        recommend_index_column = None

        if not candidate_index_list:
            return recommend_index, recommend_index_column, min_selectivity

        # if there is only one index candidate by rbo, this index is directly recommended
        if len(candidate_index_list) == 1:
            recommend_index = candidate_index_list[0].index_name
            recommend_index_column = ','.join(candidate_index_list[0].column_list)

        _selectivity_dict = {}
        for selectivity in selectivity_list:
            if selectivity.ndv > 0:
                _selectivity_dict[selectivity.column_name] = selectivity.ndv

        if not _selectivity_dict or not table_rows:
            recommend_index, recommend_index_column = self.get_recommend_index_without_statistics(candidate_index_list,
                                                                                                  filter_column_list)

        if not recommend_index:
            min_selectivity = sys.maxsize
            recommend_index_column_count = sys.maxsize

            for index_dict in candidate_index_list:
                index_name = index_dict.index_name
                index_column_list = index_dict.column_list
                column_count = index_dict.column_count

                selectivity = self.calculate_selectivity(index_dict, visitor, filter_column_list, _selectivity_dict,
                                                         table_rows)
                if selectivity and selectivity <= min_selectivity:
                    if selectivity < min_selectivity or (
                            selectivity == min_selectivity and column_count < recommend_index_column_count):
                        recommend_index = index_name
                        recommend_index_column = ','.join(index_column_list)
                        min_selectivity = selectivity
                        recommend_index_column_count = column_count

        return recommend_index, recommend_index_column, min_selectivity

    def calculate_selectivity(self, index: Index, visitor, filter_column_list, selectivity_dict, table_rows):
        """
        Calculate the selectivity of an index
        selectivity = the proportion of eligible data in the total data

        equivalent case：
        selectivity = 1 / ndv
        cost = selectivity * row_count
        cost = query_range cost + 10 * index_back cost

        Since it is the same table, the table rows are the same
        Just consider selectivity

        When there are multiple columns, if there is no multi-column ndv
        Then suppose that ndv(a) = 8 and ndv(b) = 4.
        Since do not know the correlation between a and b,
        think that a and b are independent events, then ndv(a, b) = 8 * 4
        :param index:
        :param visitor:
        :param filter_column_list:
        :param selectivity_dict:
        :param table_rows:
        :return:
        """

        in_count_list = visitor.in_count_list
        limit_number = visitor.limit_number
        order_list = visitor.order_list

        filter_column_name_list = []
        filter_column_opt_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])
            filter_column_opt_list.append(filter_column['opt'])

        column_list = index.column_list
        extract_range = index.extract_range
        index_back = index.index_back
        index_type = index.index_type
        has_interesting_order = index.has_interesting_order
        query_range_selectivity = 1
        index_back_selectivity = 0
        interesting_order_cost = 0

        # determine whether the filter column is fully covered by the index,
        # if it is has limit, then index back rows == limit number
        is_index_cover_all_column = True

        if limit_number:
            for column in filter_column_name_list:
                if column not in column_list:
                    is_index_cover_all_column = False

        if not has_interesting_order and not extract_range and index_type != IndexType.PRIMARY.value:
            # If this index does not have extract_range and can not eliminate sorting,
            # return max directly, not as primary key
            return sys.maxsize

        # subscript of in_count_list
        in_count = 0
        # calculate the selectivity of query_range
        if extract_range:
            # coefficient of in = value of in_count
            in_factor = 1
            opt_type = None
            for column in extract_range:
                # the last extract_range column operator
                opt_type = MetaDataUtils.get_column_opt_for_cost(column, filter_column_name_list,
                                                                 filter_column_opt_list)
                if opt_type == OptType.IN and len(in_count_list) > in_count:
                    in_factor = in_factor * in_count_list[in_count]
                    in_count += 1
            # at this point opt_type is already the last column of extract_range
            if opt_type == OptType.EQUAL or opt_type == OptType.IN:
                ndv = self.calculate_multi_col_ndv(extract_range, selectivity_dict)
                # if there is no single-column ndv,
                # it will not be calculated directly and handed over to the subsequent logic
                if not ndv:
                    return None
                query_range_selectivity = 1 / ndv * in_factor
            else:
                ndv = self.calculate_multi_col_ndv(extract_range[:-1], selectivity_dict)
                last_column = extract_range[-1]
                if not ndv and extract_range[:-1]:
                    return None
                if ndv:
                    query_range_selectivity = 1 / ndv * in_factor * self.get_selectivity_by_opt_type(opt_type,
                                                                                                     selectivity_dict,
                                                                                                     last_column)
                else:
                    query_range_selectivity = self.get_selectivity_by_opt_type(opt_type, selectivity_dict, last_column)

        # calculate the selectivity of index back
        if index_back:
            remain_column_list = list(set(column_list) - set(extract_range))
            if not remain_column_list:
                # in theory, this will not be the case,
                # because since the return table already exists,
                # the remain_column_list will not be empty.
                # The processing here is that there is an exception in the previous return table function.
                return query_range_selectivity
            # = and in column
            remain_equal_in_list = []
            other_list = []
            # coefficient of in = value of in_count
            in_factor = 1
            for remain_column in remain_column_list:
                opt_type = MetaDataUtils.get_column_opt_for_cost(remain_column, filter_column_name_list,
                                                                 filter_column_opt_list)

                if opt_type == OptType.EQUAL or opt_type == OptType.IN:
                    if opt_type == OptType.IN and len(in_count_list) > in_count:
                        in_factor = in_factor * in_count_list[in_count]
                        in_count = in_count + 1
                    remain_equal_in_list.append(remain_column)
                else:
                    other_list.append(remain_column)
            # = and in ndv
            ndv = self.calculate_multi_col_ndv(remain_equal_in_list, selectivity_dict)
            if ndv:
                if query_range_selectivity:
                    index_back_selectivity = query_range_selectivity * (1 / ndv) * in_factor
                else:
                    # normally query_range_selectivity != 0
                    index_back_selectivity = (1 / ndv) * in_factor
            else:
                index_back_selectivity = query_range_selectivity

            for other_column in other_list:
                opt_type = MetaDataUtils.get_column_opt_for_cost(other_column, filter_column_name_list,
                                                                 filter_column_opt_list)
                selectivity = self.get_selectivity_by_opt_type(opt_type, selectivity_dict, other_column)
                if index_back_selectivity:
                    index_back_selectivity = index_back_selectivity * selectivity
                else:
                    index_back_selectivity = query_range_selectivity * selectivity
        # The index fully covers the where condition, no order by
        if limit_number and is_index_cover_all_column and table_rows and not order_list:
            index_back_selectivity_temp = limit_number / table_rows
            if index_back_selectivity_temp < index_back_selectivity:
                index_back_selectivity = index_back_selectivity_temp

        # where a = ? and c = ? and d = ? order by b desc limit 1
        # index(a,b) , the number of index back ranges from [1 ~ query_range(a)].
        # It is difficult to estimate the number of index back after limit down.
        # Currently, it is taken as the optimal value.
        if limit_number and order_list and has_interesting_order and table_rows:
            index_back_selectivity = limit_number / table_rows
            # There is no need to care about the scope of the query here,
            # because even if a has no filterability, it may return successfully once
            query_range_selectivity = 0

        if not has_interesting_order:
            interesting_order_cost = query_range_selectivity * 0.01

        return query_range_selectivity + 10 * index_back_selectivity + interesting_order_cost

    def get_selectivity_by_opt_type(self, opt_type, selectivity_dict, column_name):
        """
        calculate selectivity based on opt_type
        :param opt_type:
        :param selectivity_dict:
        :param column_name
        :return:
        """

        if opt_type == OptType.UNKNOW:
            return 1

        # guessed selectivity, derived from OceanBase
        # a > ?
        ob_default_half_open_range_sel = 0.1
        # a > ? and a < ?
        ob_default_closed_range_sel = 0.05
        # default
        default_sel = 0.5

        selectivity = default_sel
        if opt_type == OptType.CLOSED_RANGE:
            selectivity = ob_default_closed_range_sel
        elif opt_type == OptType.CLOSED_RANGE_HALF_EQUAL:
            # a > ? and a <= ? selectivity = a > ? and a < ? + 1 / ndv(a)
            if selectivity_dict.get(column_name):
                selectivity = ob_default_closed_range_sel + 1 / selectivity_dict.get(column_name)
            else:
                # no ndv is handled as a > ? and a < ?
                selectivity = ob_default_closed_range_sel
        elif opt_type == OptType.CLOSED_RANGE_EQUAL_ALL:
            # a >= ? and a <= ? selectivity = a > ? and a < ? + 2 / ndv(a)
            if selectivity_dict.get(column_name):
                selectivity = ob_default_closed_range_sel + 2 / selectivity_dict.get(column_name)
            else:
                # no ndv is handled as a > ? and a < ?
                selectivity = ob_default_closed_range_sel
        elif opt_type == OptType.HALF_OPEN_RANGE:
            selectivity = ob_default_half_open_range_sel
        elif opt_type == OptType.HALF_OPEN_RANGE_EQUAL:
            if selectivity_dict.get(column_name):
                selectivity = ob_default_half_open_range_sel + 1 / selectivity_dict.get(column_name)
            else:
                selectivity = ob_default_half_open_range_sel
        elif opt_type == OptType.NOT_EQUAL:
            if selectivity_dict.get(column_name):
                selectivity = (selectivity_dict.get(column_name) - 1) / selectivity_dict.get(column_name)

        return selectivity

    def calculate_multi_col_ndv(self, column_list, selectivity_dict):
        """
        calculate the ndv of multiple columns.
        The columns that enter the calculation must only have in and =,
        and other operators are not within the scope of this calculation.
        :param column_list: =、in
        :param selectivity_dict:
        :return:
        """

        # return only the largest ndv combination
        # For example column_list: a,b,c know the ndv of a,b and the ndv of b,c,
        # need to see which is bigger ndv(a,b)*ndv(c) and ndv(b,c)*ndv(a)
        max_ndv = 0
        for i in range(len(column_list), 0, -1):
            # permutations
            combinations = itertools.combinations(column_list, i)
            for column_tuple in list(combinations):
                if len(column_tuple) == 1:
                    multi_column = column_tuple[0]
                else:
                    multi_column = '|'.join(column_tuple)
                if multi_column in selectivity_dict:
                    ndv = selectivity_dict.get(multi_column)
                    remain_list = list(set(column_list) - set(multi_column.split('|')))
                    if remain_list:
                        remain_multi_column = '|'.join(remain_list)
                        if remain_multi_column in selectivity_dict:
                            remain_ndv = selectivity_dict.get(remain_multi_column)
                        else:
                            # Whether there is a performance problem with recursion here needs to be verified
                            remain_ndv = self.calculate_multi_col_ndv(remain_list, selectivity_dict)
                    else:
                        # full match
                        return ndv
                    # ndv must be > 0, if = 0 it means that there is no such statistical information
                    if remain_ndv:
                        total_ndv = remain_ndv * ndv
                        if total_ndv > max_ndv:
                            max_ndv = total_ndv
        return max_ndv

    def get_recommend_index_without_statistics(self, candidate_index_list, filter_column_list):
        """
        In the absence of statistics, get the best index according to the rules
        :param candidate_index_list:
        :param filter_column_list:
        :return:
        """

        filter_column_name_list = []
        filter_column_opt_list = []
        for filter_column in filter_column_list:
            filter_column_name_list.append(filter_column['column_name'])
            filter_column_opt_list.append(filter_column['opt'])

        hit_index = None
        hit_index_column = None

        # traverse index
        for per_idx in candidate_index_list:
            column_list = per_idx.column_list
            has_interesting_order = per_idx.has_interesting_order
            # traverse index column
            ops_score = 0.0
            col_rank = 0
            hit_num = 0
            for column_name in column_list:
                col_rank += 1
                if column_name in filter_column_name_list:
                    hit_num += 1
                    sub_score = self.get_opt_weight(column_name, filter_column_name_list, filter_column_opt_list)
                    ops_score += float(math.pow(0.1, col_rank - 1) * sub_score)
                else:
                    # If the column is not in the filter column, break directly
                    break
            if has_interesting_order:
                ops_score += 0.1
            per_idx.hit_num = hit_num
            per_idx.ops_score = ops_score
        data_list = sorted(candidate_index_list, reverse=True, key=lambda item: (item.ops_score))

        # take the highest value according to the weight value,
        # if there are multiple highest values:
        # 1. Primary key;
        # 2. All index column are hit and are unique keys;
        # 3. The number of hit column is large;
        # 4. The number of index column is small
        for (ops_score), group_data in itertools.groupby(data_list, key=lambda item: (item.ops_score)):
            try:
                hit_index = ''
                hit_index_column = ''
                hit_col_num = 0
                hit_idx_cols = 0
                hit_idx_type = ''
                reset_mark = 0
                for per_idx in group_data:
                    # init
                    if not hit_index:
                        hit_index = per_idx.index_name
                        hit_col_num = per_idx.hit_num
                        hit_idx_cols = per_idx.column_count
                        hit_idx_type = per_idx.index_type
                        hit_index_column = per_idx.column_list
                    if hit_index != per_idx.index_name:
                        # If the current index is the primary key and the recommended index is not the primary key,
                        # update the recommended index as the primary key and immediately jump out of the loop
                        if per_idx.index_type == IndexType.PRIMARY and hit_idx_type != IndexType.PRIMARY:
                            hit_index = per_idx.index_name
                            hit_index_column = per_idx.column_list
                            break
                        # If the hit column count is the same as index column count,
                        # and the index type is unique, update the recommended index
                        if per_idx.hit_num == per_idx.column_count \
                                and per_idx.index_type == IndexType.UNIQUE \
                                and hit_idx_type != IndexType.UNIQUE:
                            reset_mark = 1
                        # If the hit column count is larger than the recommended index count,
                        # update the recommended index
                        elif per_idx.hit_num > hit_col_num:
                            # the full match unique index will be skip
                            if hit_idx_type == IndexType.UNIQUE and hit_col_num == hit_idx_cols:
                                reset_mark = 0
                            else:
                                reset_mark = 1
                        # If the hit column is the same,
                        # but the index column count is less than the recommended index, update the recommended index
                        elif per_idx.hit_num == hit_col_num:
                            # the full match unique index will be skip
                            if hit_idx_type == IndexType.UNIQUE and hit_col_num == hit_idx_cols:
                                reset_mark = 0
                            elif per_idx.column_count < hit_idx_cols:
                                reset_mark = 1
                            # When the hit column count and the index column count are all equal,
                            # the priority of the unique index is higher than that of the normal index
                            elif per_idx.column_count == hit_idx_cols \
                                    and hit_idx_type != IndexType.UNIQUE \
                                    and per_idx.index_type == IndexType.UNIQUE:
                                reset_mark = 1
                    if reset_mark:
                        reset_mark = 0
                        hit_index = per_idx.index_name
                        hit_index_column = per_idx.column_list
                        hit_col_num = per_idx.hit_num
                        hit_idx_cols = per_idx.column_count
                        hit_idx_type = per_idx.index_type
            except Exception as e:
                log.exception(e)
            # Only take the index with the largest weight value for judgment
            break
        return hit_index, hit_index_column

    def get_opt_weight(self, column_name, cols, opts):
        rt_weight = 0
        try:
            # Determine how many times the column appears in the filter column
            col_cnt = cols.count(column_name)
            # If the filter column is only 1
            if col_cnt == 1:
                pos = cols.index(column_name)
                opt = opts[pos]
                # Score 3 if = or in
                if opt == '=' or opt == 'in':
                    rt_weight = 3
                # Score 2 if there is a limit operation,
                elif opt == 'limit':
                    rt_weight = 2
                # Score 1 if there is a range or ordering
                elif opt == '<' or opt == '<=' or opt == '>' or opt == '>=' or opt == 'order':
                    rt_weight = 1
            else:
                tmp_cols = deepcopy(cols)
                tmp_opt = deepcopy(opts)
                tmp_hit = []
                # the weight score judgment is the same as above
                while (col_cnt > 0):
                    pos = tmp_cols.index(column_name)
                    opt = tmp_opt[pos]
                    if opt == '=' or opt == 'in':
                        rt_weight = 3
                        break
                    elif opt == '<' or opt == '>':
                        tmp_hit.append(opt)
                    elif opt == '<=' or opt == '>=':
                        tmp_hit.append(opt[:1])
                    elif opt == 'limit' and rt_weight < 2:
                        rt_weight = 2
                    elif opt == 'order' and rt_weight < 1:
                        rt_weight = 1
                    del tmp_cols[pos]
                    del tmp_opt[pos]
                    col_cnt = col_cnt - 1
                # If the score is less than 2, a bilateral judgment will be made
                if rt_weight < 2 and tmp_hit:
                    # If it is greater than 1 after deduplication,
                    # it means that it is a bilateral range, and the score is 2
                    if len(set(tmp_hit)) > 1:
                        rt_weight = 2
                    else:
                        rt_weight = 1
        except Exception as e:
            log.exception(e)

        return rt_weight
