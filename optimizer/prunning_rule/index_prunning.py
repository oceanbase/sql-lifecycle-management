# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from enum import Enum, unique


@unique
class CompareStat(Enum):
    UNCOMPARABLE = -2  # two dimension can't not compare
    RIGHT_DOMINATED = -1  # right dominate left, at least one dimension of right is better than left
    EQUAL = 0
    LEFT_DOMINATED = 1  # left dominate right, at least one dimension of left is better right


class IndexPrunning():
    """
    The logic of pruning is to compare indexes in pairs,
    A LEFT_DOMINATED B, then A has at least one dimension that is better than B, and other dimensions must be EQUAL
    Dimension：
        （1）index back
        （2）interesting order
        （3）extract the query range
    """

    def __init__(self, index_dict, other_index_dict, filter_column_cnt):
        self.left = index_dict
        self.right = other_index_dict
        self.filter_column_cnt = filter_column_cnt

    # Comparison of index back dimensions
    def index_back_dim_compare(self):
        left = self.left
        right = self.right
        status = CompareStat.UNCOMPARABLE
        if left.index_back == right.index_back:
            status = CompareStat.EQUAL
        elif (not left.index_back) and right.index_back:
            # In the case where the query range and the interesting order cannot be extracted,
            # consider the size of the columns on both sides,
            # only when the restrict info on the left is the super set on the right,
            # and the columns on the left are less than the columns on the right,
            # the left Dominated to the right
            if (not left.has_interesting_order) and (not left.extract_range) and (
                    not right.has_interesting_order) and (not right.extract_range):
                if 0 == self.filter_column_cnt:
                    # If there is no filter condition on the right,
                    # it will go through full table scan and index back, so pruning
                    status = CompareStat.LEFT_DOMINATED
                elif left.column_count <= right.column_count:
                    status = CompareStat.LEFT_DOMINATED
            else:
                status = CompareStat.LEFT_DOMINATED
        elif left.index_back and not right.index_back:
            if (not left.has_interesting_order) and (not left.extract_range) and (
                    not right.has_interesting_order) and (not right.extract_range):
                if 0 == self.filter_column_cnt:
                    # If there is no filter condition on the left,
                    # it will go through full table scan and index back, so pruning
                    status = CompareStat.RIGHT_DOMINATED
                elif left.column_count >= right.column_count:
                    status = CompareStat.RIGHT_DOMINATED
            else:
                status = CompareStat.RIGHT_DOMINATED
        return status

    # Comparison of interesting order dimensions
    def interesting_order_dim_compare(self):
        left = self.left
        right = self.right
        status = CompareStat.UNCOMPARABLE
        if left.has_interesting_order and right.has_interesting_order:
            status = CompareStat.EQUAL
        elif (not left.has_interesting_order) and not right.has_interesting_order:
            status = CompareStat.EQUAL
        elif left.has_interesting_order and not right.has_interesting_order:
            status = CompareStat.LEFT_DOMINATED
        elif (not left.has_interesting_order) and right.has_interesting_order:
            status = CompareStat.RIGHT_DOMINATED

        return status

    # Comparison of query range dimensions
    def query_range_dim_compare(self):
        left = self.left
        right = self.right
        status = CompareStat.UNCOMPARABLE

        if left.extract_range == right.extract_range:
            status = CompareStat.EQUAL
        elif set(right.extract_range).issubset(set(left.extract_range)):
            status = CompareStat.LEFT_DOMINATED
        elif set(left.extract_range).issubset(set(right.extract_range)):
            status = CompareStat.RIGHT_DOMINATED
        return status

    # Compare three dimensions
    # if one dimension UNCOMPARABLE, then cannot compare
    # A LEFT_DOMINATED B, Then A is better than B in at least one dimension, and other dimensions must be EQUAL
    def skyline_compare(self) -> CompareStat:

        dim1 = self.index_back_dim_compare().value
        dim2 = self.interesting_order_dim_compare().value
        dim3 = self.query_range_dim_compare().value

        if dim1 >= 0 and dim2 >= 0 and dim3 >= 0 and dim1 + dim2 + dim3 != 0:
            return CompareStat.LEFT_DOMINATED

        if (dim1 == -1 or dim1 == 0) and (dim2 == -1 or dim2 == 0) and (dim3 == -1 or dim3 == 0) \
                and dim1 + dim2 + dim3 != 0:
            return CompareStat.RIGHT_DOMINATED

        return CompareStat.UNCOMPARABLE
