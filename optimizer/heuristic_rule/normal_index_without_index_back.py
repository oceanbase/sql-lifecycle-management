# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import sys
from typing import List

from optimizer.optimizer_enum import IndexType
from parser.tree import Statement
from .heuristic_rule_return_result import HeuristicRuleReturnResult
from ..abstract_rule import AbstractRewriteRule


class NormalIndexWithoutIndexBackRule(AbstractRewriteRule):
    """
    normal index full match and no need to index back, then recommend this index.
    If multiple such indexes exist, the one with the smallest number of index columns is recommended.
    """

    def match_action(self, root: Statement, candidate_index_list=None):
        _candidate_index_list = []
        for _index in candidate_index_list:
            _index_type = _index.index_type
            if _index_type == IndexType.NORMAL.value:
                _index_all_match = _index.index_all_match
                _index_back = _index.index_back
                if _index_all_match and not _index_back:
                    _candidate_index_list.append(_index)

        if _candidate_index_list:
            result_index_name, result_index_column = self.get_min_column_count_index(_candidate_index_list)
            return HeuristicRuleReturnResult(index_name=result_index_name,
                                             index_column_list=result_index_column,
                                             rule="NormalIndexWithoutIndexBackRule",
                                             message="")

        return None

    @staticmethod
    def get_min_column_count_index(index_list: List):
        """
        Get the index column with the smallest column_count in the index list
        :param index_list:
        :return:
        """

        if not index_list:
            return None

        if len(index_list) == 1:
            return index_list[0]

        _min_index_column_count = sys.maxsize
        result_index_name = None
        result_index_column = None
        for _index in index_list:
            _column_count = _index.column_count
            _index_name = _index.index_name
            _index_name_column = _index.column_list
            # Choose the one with the smallest number of index columns.
            if _column_count < _min_index_column_count:
                result_index_name = _index_name
                result_index_column = _index_name_column
                _min_index_column_count = _column_count

        return result_index_name, result_index_column
