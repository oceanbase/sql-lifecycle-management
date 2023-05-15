# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from optimizer.optimizer_enum import IndexType
from parser.tree import Statement
from .heuristic_rule_return_result import HeuristicRuleReturnResult
from ..abstract_rule import AbstractRewriteRule


class UniqueIndexWithIndexBackRule(AbstractRewriteRule):
    """
    uk match all
    """

    def match_action(self, root: Statement, candidate_index_list=None):
        _candidate_index_list = []
        for _index in candidate_index_list:
            _index_type = _index.index_type
            _index_name = _index.index_name
            _column_list = _index.column_list
            if _index_type == IndexType.PRIMARY.value or _index_type == IndexType.UNIQUE.value:
                _index_all_match = _index.index_all_match
                if _index_all_match:
                    return HeuristicRuleReturnResult(index_name=_index_name,
                                                     index_column_list=_column_list,
                                                     rule="UniqueIndexWithIndexBackRule",
                                                     message="")
