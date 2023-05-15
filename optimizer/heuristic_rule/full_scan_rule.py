# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from parser.tree import Statement, Query
from parser.tree.expression import *
from .heuristic_rule_return_result import HeuristicRuleReturnResult
from ..abstract_rule import AbstractRewriteRule


class FullScanRule(AbstractRewriteRule):
    """
        Full Scan
    """

    could_range_predicate = ['=', '<', '>', '<=', '>=']

    def match(self, root: Statement, candidate_index_list=None):

        if isinstance(root, Query):
            query_body = root.query_body

            # no conditions
            if query_body is not None and query_body.limit is None and query_body.where is None:
                return True

            # There are only query conditions that cannot extract range, such as != 、not in and other predicates
            if query_body is not None and query_body.limit is None and query_body.where is not None:
                return not self.could_scan_range(query_body.where)

        return False

    def match_action(self, root: Statement, candidate_index_list=None):
        return HeuristicRuleReturnResult(index_name=None,
                                         index_column_list=None,
                                         rule="FullScanRule",
                                         message="Full table scan risk")

    def could_scan_range(self, where):
        """
        Determine whether there is a possibility that the range can be extracted in the where condition
        :param where:
        :return:
        """
        if isinstance(where, LogicalBinaryExpression):
            return self.could_scan_range(where.left) or self.could_scan_range(where.right)

        if isinstance(where, ComparisonExpression):
            type = where.type
            if type in self.could_range_predicate:
                return True

        if isinstance(where, LikePredicate):
            value = where.pattern.value
            if not value.startswith('%'):
                return True

        # between exists in may be able to extract the range
        if isinstance(where, BetweenPredicate) or isinstance(where, ExistsPredicate) or isinstance(where, InPredicate):
            return True

        return False
