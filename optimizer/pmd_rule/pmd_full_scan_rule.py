# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from parser.tree import *
from parser.tree.visitor import DefaultTraversalVisitor
from .pmd_enum import PMDLevel
from .pmd_result import PMDResultRule
from ..abstract_rule import AbstractRewriteRule


class PMDFullScanRule(AbstractRewriteRule):
    rule_description = """
    Online query full table scan is not recommended. 
    Exceptions are: 
    1. very small table
    2. very low frequency
    3. the table/result set returned is very small (within 100 records / 100 KB).
    """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        match:
        select 1 from a
        select 1 from a where b != / <>
        select 1 from a where b not like
        select 1 from a where b not in
        select 1 from a where not exists
        select 1 from a where b like %a / %a%

        not match:
        select * from a left join b on (a.id = b.id) and a.c=1

        :param root:
        :param catalog:
        :return:
        """

        # Remove clauses such as exists / != / <> / not in / not like / like %a
        class Remove_Visitor(DefaultTraversalVisitor):
            def visit_comparison_expression(self, node, context):
                type = node.type
                if type in ('!=', '<>'):
                    node.left = None
                    node.right = None
                    node.type = None
                else:
                    self.process(node.left, context)
                    self.process(node.right, context)
                return None

            def visit_not_expression(self, node, context):
                node.value = None

            def visit_like_predicate(self, node, context):

                process_flag = True

                pattern = node.pattern

                if isinstance(pattern, StringLiteral):
                    value = pattern.value
                    if value.startswith('%'):
                        process_flag = False
                        node.pattern = None
                        node.value = None
                        node.escape = None

                if process_flag:
                    self.process(node.value, context)
                    self.process(node.pattern, context)
                    if node.escape is not None:
                        self.process(node.escape, context)
                return None

        # Determine whether there is a expression that can extract query range, if there is, it is not a full table scan
        class Query_Range_Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = True

            def visit_comparison_expression(self, node, context):
                type = node.type
                if type and type in ('=', '>', '<', '>=', '<='):
                    self.match = False
                if node.left:
                    self.process(node.left, context)
                if node.right:
                    self.process(node.right, context)
                return None

            def visit_like_predicate(self, node, context):

                if node.pattern and node.value:
                    pattern = node.pattern
                    if isinstance(pattern, StringLiteral):
                        value = pattern.value
                        if value.endswith('%'):
                            self.match = False

                if node.value:
                    self.process(node.value, context)
                if node.pattern:
                    self.process(node.pattern, context)
                if node.escape:
                    self.process(node.escape, context)
                return None

            def visit_not_expression(self, node, context):
                pass

            def visit_between_predicate(self, node, context):

                self.match = False

                self.process(node.value, context)
                self.process(node.min, context)
                self.process(node.max, context)
                return None

        remove_visitor = Remove_Visitor()
        remove_visitor.process(root, None)

        query_range_visitor = Query_Range_Visitor()
        query_range_visitor.process(root, None)

        return query_range_visitor.match

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MAJOR, self.rule_description)
