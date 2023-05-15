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


class PMDIsNullRule(AbstractRewriteRule):
    rule_description = """
        Use IS NULL to determine whether it is a NULL value
        A direct comparison of NULL to any value is NULL.
         1) The return result of NULL<>NULL is NULL, not false.
         2) The return result of NULL=NULL is NULL, not true.
         3) The return result of NULL<>1 is NULL, not true.
        """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        NULL<>、<>NULL、=NULL、NULL=
        :param root:
        :param catalog:
        :return:
        """

        # NULL<>、<>NULL、=NULL、NULL=、!=NULL、 NULL!=
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_comparison_expression(self, node, context):
                if isinstance(node.left, NullLiteral):
                    self.match = True
                if isinstance(node.right, NullLiteral):
                    self.match = True
                return None

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MAJOR, self.rule_description)
