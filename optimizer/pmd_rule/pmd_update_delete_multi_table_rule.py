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


class PMDUpdateDeleteMultiTableRule(AbstractRewriteRule):
    rule_description = """
        UPDATE / DELETE does not recommend using multiple tables
        """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        :param root:
        :param catalog:
        :return:
        """

        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_delete(self, node, context):
                table = node.table
                if table and isinstance(table[0], Join):
                    self.match = True

            def visit_update(self, node, context):
                table = node.table
                if table and isinstance(table[0], Join):
                    self.match = True

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MINOR, self.rule_description)
