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


class PMDSelectAllRule(AbstractRewriteRule):
    rule_description = 'Please use select column name instead of select *'

    def match(self, root: Statement, catalog=None) -> bool:
        """
        select *
        :param root:
        :param catalog:
        :return:
        """

        # select *
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.is_select_all = False

            def visit_select(self, node, context):
                for item in node.select_items:
                    if isinstance(item, SingleColumn) \
                            and isinstance(item.expression, QualifiedNameReference) \
                            and isinstance(item.expression.name, QualifiedName):
                        parts = item.expression.name.parts
                        for part in parts:
                            if part == '*':
                                self.is_select_all = True
                                break

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.is_select_all

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MAJOR, self.rule_description)
