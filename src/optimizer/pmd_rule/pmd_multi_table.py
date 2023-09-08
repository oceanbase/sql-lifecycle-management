# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from sqlgpt_parser.sql_parser.tree.join_criteria import JoinOn, JoinUsing
from sqlgpt_parser.sql_parser.tree.statement import Statement
from sqlgpt_parser.sql_parser.tree.visitor import DefaultTraversalVisitor
from .pmd_enum import PMDLevel
from .pmd_result import PMDResultRule
from ..abstract_rule import AbstractRewriteRule


class PMDMultiTableRule(AbstractRewriteRule):
    rule_description = """
        The number of association tables is not recommended to exceed 3
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
                self.join_count = 0

            def visit_join(self, node, context):
                self.join_count = self.join_count + 1

                if self.join_count >= 3:
                    self.match = True
                else:
                    self.process(node.left, context)
                    self.process(node.right, context)

                    if isinstance(node.criteria, JoinOn):
                        self.process(node.criteria.expression, context)
                    elif isinstance(node.criteria, JoinUsing):
                        self.process(node.criteria.columns)

                return None

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MAJOR, self.rule_description)
