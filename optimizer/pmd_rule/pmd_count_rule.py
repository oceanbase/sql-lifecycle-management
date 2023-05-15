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


class PMDCountRule(AbstractRewriteRule):
    rule_description = """
        count (column name) or count (constant) will miss null, 
        please determine whether you need to use count (*)
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

            def visit_function_call(self, node, context):
                name = node.name
                if isinstance(name,QualifiedName) and name.parts[0].lower() == 'count':
                    self.match = True
                    # count(*)
                    if not node.arguments:
                        self.match = False

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def match_action(self, root: Statement, catalog=None):
        return PMDResultRule(PMDLevel.MINOR, self.rule_description)
