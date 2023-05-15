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
from ..abstract_rule import AbstractRewriteRule


class RemoveOrderByInDeleteUpdateRule(AbstractRewriteRule):
    rule_explanation = """
    Q1: 
    DELETE FROM tbl WHERE col1 = ? ORDER BY col
    =>
    Q2: 
    DELETE FROM tbl WHERE col1 = ?
    
    The order by in the delete/update statement must be used together with the limit to make sense
    """

    def match(self, root: Statement, catalog=None) -> bool:

        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.is_match = False

            def visit_update(self, node, context):
                order_by = node.order_by
                limit = node.limit
                if len(order_by) > 0 and not limit:
                    self.is_match = True

            def visit_delete(self, node, context):
                order_by = node.order_by
                limit = node.limit
                if len(order_by) > 0 and not limit:
                    self.is_match = True

        visitor = Visitor()
        visitor.process(root, None)
        return visitor.is_match

    def match_action(self, root: Query, catalog=None):
        """
        Q1:
        DELETE FROM tbl WHERE col1 = ? ORDER BY col
        =>
        Q2:
        DELETE FROM tbl WHERE col1 = ?

        The order by in the delete/update statement must be used together with the limit to make sense

        :param root:
        :param catalog:
        :return:
        """

        class Visitor(DefaultTraversalVisitor):
            def visit_update(self, node, context):
                node.order_by = []

            def visit_delete(self, node, context):
                node.order_by = []
                return self.visit_statement(node, context)

        visitor = Visitor()
        visitor.process(root, None)
        return self.rule_explanation
