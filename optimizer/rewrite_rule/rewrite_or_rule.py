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
from parser.tree import *
from parser.tree.visitor import DefaultTraversalVisitor
from ..abstract_rule import AbstractRewriteRule


class RewriteMySQLORRule(AbstractRewriteRule):
    rule_explanation = """
    Q1: 
    SELECT * FROM T1 WHERE C1 < 20000 OR C2 < 30 ;
    =>
    Q2: 
    SELECT /*SEL_1*/ * FROM T1 WHERE C1 < 20000 UNION
    SELECT /*SEL_2*/ * FROM T1 WHERE C2 < 30;

    If both C1 and C2 have index on the T1 table, then rewrite it as Q2 can use the index on both sides
    If only 1 column has an index, rewrite has no promotion
    If there is no index, the rewrite will twice full table scans, and the rewrite will be worse.

    Q1: 
    SELECT * FROM T1 WHERE C1 = 20000 OR C1 = 30 ;
    =>
    Q2: 
    SELECT * FROM T1 WHERE C1 in (2000, 30)
    """

    def match(self, root: Statement, catalog=None) -> bool:

        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.is_match = False

            def visit_query(self, node, context):
                query_body = root.query_body
                if isinstance(query_body, QuerySpecification):
                    where = query_body.where
                    if isinstance(where, LogicalBinaryExpression):
                        if str(where.type).lower() == 'or':
                            self.is_match = True

            def visit_update(self, node, context):
                if isinstance(node.where, LogicalBinaryExpression):
                    if str(node.where.type).lower() == 'or' and context:
                        # must contain primary key information
                        if context.table_list and context.table_list[0].index_list:
                            index_list = context.table_list[0].index_list
                            for index in index_list:
                                if index.index_type == IndexType.PRIMARY:
                                    self.is_match = True

            def visit_delete(self, node, context):
                if isinstance(node.where, LogicalBinaryExpression):
                    if str(node.where.type).lower() == 'or' and context:
                        # must contain primary key information
                        if context.table_list and context.table_list[0].index_list:
                            index_list = context.table_list[0].index_list
                            for index in index_list:
                                if index.index_type == IndexType.PRIMARY:
                                    self.is_match = True

        visitor = Visitor()
        visitor.process(root, catalog)
        return visitor.is_match

    def match_action(self, root: Query, catalog=None):
        """
        Q1:
        SELECT * FROM T1 WHERE C1 < 20000 OR C2 < 30 ;
        =>
        Q2:
        SELECT /*SEL_1*/ * FROM T1 WHERE C1 < 20000 UNION
        SELECT /*SEL_2*/ * FROM T1 WHERE C2 < 30;

        If both C1 and C2 have index on the T1 table, then rewrite it as Q2 can use the index on both sides
        If only 1 column has an index, rewrite has no promotion
        If there is no index, the rewrite will twice full table scans, and the rewrite will be worse.

        Q1:
        SELECT * FROM T1 WHERE C1 = 20000 OR C1 = 30 ;
        =>
        Q2:
        SELECT * FROM T1 WHERE C1 in (2000, 30)

        :param root:
        :param catalog:
        :return:
        """

        class Visitor(DefaultTraversalVisitor):
            def visit_query(self, node, context):
                query_body = root.query_body
                select = query_body.select
                left = query_body.where.left
                right = query_body.where.right
                table = query_body.from_

                is_match = False

                # SELECT * FROM T1 WHERE C1 = 20000 OR C1 = 30 ;
                if isinstance(left, ComparisonExpression) and isinstance(right, ComparisonExpression):
                    if left.type == right.type == '=':
                        left_qualified_name_reference = left.left if isinstance(left.left, QualifiedNameReference) \
                            else left.right
                        left_value = left.right if isinstance(left.left, QualifiedNameReference) \
                            else left.left
                        right_qualified_name_reference = right.left if isinstance(right.left, QualifiedNameReference) \
                            else right.right
                        right_value = right.right if isinstance(right.left, QualifiedNameReference) \
                            else right.left

                        if left_qualified_name_reference == right_qualified_name_reference:
                            is_match = True
                            query_body.where = InPredicate(value=left_qualified_name_reference,
                                                           value_list=InListExpression(
                                                               values=[left_value, right_value]))
                # SELECT * FROM T1 WHERE C1 in (20000) OR C1 = 30
                if (isinstance(left, ComparisonExpression) and isinstance(right, InPredicate)) or \
                        (isinstance(left, InPredicate) and isinstance(right, ComparisonExpression)):
                    in_expression = left if isinstance(left, InPredicate) else right
                    comparison_expression = left if isinstance(left, ComparisonExpression) else right
                    if comparison_expression.type == '=':
                        qualified_name_reference = comparison_expression.left \
                            if isinstance(comparison_expression.left, QualifiedNameReference) \
                            else comparison_expression.right
                        comparison_value = comparison_expression.right \
                            if isinstance(comparison_expression.left, QualifiedNameReference) \
                            else comparison_expression.left

                        if qualified_name_reference == in_expression.value and isinstance(in_expression.value_list,
                                                                                          InListExpression):
                            in_expression.value_list.values.append(comparison_value)
                            query_body.where = in_expression
                            is_match = True

                # SELECT * FROM T1 WHERE C1 in (20000) OR C1 in (30)
                if isinstance(left, InPredicate) and isinstance(right, InPredicate):
                    if left.value == right.value and isinstance(left.value_list, InListExpression) and isinstance(
                            right.value_list, InListExpression):
                        left.value_list.values.extend(right.value_list.values)
                        query_body.where = left
                        is_match = True

                if not is_match:
                    relations = [QuerySpecification(select=select, from_=table, where=left),
                                 QuerySpecification(select=select, from_=table, where=right)]
                    node.query_body = Union(relations=relations)

            def visit_update(self, node, context):
                index_list = context.table_list[0].index_list
                for index in index_list:
                    pass

            def visit_delete(self, node, context):
                if isinstance(node.where, LogicalBinaryExpression):
                    if str(node.where.type).lower() == 'or':
                        self.is_match = True

        visitor = Visitor()
        visitor.process(root, catalog)
        return self.rule_explanation
