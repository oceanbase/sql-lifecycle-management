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


class RewriteSupplementColumnRule(AbstractRewriteRule):
    rule_explanation = """
        Q1: 
        SELECT * FROM T1;
        =>
        Q2: 
        SELECT a,b,c,d FROM T1
        """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        To supplement column names, catalog are required
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

        if not visitor.is_select_all:
            return False

        if catalog:
            table_list = catalog.table_list
            if table_list and table_list[0].column_list:
                return True

        return False

    def match_action(self, root: Query, catalog=None):

        class Visitor(DefaultTraversalVisitor):
            def __init__(self, catalog_table_list):
                self.catalog_table_list = catalog_table_list
                self.table_list = []
                self.alias_list = []

            def visit_query_specification(self, node, context):
                if node.from_:
                    self.process(node.from_, context)
                self.process(node.select, context)
                return None

            def visit_table(self, node, context):
                parts = node.name.parts
                if len(parts) == 1:
                    self.table_list.append(parts[0])
                else:
                    self.table_list.append(parts[1])
                return self.visit_query_body(node, context)

            def visit_aliased_relation(self, node, context):
                alias = node.alias
                if isinstance(node.relation, Table):
                    parts = node.relation.name.parts
                    if len(parts) == 1:
                        table_name = parts[0]
                    else:
                        table_name = parts[1]
                    self.alias_list.append({
                        'alias': alias[1],
                        'table_name': table_name
                    })
                return self.process(node.relation, context)

            def visit_select(self, node, context):
                projection_column_list = []
                for item in node.select_items:
                    if isinstance(item, SingleColumn) \
                            and isinstance(item.expression, QualifiedNameReference) \
                            and isinstance(item.expression.name, QualifiedName):
                        parts = item.expression.name.parts
                        if len(parts) == 1 and parts[0] == '*':
                            table_name = self.table_list[0]
                            for catalog_table in catalog_table_list:
                                if table_name == catalog_table.table_name:
                                    column_list = catalog_table.column_list
                                    for column in column_list:
                                        projection_column_list.append(
                                            SingleColumn(expression=QualifiedNameReference(
                                                name=QualifiedName.of(column.column_name))))

                        elif len(parts) == 2 and parts[1] == '*' and self.alias_list:
                            table_name = self.alias_list[0]['table_name']
                            alias = self.alias_list[0]['alias']
                            for catalog_table in catalog_table_list:
                                if table_name == catalog_table.table_name:
                                    column_list = catalog_table.column_list
                                    for column in column_list:
                                        projection_column_list.append(
                                            SingleColumn(expression=QualifiedNameReference(
                                                name=QualifiedName.of(alias + '.' + column.column_name))))
                        else:
                            projection_column_list.append(item)
                    else:
                        projection_column_list.append(item)

                if projection_column_list:
                    node.select_items = projection_column_list

                return None

        catalog_table_list = catalog.table_list
        visitor = Visitor(catalog_table_list)
        visitor.process(root, None)

        return self.rule_explanation
