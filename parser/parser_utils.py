# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .tree import *


class ParserUtils(object):

    @staticmethod
    def format_statement(statement):
        class FormatVisitor(DefaultTraversalVisitor):
            def __init__(self):
                """
                [
                    {
                        alias :
                        table_name :
                        filter_column_list: [
                            {
                                column_name :
                                opt :
                            },
                        ]
                    },
                    ...
                ]
                """
                self.table_list = []
                self.projection_column_list = []
                self.order_list = []
                self.min_max_list = []
                self.in_count_list = []
                self.limit_number = 0
                self.recursion_count = 0

            def visit_table(self, node, context):
                self.table_list.append({
                    'table_name': node.name.parts[0] if len(node.name.parts) == 1 else node.name.parts[1],
                    'alias': '',
                    'filter_column_list': []
                })
                return self.visit_query_body(node, context)

            def visit_aliased_relation(self, node, context):

                if not isinstance(node.relation, SubqueryExpression):
                    self.table_list.append({
                        'table_name': node.relation.name.parts[0],
                        'alias': node.alias[1],
                        'filter_column_list': []
                    })
                else:
                    return self.process(node.relation, context)

            def visit_logical_binary_expression(self, node, context):
                self.recursion_count = self.recursion_count + 1
                # A case similar to test_parser_utils.test_recursion_error may appear
                # discard the following data
                if self.recursion_count > 300:
                    return
                self.process(node.left, context)
                self.process(node.right, context)
                return None

            def visit_comparison_expression(self, node, context):
                left = node.left
                right = node.right
                type = node.type
                qualified_name_list = []

                if isinstance(right, QualifiedNameReference):
                    qualified_name_list.append(right.name)
                if isinstance(left, QualifiedNameReference):
                    qualified_name_list.append(left.name)

                for qualified_name in qualified_name_list:
                    if len(qualified_name.parts) == 2:
                        table_or_alias_name = qualified_name.parts[0]
                        for _table in self.table_list:
                            if _table['alias'] == table_or_alias_name or _table['table_name'] == table_or_alias_name:
                                filter_column_list = _table['filter_column_list']
                                filter_column_list.append({
                                    'column_name': qualified_name.parts[1],
                                    'opt': type
                                })
                    else:
                        filter_column_list = self.table_list[-1]['filter_column_list']
                        filter_column_list.append({
                            'column_name': qualified_name.parts[0],
                            'opt': type
                        })

                return self.visit_expression(node, context)

            def visit_like_predicate(self, node, context):
                if isinstance(node.value, QualifiedNameReference):
                    can_query_range = False
                    pattern = node.pattern
                    if isinstance(pattern, StringLiteral):
                        if not pattern.value.startswith('%'):
                            can_query_range = True
                    qualifed_name = node.value.name
                    if can_query_range:
                        if len(qualifed_name.parts) == 2:
                            table_or_alias_name = qualifed_name.parts[0]
                            for _table in self.table_list:
                                if _table['alias'] == table_or_alias_name or _table[
                                    'table_name'] == table_or_alias_name:
                                    filter_column_list = _table['filter_column_list']
                                    filter_column_list.append({
                                        'column_name': qualifed_name.parts[1],
                                        'opt': 'like'
                                    })
                        else:
                            filter_column_list = self.table_list[-1]['filter_column_list']
                            filter_column_list.append({
                                'column_name': qualifed_name.parts[0],
                                'opt': 'like'
                            })

                return self.visit_expression(node, context)

            def visit_not_expression(self, node, context):
                return self.process(node.value, "not")

            def visit_in_predicate(self, node, context):

                value = node.value

                if not context:
                    if isinstance(node.value_list, InListExpression):
                        self.in_count_list.append(len(node.value_list.values))

                    if isinstance(value, QualifiedNameReference):
                        if len(value.name.parts) == 2:
                            table_or_alias_name = value.name.parts[0]
                            for _table in self.table_list:
                                if _table['alias'] == table_or_alias_name or _table[
                                    'table_name'] == table_or_alias_name:
                                    filter_column_list = _table['filter_column_list']
                                    filter_column_list.append({
                                        'column_name': value.name.parts[1],
                                        'opt': 'in'
                                    })
                        else:
                            filter_column_list = self.table_list[-1]['filter_column_list']
                            filter_column_list.append({
                                'column_name': value.name.parts[0],
                                'opt': 'in'
                            })

                self.process(node.value, None)
                self.process(node.value_list, None)
                return None

            def visit_select(self, node, context):
                for item in node.select_items:
                    if isinstance(item, SingleColumn):
                        expression = item.expression
                        if isinstance(expression, QualifiedNameReference):
                            name = expression.name
                            if len(name.parts) == 2:
                                self.projection_column_list.append(name.parts[1])
                            else:
                                self.projection_column_list.append(name.parts[0])
                        if isinstance(expression, FunctionCall):
                            arguments = expression.arguments
                            if len(arguments) > 0:
                                for argument in arguments:
                                    if isinstance(argument, QualifiedNameReference):
                                        name = argument.name
                                        _column_name = ''
                                        if len(name.parts) == 2:
                                            _column_name = name.parts[1]
                                        else:
                                            _column_name = name.parts[0]

                                        if expression.name.parts[0] == 'max':
                                            self.min_max_list.append(_column_name)

                                        self.projection_column_list.append(_column_name)

                                    if isinstance(argument, LongLiteral):
                                        name = expression.name
                                        if isinstance(name, QualifiedName):
                                            if len(name.parts) == 1 and name.parts[0] == 'count':
                                                self.projection_column_list.append('count(*)')

                            else:
                                name = expression.name
                                if isinstance(name, QualifiedName):
                                    if len(name.parts) == 1 and name.parts[0] == 'count':
                                        self.projection_column_list.append('count(*)')
                    self.process(item, context)

            def visit_sort_item(self, node, context):
                sort_key = node.sort_key
                ordering = node.ordering
                if isinstance(sort_key, QualifiedNameReference):
                    name = sort_key.name
                    if len(name.parts) == 2:
                        self.order_list.append({
                            'ordering': ordering,
                            'column_name': name.parts[1]
                        })
                    else:
                        self.order_list.append({
                            'ordering': ordering,
                            'column_name': name.parts[0]
                        })
                return self.process(node.sort_key, context)

            def visit_query_specification(self, node, context):
                self.limit_number = node.limit
                self.process(node.select, context)
                if node.from_:
                    self.process(node.from_, context)
                if node.where:
                    self.process(node.where, context)
                if node.group_by:
                    grouping_elements = []
                    if isinstance(node.group_by, SimpleGroupBy):
                        grouping_elements = node.group_by.columns
                    elif isinstance(node.group_by, GroupingSets):
                        grouping_elements = node.group_by.sets
                    for grouping_element in grouping_elements:
                        self.process(grouping_element, context)
                if node.having:
                    self.process(node.having, context)
                for sort_item in node.order_by:
                    self.process(sort_item, context)
                return None

            def visit_update(self, node, context):
                table_list = node.table
                if table_list:
                    for _table in table_list:
                        self.process(_table, context)
                if node.where:
                    self.process(node.where, context)
                return None

            def visit_delete(self, node, context):
                table_list = node.table
                if table_list:
                    for _table in table_list:
                        self.process(_table, context)
                if node.where:
                    self.process(node.where, context)
                return None

        visitor = FormatVisitor()
        visitor.process(statement, None)
        return visitor

    @staticmethod
    def parameterized_query(statement):
        """
        Parameterized/normalized statement, used to normalize homogeneous SQL
        1. Parameterized
        2. Turn multiple in into single in
        3. Limit parameterized

        :param statement:
        :return:
        """

        class Visitor(DefaultTraversalVisitor):

            def visit_long_literal(self, node, context):
                node.value = '?'

            def visit_double_literal(self, node, context):
                node.value = '?'

            def visit_interval_literal(self, node, context):
                node.value = '?'

            def visit_timestamp_literal(self, node, context):
                node.value = '?'

            def visit_string_literal(self, node, context):
                node.value = '?'

            def visit_in_predicate(self, node, context):
                value_list = node.value_list
                if isinstance(value_list, InListExpression):
                    node.value_list.values = node.value_list.values[0:1]
                self.process(node.value, context)
                self.process(node.value_list, context)

            def visit_query_specification(self, node, context):
                node.limit = '?'
                self.process(node.select, context)
                if node.from_:
                    self.process(node.from_, context)
                if node.where:
                    self.process(node.where, context)
                if node.group_by:
                    grouping_elements = []
                    if isinstance(node.group_by, SimpleGroupBy):
                        grouping_elements = node.group_by.columns
                    elif isinstance(node.group_by, GroupingSets):
                        grouping_elements = node.group_by.sets
                    for grouping_element in grouping_elements:
                        self.process(grouping_element, context)
                if node.having:
                    self.process(node.having, context)
                for sort_item in node.order_by:
                    self.process(sort_item, context)
                return None

        visitor = Visitor()
        visitor.process(statement, None)
        return statement


def node_str_omit_none(node, *args):
    fields = ", ".join([": ".join([a[0], str(a[1])]) for a in args if a[1]])
    return "{clazz}({fields})".format(clazz=node.__class__.__name__, fields=fields)


def node_str(node, *args):
    fields = ", ".join([": ".join([a[0], a[1] or "None"]) for a in args])
    return "{class}({fields})".format(clazz=node.__class__.__name__, fields=fields)


FIELD_REFERENCE_PREFIX = "$field_reference$"


def mangle_field_reference(field_name):
    return FIELD_REFERENCE_PREFIX + field_name


def unmangle_field_reference(mangled_name):
    if not mangled_name.startswith(FIELD_REFERENCE_PREFIX):
        raise ValueError("Invalid mangled name: %s" % mangled_name)
    return mangled_name[len(FIELD_REFERENCE_PREFIX):]
