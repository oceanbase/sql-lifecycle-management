# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .node import Node


class CommonTableExpr(Node):
    # column_name_list is zero, means select all columns.
    def __init__(self, line, pos, table_name, column_name_list, subquery):
        super(CommonTableExpr, self).__init__(line, pos)
        self.table_name = table_name
        self.column_name_list = column_name_list
        self.subquery = subquery

    def accept(self, visitor, context):
        return visitor.visit_common_table_expr(self, context)

    def __str__(self):
        return " ".join([self.name, self.typedef])


# With key word
class With(Node):
    def __init__(self, line, pos, common_table_expr_list):
        super(With, self).__init__(line, pos)
        self.common_table_expr_list = common_table_expr_list

    def accept(self, visitor, context):
        return visitor.visit_with(self, context)

    def __str__(self):
        return " ".join([self.name, self.typedef])


# With key word and subquery
class WithHasQuery(Node):
    def __init__(self, line, pos, with_list, query):
        super(WithHasQuery, self).__init__(line, pos)
        self.with_list = with_list
        self.query = query

    def accept(self, visitor, context):
        return visitor.visit_with(self, context)

    def __str__(self):
        return " ".join([self.name, self.typedef])
