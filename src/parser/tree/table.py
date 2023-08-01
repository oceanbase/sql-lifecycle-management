# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .relation import QueryBody


class Table(QueryBody):
    def __init__(
        self,
        line=None,
        pos=None,
        name=None,
        order_by=None,
        limit=None,
        offset=None,
        for_update=False,
    ):
        super(Table, self).__init__(line, pos)
        self.name = name
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.for_update = for_update

    def accept(self, visitor, context):
        return visitor.visit_table(self, context)

    def __str__(self):
        return str(self.name)


class TableSubquery(QueryBody):
    def __init__(self, line=None, pos=None, query=None):
        super(TableSubquery, self).__init__(line, pos)
        self.query = query

    def accept(self, visitor, context):
        return visitor.visit_table_subquery(self, context)
