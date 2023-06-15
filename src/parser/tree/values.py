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


class Values(QueryBody):
    def __init__(self, line=None, pos=None, rows=None):
        super(Values, self).__init__(line, pos)
        self.rows = rows

    def accept(self, visitor, context):
        return visitor.visit_values(self, context)

    def __str__(self):
        return "(" + ", ".join(self.rows or []) + ")"
