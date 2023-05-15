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


class Select(Node):
    def __init__(self, line=None, pos=None, distinct=False, select_items=None):
        super(Select, self).__init__(line, pos)
        self.distinct = distinct
        self.select_items = select_items

    def accept(self, visitor, context):
        return visitor.visit_select(self, context)
