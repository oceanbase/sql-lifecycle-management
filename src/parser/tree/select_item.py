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


class SelectItem(Node):
    def __init__(self, line=None, pos=None):
        super(SelectItem, self).__init__(line, pos)


class AllColumns(SelectItem):
    def __init__(self, line=None, pos=None, prefix=None):
        super(AllColumns, self).__init__(line=line, pos=pos)
        self.prefix = prefix

    def accept(self, visitor, context):
        return visitor.visit_all_columns(self, context)

    def __str__(self):
        return "%s*" % (self.prefix + ".") if self.prefix else ""


class SingleColumn(SelectItem):
    def __init__(self, line=None, pos=None, alias=None, expression=None):
        super(SingleColumn, self).__init__(line, pos)
        self.alias = alias
        self.expression = expression

    def accept(self, visitor, context):
        return visitor.visit_single_column(self, context)

    def __str__(self):
        return str(self.expression) + (" " + self.alias) if self.alias else ""


class Partition(Node):
    def __init__(self, line=None, pos=None, partition_list=Node):
        super(Partition, self).__init__(line, pos)
        self.partition_list = partition_list

    def accept(self, visitor, context):
        return visitor.visit_single_column(self, context)
