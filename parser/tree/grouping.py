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


class GroupingElement(Node):
    def __init__(self, line=None, pos=None):
        super(GroupingElement, self).__init__(line, pos)


class GroupingSets(GroupingElement):
    def __init__(self, line=None, pos=None, sets=None):
        super(GroupingSets, self).__init__(line, pos)
        self.sets = sets


class SimpleGroupBy(GroupingElement):
    def __init__(self, line=None, pos=None, columns=None):
        super(SimpleGroupBy, self).__init__(line, pos)
        self.columns = columns
