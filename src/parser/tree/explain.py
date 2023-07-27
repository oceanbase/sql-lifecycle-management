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


class ExplainOption(Node):
    def __init__(self, line=None, pos=None):
        super(ExplainOption, self).__init__(line, pos)

    def accept(self, visitor, context):
        return visitor.visit_explain_option(self, context)


class ExplainType(ExplainOption):
    def __init__(self, line=None, pos=None, type=None):
        super(ExplainType, self).__init__(line, pos)
        self.type = type


class ExplainFormat(ExplainOption):
    def __init__(self, line=None, pos=None, type=None):
        super(ExplainFormat, self).__init__(line, pos)
        self.type = type
