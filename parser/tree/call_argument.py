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


class CallArgument(Node):
    def __init__(self, line=None, pos=None, name=None, value=None):
        super(CallArgument, self).__init__(line, pos)
        self.name = name
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_call_argument(self, context)
