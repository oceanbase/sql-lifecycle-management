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


class WindowFunc(Node):
    def __init__(
        self,
        line=None,
        pos=None,
        func_name=None,
        func_args=None,
        ignore_null=None,
        window_spec=None,
    ):
        super(WindowFunc, self).__init__(line, pos)
        self.func_name = func_name
        self.func_args = func_args
        self.ignore_null = ignore_null
        self.window_spec = window_spec

    def accept(self, visitor, context):
        return super().accept(visitor, context)


class WindowSpec(Node):
    def __init__(
        self,
        line=None,
        pos=None,
        window_name=None,
        partition_by=None,
        order_by=None,
        frame_clause=None,
    ):
        super(WindowSpec, self).__init__(line, pos)
        self.window_name = window_name
        self.partition_by = partition_by
        self.order_by = order_by
        self.frame_clause = frame_clause

    def accept(self, visitor, context):
        return visitor.visit_window_spec(self, context)


class FrameClause(Node):
    def __init__(self, line=None, pos=None, type=None, frame_range=None):
        super(FrameClause, self).__init__(line, pos)
        self.type = type
        self.frame_range = frame_range

    def accept(self, visitor, context):
        return visitor.visit_frame_clause(self, context)


class WindowFrame(Node):
    def __init__(self, line=None, pos=None, type=None, start=None, end=None):
        super(WindowFrame, self).__init__(line, pos)
        self.type = type
        self.start = start
        self.end = end

    def accept(self, visitor, context):
        return visitor.visit_window_frame(self, context)


class FrameBound(Node):
    def __init__(self, line=None, pos=None, type=None, expr=None):
        super(FrameBound, self).__init__(line, pos)
        self.type = type
        self.expr = expr

    def accept(self, visitor, context):
        return visitor.visit_frame_bound(self, context)


class FrameExpr(Node):
    def __init__(self, line=None, pos=None, value=None, unit=None):
        super(FrameExpr, self).__init__(line, pos)
        self.value = value
        self.unit = unit
