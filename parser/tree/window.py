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


class Window(Node):
    def __init__(self, line=None, pos=None, partition_by=None, order_by=None, frame=None):
        super(Window, self).__init__(line, pos)
        self.partition_by = partition_by
        self.order_by = order_by
        self.frame = frame

    def accept(self, visitor, context):
        return visitor.visit_window(self, context)


class WindowFrame(Node):
    def __init__(self, line=None, pos=None, type=None, start=None, end=None):
        super(WindowFrame, self).__init__(line, pos)
        self.type = type
        self.start = start
        self.end = end

    def accept(self, visitor, context):
        return visitor.visit_window_frame(self, context)


class FrameBound(Node):
    def __init__(self, line=None, pos=None, type=None, value=None):
        super(FrameBound, self).__init__(line, pos)
        self.type = type
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_frame_bound(self, context)
