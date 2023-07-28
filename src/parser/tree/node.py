# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from inspect import getfullargspec


class Node(object):
    def __init__(self, line=None, pos=None, **kwargs):
        self.line = line
        self.pos = pos
        if kwargs:
            for attr, value in kwargs.items():
                setattr(self, attr, value)

    def accept(self, visitor, context):
        return visitor.visit_node(self, context)

    def __str__(self):
        return str({k: v for k, v in self.__dict__.items() if v is not None})

    def __repr__(self):
        clz = self.__class__
        argspec = [
            x
            for x in getfullargspec(clz.__init__).args[3:]
            if self.__dict__[x] is not None
        ]
        args = ", ".join(["=".join((arg, repr(getattr(self, arg)))) for arg in argspec])
        return "{name}({args})".format(name=clz.__name__, args=args)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            keys = [key for key in self.__dict__.keys() if key not in ("line", "pos")]
            for key in keys:
                if getattr(self, key) != getattr(other, key):
                    return False
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
