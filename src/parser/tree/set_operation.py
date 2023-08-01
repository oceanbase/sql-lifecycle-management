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


class SetOperation(QueryBody):
    def __init__(self, line=None, pos=None):
        super(SetOperation, self).__init__(line, pos)

    def accept(self, visitor, context):
        return visitor.visit_set_operation(self, context)

    def __repr__(self):
        return repr(self.__dict__)


class Except(SetOperation):
    def __init__(
        self, line=None, pos=None, left=None, right=None, distinct=None, all=None
    ):
        super(Except, self).__init__(line, pos)
        self.left = left
        self.right = right
        self.distinct = distinct
        self.all = all

    def accept(self, visitor, context):
        return visitor.visit_except(self, context)

    def __str__(self):
        maybe_distinct = (" " + self.distinct) if self.distinct else ""
        maybe_all = (" " + self.all) if self.all else ""
        return " EXCEPT{}{} ".format(maybe_distinct, maybe_all).join(
            [str(self.left), str(self.right)]
        )


class Intersect(SetOperation):
    def __init__(self, line=None, pos=None, relations=None, distinct=None, all=None):
        super(Intersect, self).__init__(line, pos)
        self.relations = relations
        self.distinct = distinct
        self.all = all

    def accept(self, visitor, context):
        return visitor.visit_intersect(self, context)

    def __str__(self):
        maybe_distinct = (" " + self.distinct) if self.distinct else ""
        return " INTERSECT{} ".format(maybe_distinct).join(self.relations)


class Union(SetOperation):
    def __init__(self, line=None, pos=None, relations=None, distinct=None, all=None):
        super(Union, self).__init__(line, pos)
        self.relations = relations
        self.distinct = distinct
        self.all = all

    def accept(self, visitor, context):
        return visitor.visit_union(self, context)

    def __str__(self):
        maybe_distinct = (" " + self.distinct) if self.distinct else ""
        maybe_all = (" " + self.all) if self.all else ""
        return " UNION{}{} ".format(maybe_distinct, maybe_all).join(self.relations)
