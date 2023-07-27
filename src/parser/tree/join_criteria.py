# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class JoinCriteria(object):
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            # skip dunders
            keys = [key for key in self.__dict__.keys() if "__" not in key]
            for key in keys:
                if getattr(self, key) != getattr(other, key):
                    return False
            return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)


class NaturalJoin(JoinCriteria):
    def __repr__(self):
        clz = self.__class__
        return "{name}()".format(name=clz.__name__)


class JoinUsing(JoinCriteria):
    def __init__(self, columns=None):
        self.columns = columns

    def __str__(self):
        return "{}{{columns={}}}".format(self.__class__.__name__, str(self.columns))

    def __repr__(self):
        clz = self.__class__
        args = "columns=" + repr(self.columns)
        return "{name}({args})".format(name=clz.__name__, args=args)


class JoinOn(JoinCriteria):
    def __init__(self, expression=None):
        self.expression = expression

    def __str__(self):
        return "{}{{expression={}}}".format(
            self.__class__.__name__, str(self.expression)
        )

    def __repr__(self):
        clz = self.__class__
        args = "expression=" + repr(self.expression)
        return "{name}({args})".format(name=clz.__name__, args=args)
