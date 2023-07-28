# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class QualifiedName(object):
    def __init__(self, parts=None):
        self.parts = parts

    @staticmethod
    def of(*parts):
        """
        Convenience method for constructiong QualfiedNames.
        :param parts: If len(parts) == 1, will split on the periods for you
        """
        if len(parts) == 1 and "." in parts:
            parts = parts[0].split(".")
        return QualifiedName(parts=[part for part in parts])

    def __str__(self):
        return '.'.join(self.parts or [])

    def __repr__(self):
        return "QualifiedName.of(\"%s\")" % '.'.join(self.parts or [])

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.parts == other.parts
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(tuple(self.parts))
