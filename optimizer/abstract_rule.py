# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from abc import ABCMeta, abstractmethod

from parser.tree.statement import Statement


class AbstractRewriteRule(metaclass=ABCMeta):

    def match(self, root: Statement, catalog=None) -> bool:
        return True

    @abstractmethod
    def match_action(self, root: Statement, catalog=None):
        pass
