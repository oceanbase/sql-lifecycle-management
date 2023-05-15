# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from optimizer.engine import Engine
from parser.oceanbase_parser import parser
from .rewrite_rule import common_rules


class OceanBaseEngine(Engine):

    def __new__(cls):
        singleton = cls.__dict__.get('__singleton__')
        if singleton is not None:
            return singleton

        cls.__singleton__ = singleton = object.__new__(cls)

        return singleton

    def parse(self, sql, tracking=False):
        return parser.parse(sql, tracking)

    def rewrite(self, statement, catalog=None):
        rule_explanation_list = []
        if common_rules:
            for rewrite_rule in common_rules:
                if rewrite_rule.match(statement, catalog):
                    rule_explanation_list.append(rewrite_rule.match_action(statement, catalog))
        return statement, rule_explanation_list
