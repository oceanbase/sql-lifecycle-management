# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from src.optimizer.engine import Engine
from sqlgpt_parser.parser.mysql_parser import parser as mysql_parser
from .rewrite_rule import mysql_rules, common_rules


class MySQLEngine(Engine):
    def __new__(cls):
        singleton = cls.__dict__.get('__singleton__')
        if singleton is not None:
            return singleton

        cls.__singleton__ = singleton = object.__new__(cls)

        return singleton

    def parse(self, sql, tracking=False):
        return mysql_parser.parse(sql, tracking=tracking)

    def rewrite(self, statement, catalog=None):
        common_rules.extend(mysql_rules)
        rule_explanation_list = []
        if common_rules:
            for rewrite_rule in common_rules:
                if rewrite_rule.match(statement, catalog):
                    rule_explanation_list.append(
                        rewrite_rule.match_action(statement, catalog)
                    )
        return statement, rule_explanation_list
