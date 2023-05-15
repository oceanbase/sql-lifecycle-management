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
from typing import List

from optimizer.cbo.cbo import CBO
from parser.tree.statement import Statement
from .heuristic_rule import heuristic_rule_list
from .pmd_rule import common_pmd_list


class Engine(metaclass=ABCMeta):

    @abstractmethod
    def parse(self, sql: str, tracking: bool) -> List:
        pass

    @abstractmethod
    def rewrite(self, statement: Statement, catalog = None) -> Statement:
        pass

    def rbo(self, statement: Statement, candidate_index_list):
        # heuristic_rule
        for heuristic_rule in heuristic_rule_list:
            if heuristic_rule.match(statement, candidate_index_list):
                heuristic_rule_return_result = heuristic_rule.match_action(statement, candidate_index_list)
                if heuristic_rule_return_result:
                    return heuristic_rule_return_result

    def cbo(self, statement: Statement, cbo_info: CBO):
        """
        enterprise suit
        :param statement:
        :param catalog:
        :return:
        """
        pass

    def pmd(self, statement: Statement, catalog=None):
        pmd_result_list = []

        for pmd_rule in common_pmd_list:
            if pmd_rule.match(statement, catalog):
                pmd_rule_result = pmd_rule.match_action(statement, catalog)
                if pmd_rule_result:
                    pmd_result_list.append(pmd_rule_result.__str__())

        return pmd_result_list
