# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from optimizer.optimizer import Optimizer
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils
from common.utils import Utils


class ApiUtils(object):

    @staticmethod
    def get_xml_log_details(sql_text, catalog_object):

        optimizer = Optimizer()

        sql_text = Utils.remove_sql_text_affects_parser(sql_text)

        index_optimization_recommendation_list, development_specification_recommendation_list, after_sql_rewrite = \
            optimizer.optimize(sql_text, catalog_object)

        visitor = ParserUtils.format_statement(parser.parse(sql_text))
        table_list = []
        for _table in visitor.table_list:
            table_list.append(_table['table_name'])

        grade = 3

        optimize_action_list = []
        if index_optimization_recommendation_list:
            grade -= 1
        if development_specification_recommendation_list:
            optimize_action_list.append("issue")
            grade -= 1
        if after_sql_rewrite:
            optimize_action_list.append("rewrite")
            grade -= 1

        return {
            "grade": grade,
            "tableName": table_list,
            "optimizeAction": optimize_action_list,
            "sqlText": sql_text,
            "report": {
                "indexOptimizeationRecommendations": index_optimization_recommendation_list,
                "developmentSpecificationRecommendations": development_specification_recommendation_list,
                "sqlRewriteRecommendations": {
                    "sqlAfterRewrite": after_sql_rewrite.sql,
                    "ruleExplanationList": after_sql_rewrite.rule_explanation_list
                }
            }
        }
