# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
from flask_restful import reqparse

from api.base_api import APIArgument, BaseAPI
from common.db_query import *
from common.enum import OptimizationTypeEunm
from common.utils import Utils
from metadata.metadata_utils import MetaDataUtils
from optimizer.optimizer import Optimizer as opt
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils

NOTHING_TO_DO = 'Current table index is so good , nothing to do'


class Optimizer(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(Optimizer, self).__init__(*args, **kwargs)
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('sqlText', required=True, help="sqlText cannot be blank!")
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('catalogJson')
        parser.add_argument('schemaSQL')
        parser.add_argument('isMonitor', type=bool)
        args = parser.parse_args()
        self.sql_text = args['sqlText']
        self.db_alias = args['databaseAlias']
        catalog = args.get('catalogJson', '{}')
        try:
            self.catalog_json = json.loads(catalog) if catalog else None
        except Exception as e:
            self.catalog_json = None
        self.schema_sql = args.get('schemaSQL', '')
        self.is_monitor = args.get('isMonitor', False)

    def post(self):
        """
            Single sql optimize
            ---
            tags:
              - Optimizer
            parameters:
                - in: body
                  name: sqlText
                  type: string
                  description: SQL文本
                  required: true
                - in: body
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: body
                  name: catalogJson
                  type: string
                  description: catalog信息
                  example:
                   "columns": [
                     {"schema":"luli1","table":"adbase_ad_word","name":"id","type":"bigint(20) unsigned","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"gmt_create","type":"datetime","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"gmt_modified","type":"datetime","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"word","type":"varchar(256)","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"status","type":"tinyint(4)","nullable":false},
                     {"schema":"luli1","table":"adbase_ad_word","name":"source","type":"tinyint(4)","nullable":true},
                     {"schema":"luli1","table":"adbase_ad_word","name":"order_index","type":"bigint(20)","nullable":true}
                    ]
                   "indexes": [
                    {"schema":"luli1","table":"adbase_ad_word","name":"PRIMARY","column":"id","cardinality":0,"unique":true},
                    {"schema":"luli1","table":"adbase_ad_word","name":"uk_word","column":"word","cardinality":0,"unique":true},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word","column":"word","cardinality":0,"unique":false},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word_status","column":"word","cardinality":0,"unique":false},
                    {"schema":"luli1","table":"adbase_ad_word","name":"idx_word_status","column":"status","cardinality":0,"unique":false}
                   ]
                   "tables": [
                    {"schema":"luli1","table":"adbase_ad_word","rows":0,"engine":"InnoDB"}
                   ]
                   "version": "5.7.36"
                  required: false
                - in: body
                  name: schemaSQL
                  type: string
                  description: 建表语句
                  example: CREATE TABLE `sqless_base` ( `a` int(2) NOT NULL, `b` int(2) NOT NULL, PRIMARY KEY (`a`), KEY `idx_a` (`b`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
                  required: false
                - in: body
                  name: isMonitor
                  type: bool
                  description: 判断是否为monitor类请求
                  required: false
            responses:
                200:
                   description: optimize result
        """
        schema_sql = self.schema_sql
        catalog_json = self.catalog_json
        catalog_object = ''
        if catalog_json:
            catalog_object = MetaDataUtils.json_to_catalog(catalog_json, schema_sql) if catalog_json else None
        elif schema_sql:
            catalog_object = MetaDataUtils.schema_sql_to_catalog_index(schema_sql)
        index_optimization_recommendation_list, development_specification_recommendation_list, after_sql_rewrite = opt().optimize(
            self.sql_text, catalog_object)
        grade = 3

        if index_optimization_recommendation_list:
            grade -= 1
        if development_specification_recommendation_list:
            grade -= 1
        if after_sql_rewrite:
            grade -= 1

        data = {
            "sqlOptimizationGrade": grade,
            "indexOptimizationRecommendations": index_optimization_recommendation_list,
            "developmentSpecificationRecommendations": development_specification_recommendation_list,
            "sqlRewriteRecommendations": {
                "sqlAfterRewrite": after_sql_rewrite.sql,
                "ruleExplanationList": after_sql_rewrite.rule_explanation_list
            }
        }
        if not self.is_monitor:
            insert_user_optimization(self.user_id, self.db_alias, self.sql_text, data,
                                     OptimizationTypeEunm.OPTIMIZE.value)

        return self.construct_success_response_entity(data=data)


class Parse(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(Parse, self).__init__(*args, **kwargs)
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('sqlText', required=True, help="sqlText cannot be blank!")
        parser.add_argument('databaseEngine', required=True, choices=['MySQL', 'OceanBase']
                            , help="The databaseEngine only supports MySQL/OceanBase")
        args = parser.parse_args()
        self.sql_text = args['sqlText']
        self.database_engine = args['databaseEngine']

    def get(self):
        """
            parse sql
            ---
            tags:
              - Parser
            parameters:
                - in: query
                  name: sqlText
                  type: string
                  description: SQL文本
                  required: true
                - in: query
                  name: databaseEngine
                  type: string
                  enum: ['MySQL', 'OceanBase']
                  description: 数据库类型
                  required: true
            responses:
                200:
                   description: parse result
        """
        sql_text = Utils.remove_sql_text_affects_parser(self.sql_text)
        visitor = ParserUtils.format_statement(parser.parse(sql_text))
        table_list = visitor.table_list
        column_list = []
        temp_table_list = []
        for _table in table_list:
            table_name = _table['table_name']
            filter_column_list = _table['filter_column_list']
            temp_table_list.append(table_name)
            for column in filter_column_list:
                column_list.append({
                    "tableName": table_name,
                    "columnName": column['column_name']
                })

        data = {
            "columnList": column_list,
            "tableList": temp_table_list
        }
        return self.construct_success_response_entity(data=data)
