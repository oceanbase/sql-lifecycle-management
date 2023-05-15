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


class Database(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)

    def get(self):
        """
            get database
            ---
            tags:
              - Database
            parameters:
                - in: query
                  name: currentPage
                  type: long
                  description: 当前页，默认不分页
                  required: false
                - in: query
                  name: pageSize
                  type: long
                  description: 每页大小，默认不分页
                  required: false
            responses:
                200:
                   description: get database result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('currentPage')
        parser.add_argument('pageSize')
        args = parser.parse_args()
        db_list = get_user_database(self.user_id, args['currentPage'], args['pageSize'])
        data = {
            "databaseAssetList": db_list
        }

        if args['currentPage'] and args['pageSize']:
            return self.construct_success_response_entity(data=data, total_count=len(db_list))
        else:
            return self.construct_success_response_entity(data=data)

    def post(self):
        """
            add database
            ---
            tags:
              - Database
            parameters:
                - in: body
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: body
                  name: databaseEngine
                  enum: ['MySQL', 'OceanBase']
                  type: string
                  description: 数据库类型
                  required: true
                - in: body
                  name: databaseVersion
                  type: string
                  description: 数据库版本
                  required: true
                - in: body
                  name: platform
                  type: string
                  description: 平台类型
                  required: true
                - in: body
                  name: databaseName
                  type: string
                  description: 数据库名称
                  required: true
            responses:
                200:
                   description: add database result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('databaseEngine', required=True, choices=['MySQL', 'OceanBase'],
                            help="The databaseEngine only supports MySQL/OceanBase")
        parser.add_argument('databaseVersion', required=True, help="databaseVersion cannot be blank!")
        parser.add_argument('platform', required=True, help="platform cannot be blank!")
        parser.add_argument('databaseName', required=True, help="databaseName cannot be blank!")
        args = parser.parse_args()
        is_insert_success, error_message = insert_user_database(self.user_id, args['databaseAlias'],
                                                                args['databaseEngine'],
                                                                args['databaseVersion'], args['platform'],
                                                                args['databaseName'])
        if error_message:
            return self.construct_success_response_entity(message=error_message, success=is_insert_success)
        else:
            return self.construct_success_response_entity(success=is_insert_success)

    def patch(self):
        """
            update database
            ---
            tags:
              - Database
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: databaseEngine
                  enum: ['MySQL', 'OceanBase']
                  type: string
                  description: 数据库类型
                  required: true
                - in: query
                  name: databaseVersion
                  type: string
                  description: 数据库版本
                  required: true
                - in: query
                  name: platform
                  type: string
                  description: 平台类型
                  required: true
                - in: query
                  name: databaseName
                  type: string
                  description: 数据库名称
                  required: true
            responses:
                200:
                   description: update database result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('databaseEngine', required=True, choices=['MySQL', 'OceanBase'],
                            help="The databaseEngine only supports MySQL/OceanBase")
        parser.add_argument('databaseVersion', required=True, help="databaseVersion cannot be blank!")
        parser.add_argument('platform', required=True, help="platform cannot be blank!")
        parser.add_argument('databaseName', required=True, help="databaseName cannot be blank!")
        args = parser.parse_args()
        success, error_message = update_user_database(args['databaseAlias'], args['databaseEngine'],
                                                      args['databaseVersion'], args['platform'], self.user_id)

        if error_message:
            return self.construct_success_response_entity(message=error_message, success=success)
        else:
            return self.construct_success_response_entity(success=success)


class UserOptimization(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(UserOptimization, self).__init__(*args, **kwargs)

    def get(self):
        """
            get optimization
            ---
            tags:
              - Optimization
            parameters:
                - in: query
                  name: databaseEngine
                  enum: ['MySQL', 'OceanBase']
                  type: string
                  description: 数据库类型
                  required: true
                - in: query
                  name: optimizationType
                  type: string
                  description: 类型
                  required: false
                - in: query
                  name: optimizationStatus
                  type: string
                  description: 状态
                  required: false
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: false
                - in: query
                  name: sqlText
                  type: string
                  description: SQL文本
                  required: false
                - in: query
                  name: startTimeTs
                  type: timestamp
                  description: 开始时间
                  required: true
                - in: query
                  name: endTimeTs
                  type: timestamp
                  description: 结束时间
                  required: true
            responses:
                200:
                   description: get optimization result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseEngine', required=True, choices=['MySQL', 'OceanBase']
                            , help="The databaseEngine only supports MySQL/OceanBase")
        parser.add_argument('optimizationType', choices=['optimize', 'analysis', 'review']
                            , help="The optimizationType only supports optimize/analysis/review")
        parser.add_argument('optimizationStatus')
        parser.add_argument('databaseAlias')
        parser.add_argument('sqlText')
        parser.add_argument('startTimeTs', required=True, help="startTimeTs cannot be blank!", type=int)
        parser.add_argument('endTimeTs', required=True, help="endTimeTs cannot be blank!", type=int)
        args = parser.parse_args()

        optimization_list, total_count = get_user_optimization(self.user_id, args['databaseEngine'],
                                                               args['startTimeTs'], args['endTimeTs'])

        for optimization in optimization_list:
            export_list = []
            report = json.loads(optimization['report'].replace('\\', ''))
            if 'reviewDetail' in report:
                for detail in report['reviewDetail']:
                    recommendation_list = detail['report']['indexOptimizeationRecommendations']
                    for recommendation in recommendation_list:
                        index_recommendation = recommendation['index_recommendation']
                        if index_recommendation.startswith('alter'):
                            export_list.append(index_recommendation)
            elif 'indexOptimizationRecommendations' in report:
                recommendation_list = report['indexOptimizationRecommendations']
                for recommendation in recommendation_list:
                    index_recommendation = recommendation['index_recommendation']
                    if index_recommendation.startswith('alter'):
                        export_list.append(index_recommendation)
            optimization['exportList'] = export_list

        data = {
            "optimizationHistoryList": optimization_list
        }
        return self.construct_success_response_entity(data=data, total_count=total_count)


class ReadUserOptimization(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(ReadUserOptimization, self).__init__(*args, **kwargs)

    def get(self, tag):
        """
            read optimization
            ---
            tags:
              - Optimization
            parameters:
                - in: path
                  name: tag
                  type: string
                  description: Tag
                  required: true
            responses:
                200:
                   description: read optimization result
        """
        read_user_optimization(tag, self.user_id)
        return self.construct_success_response_entity()
