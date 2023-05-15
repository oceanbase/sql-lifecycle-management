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


class TopSQL(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(TopSQL, self).__init__(*args, **kwargs)

    def get(self):
        """
            get top sql
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
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
                - in: query
                  name: searchSQLText
                  type: string
                  description: SQL Text查询的内容
                  required: false
                - in: query
                  name: searchName
                  type: string
                  description: 高级搜索的指标名
                  required: false
                - in: query
                  name: searchSymbol
                  type: string
                  description: 高级搜索的符号
                  required: false
                - in: query
                  name: searchContext
                  type: string
                  description: 高级搜索的内容
                  required: false
            responses:
                200:
                   description: get top sql result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('startTimeTs', required=True, help="startTimeTs cannot be blank!", type=int)
        parser.add_argument('endTimeTs', required=True, help="endTimeTs cannot be blank!", type=int)
        parser.add_argument('searchSQLText')
        parser.add_argument('searchName', choices=["sqlId", "userName", "clientIp", "executions", "elapsedTime",
                                                   "cpuTime", "queueTime", "getplanTime", "totalWaitTime",
                                                   "netwaitTime", "iowaitTime", "returnRows", "affectedRows",
                                                   "logicalReads", "retryCnt", "failTimes", "rpcCount",
                                                   "remotePlans", "missPlans"],
                            help='''The searchName only supports "sqlId", "userName", "clientIp", "executions", 
                            "elapsedTime", "cpuTime", "queueTime", "getplanTime", "totalWaitTime", "netwaitTime", 
                            "iowaitTime", "returnRows", "affectedRows", "logicalReads", "retryCnt", "failTimes", 
                            "rpcCount", "remotePlans", "missPlans"''')
        parser.add_argument('searchSymbol', choices=["=", ">", ">=", "<", "<=", "!=", " like ", " not like "],
                            help='''The searchName only supports "=", ">", ">=", "<", "<=", "!=", " like ", " not like " ''')
        parser.add_argument('searchContext')
        args = parser.parse_args()

        data = MonitorDBInfo().get_top_sql(database_alias=args['databaseAlias'], user_id=self.user_id,
                                           start_time=args['startTimeTs'], end_time=args['endTimeTs'],
                                           search_sql_text=args['searchSQLText'],
                                           search_context=args['searchContext'], search_name=args['searchName'],
                                           search_symbol=args['searchSymbol'])

        return self.construct_success_response_entity(data=data)


class SQLPlan(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(SQLPlan, self).__init__(*args, **kwargs)

    def get(self):
        """
            get sql plan
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: sqlId
                  type: string
                  description: SQL ID
                  required: true
            responses:
                200:
                   description: get sql plan result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('sqlId', required=True, help="sqlId cannot be blank!")
        args = parser.parse_args()

        data = MonitorDBInfo().get_sql_plan(database_alias=args['databaseAlias'], user_id=self.user_id,
                                            sql_id=args['sqlId'])

        return self.construct_success_response_entity(data=data)


class SQLText(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(SQLText, self).__init__(*args, **kwargs)

    def get(self):
        """
            get sql text
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: sqlId
                  type: string
                  description: SQL ID
                  required: true
            responses:
                200:
                   description: get sql text result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('sqlId', required=True, help="sqlId cannot be blank!")
        args = parser.parse_args()

        data = MonitorDBInfo().get_sql_text(database_alias=args['databaseAlias'], user_id=self.user_id,
                                            sql_id=args['sqlId'])

        if data:
            data = data[0]
        else:
            data = {}

        return self.construct_success_response_entity(data=data)


class SQLDetail(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(SQLDetail, self).__init__(*args, **kwargs)

    def get(self):
        """
            get sql detail
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: sqlId
                  type: string
                  description: SQL ID
                  required: true
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
                   description: get sql detail result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('sqlId', required=True, help="sqlId cannot be blank!")
        parser.add_argument('startTimeTs', required=True, help="startTimeTs cannot be blank!", type=int)
        parser.add_argument('endTimeTs', required=True, help="endTimeTs cannot be blank!", type=int)
        args = parser.parse_args()

        data = MonitorDBInfo().get_sql_detail(database_alias=args['databaseAlias'], user_id=self.user_id,
                                              sql_id=args['sqlId'], start_time=args['startTimeTs'],
                                              end_time=args['endTimeTs'])
        return self.construct_success_response_entity(data=data)


class TableIndex(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(TableIndex, self).__init__(*args, **kwargs)

    def get(self):
        """
            get table index
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: tableName
                  type: string
                  description: 表名
                  required: true
            responses:
                200:
                   description: get table index result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('tableName', required=True, help="tableName cannot be blank!")
        args = parser.parse_args()

        data = MonitorDBInfo().get_table_index(database_alias=args['databaseAlias'], user_id=self.user_id,
                                               table_name=args['tableName'])

        return self.construct_success_response_entity(data=data)


class TableStatistics(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(TableStatistics, self).__init__(*args, **kwargs)

    def get(self):
        """
            get table statistics
            ---
            tags:
              - Monitor
            parameters:
                - in: query
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: query
                  name: tableName
                  type: string
                  description: 表名
                  required: true
            responses:
                200:
                   description: get table statistics result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('tableName', required=True, help="tableName cannot be blank!")
        args = parser.parse_args()

        data = MonitorDBInfo().get_table_statistics(database_alias=args['databaseAlias'], user_id=self.user_id,
                                                    table_name=args['tableName'])

        return self.construct_success_response_entity(data=data)


class DatabaseConnectionCheck(BaseAPI):

    def __init__(self, *args, **kwargs):
        super(DatabaseConnectionCheck, self).__init__(*args, **kwargs)

    def post(self):
        """
            database connection check
            ---
            tags:
              - Monitor
            parameters:
                - in: body
                  name: databaseAlias
                  type: string
                  description: 数据库别名
                  required: true
                - in: body
                  name: databaseName
                  type: string
                  description: 数据库名称
                  required: true
                - in: body
                  name: approveType
                  type: string
                  enum: ['pull', 'push', 'manual']
                  description: 数据采集的授权方式
                  required: true
                - in: body
                  name: approveScope
                  type: string
                  description: 数据采集的授权范围
                  required: false
                - in: body
                  name: hostIp
                  type: string
                  description: DB服务器地址
                  required: false
                - in: body
                  name: hostPort
                  type: string
                  description: DB服务器端口
                  required: false
                - in: body
                  name: userName
                  type: string
                  description: DB连接用户名
                  required: false
                - in: body
                  name: password
                  type: string
                  description: DB连接密码
                  required: false
            responses:
                200:
                   description: database connection check result
        """
        parser = reqparse.RequestParser(argument_class=APIArgument, bundle_errors=True)
        parser.add_argument('databaseAlias', required=True, help="databaseAlias cannot be blank!")
        parser.add_argument('databaseName', required=True, help="databaseName cannot be blank!")
        parser.add_argument('approveType', required=True,
                            choices=["pull", "push", "manual"],
                            help="approveType only supports pull/push/manual!")
        parser.add_argument('approveScope')
        parser.add_argument('hostIp')
        parser.add_argument('hostPort')
        parser.add_argument('userName')
        parser.add_argument('password')
        args = parser.parse_args()

        approve_type = args['approveType']
        if approve_type and approve_type == 'pull':
            if not args['approveScope'] or not args['hostIp'] or not args['hostPort'] \
                    or not args['userName'] or not args['password']:
                message = "When approveType != null, approveScope/hostIp/hostPort/userName/password is required"
                return self.construct_error_response_entity(code=400, message=message)
            else:
                message, grant_action, success = monitor_database_connection_check(
                    host_ip=args['hostIp'],
                    host_port=args['hostPort'],
                    user_name=args['userName'],
                    password=args['password'],
                    database_name=args['databaseName']
                )

                if success:
                    insert_monitor_database(args['databaseAlias'], self.user_id, approve_type, args['approveScope'],
                                            args['hostIp'], args['hostPort'], args['userName'], args['password'])

                data = {
                    "monitorUserCheckMessage": message,
                    "grantAction": grant_action,
                }

                return self.construct_success_response_entity(data=data)
        else:
            return self.construct_success_response_entity()
