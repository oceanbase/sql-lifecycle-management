# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from consume.mysql_slowlog_parse import SlowQueryParser


class MySQLSlowlogParse():

    def __new__(cls):
        singleton = cls.__dict__.get('__singleton__')
        if singleton is not None:
            return singleton

        cls.__singleton__ = singleton = object.__new__(cls)

        return singleton

    def logparse(self, log_file, db_version, sort):
        query_parser = SlowQueryParser(log_file, db_version, sort)
        sql_list = query_parser.parser_from_log()
        return sql_list
