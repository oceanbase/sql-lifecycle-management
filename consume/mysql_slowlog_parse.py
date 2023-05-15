# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import hashlib
import os
import re
import sys

from common.logger import Logger
from consume.file_parse_common import get_encoding
from consume.mysql_logparser_base import MysqlSlowLogParse
from optimizer.formatter import format_sql
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)

# Determine whether the sql content has trace_id, if it needs to be truncated
re_trace = re.compile(r'''\/\*.*trace_id.*rpc_id.*\*\/.*''', re.VERBOSE)
re_annotation = re.compile(r'''^\/\*.*\*\/.*''', re.VERBOSE)
re_hint = re.compile(r"\/\*.*\*\/")


class SlowQueryParser(object):
    """ parse mysql slow query log and turn it into a sql log stream
        :param
            log_file: slow query logfile name
            db_version: default 5.6
            sort: global=total_time, indicator_name like avg_query_time, default is null
    """

    def __init__(self, log_file, db_version='5.6', sort=''):
        self.log_file = log_file
        self.db_version = db_version
        self.sort = sort

    def pattern(self, sql):
        """ parameterize sql values for unifing sql pattern """
        if not sql:
            raise ValueError("Invalid sql: %s" % sql)
        statement = sql
        sql_hint = re_hint.findall(statement)
        if sql_hint:
            sql_hint = sql_hint[0].strip()
            statement = statement.replace(sql_hint, '')
        # Parameterize sql, normalize the value, and generate a sql_id
        # Currently only the select syntax is supported
        if statement.strip().lower().startswith('select ') \
                or statement.strip().lower().startswith('update ') \
                or statement.strip().lower().startswith('delete ') :
            try:
                statement_node = ParserUtils.parameterized_query(parser.parse(statement))
                statement = format_sql(statement_node, 0)
            except Exception as e:
                log.error(statement)
                log.exception(e)

        sql_id = hashlib.md5(statement.encode('utf-8')).hexdigest().upper()
        return sql_id, statement

    def skip_sql(self, sql):
        """
        skip sql:
            use database_name;
            SET timestamp=1601013923;
        """
        skip_patterns = ['use ', 'SET timestamp']
        for p in skip_patterns:
            if sql.startswith(p):
                sql = sql[sql.find(';') + 1:].strip()
        return sql.strip(';')

    def cutoff_sql(self, sql):
        """ sql is too long, cut off to remain head and tail """
        if len(sql) > 4000:
            return sql[0:2000] + '...' + sql[-2000:]
        return sql

    def group_sql(self):
        """ After parameterizing the sql, normalize it, and then group by the normalized sql """
        ret = {}
        # get file encoding
        read_encoding = get_encoding(self.log_file)
        # group by normalized sql_id
        with open(self.log_file, "r", encoding=read_encoding) as f:
            for e in MysqlSlowLogParse(f, self.db_version):
                if not e.query_time:
                    continue
                sql_id = ''
                try:
                    # skip use and set timestamp
                    sql_text = self.skip_sql(e.query)
                    if not sql_text:
                        continue
                    # need to remove the influence of trace_id,
                    # trace_id is different from each other and the influence is normalized
                    m1 = re.search(re_trace, sql_text)
                    m2 = re.search(re_annotation, sql_text.lstrip())
                    if m1 or m2:
                        sql_text = sql_text[sql_text.index(' */') + 3:]
                    # get normalized sql_id parameterized with sql text
                    sql_id, statement = self.pattern(sql_text)
                    e.query = sql_text
                except Exception as e:
                    log.exception(e)
                    pass
                if sql_id:
                    if sql_id not in ret:
                        ret[sql_id] = []
                    ret[sql_id].append(e)
        return ret

    def calc_stats(self):
        """ calculate performance consumption group by sql_id """
        slow_queries = self.group_sql()
        # calculate grouped aggregate values ​​for each sql_id
        ret = {}
        for sql_id, entry_list in slow_queries.items():
            max_query_time = 0
            sum_query_time = 0
            for e in entry_list:
                sum_query_time += e.query_time
                if not max_query_time or max_query_time < e.query_time:
                    max_query_time = e.query_time
            entry = {
                'org': entry_list[0],
                'avg_query_time': sum_query_time / len(entry_list),
                'max_query_time': max_query_time,
                'first_execute_time': entry_list[0].datetime.strftime("%Y-%m-%d %H:%M:%S"),
                'last_execute_time': entry_list[-1].datetime.strftime("%Y-%m-%d %H:%M:%S"),
                'count': len(entry_list),
                'sql_id': sql_id,
                'sql_text': self.cutoff_sql(entry_list[0].query.strip())
            }
            ret[sql_id] = entry
        return ret

    def parser_from_log(self):
        stats = self.calc_stats()
        res = []
        for sql_id, entry in stats.items():
            res.append(entry)
        if res and self.sort == 'total_time':
            res = sorted(res, reverse=True, key=lambda x: x['avg_query_time'] * x['count'])
        elif res and self.sort and self.sort in res[0]:
            res = sorted(res, reverse=True, key=lambda x: x[self.sort])
        return res
