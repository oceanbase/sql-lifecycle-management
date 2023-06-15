# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import re

re_trace = re.compile(r'''\/\*.*trace_id((?!\/\*).)*rpc_id.*\*\/''', re.VERBOSE)
re_annotation = re.compile(r'''\/\*((?!\/\*).)*\*\/''', re.VERBOSE)

re_interval = re.compile(
    r'''interval[\?\s\d]*(day|hour|minute|second|microsecond|week|month|quarter|year|second_microsecond|minute_microsecond|minute_second|hour_microsecond|hour_second|hour_minute|day_microsecond|day_second|day_minute|day_hour|year_month)''',
    re.VERBOSE)
re_force_index = re.compile(r'''force[\s]index[\s][(]\w+[)]''', re.VERBOSE)
re_cast_1 = re.compile(r'''cast\(.*?\(.*?\)\)''', re.VERBOSE)
re_cast_2 = re.compile(r'''cast\(.*?\)''', re.VERBOSE)
re_now = re.compile(r'''now\(\)''', re.VERBOSE)


class Utils(object):

    @staticmethod
    def remove_sql_text_affects_parser(sql):
        sql = sql.lower()
        sql = Utils.remove_hint_and_annotate(sql)
        sql = Utils.replace_interval_day(sql)
        sql = Utils.remove_force_index(sql)
        sql = Utils.remove_cast(sql)
        sql = Utils.remove_now_in_insert(sql)
        return sql

    @staticmethod
    def remove_hint_and_annotate(sql):
        sql = sql.lower()
        sql = re.sub(re_annotation, '', sql)
        sql = re.sub(re_trace, '', sql)
        return sql

    @staticmethod
    def replace_interval_day(sql):
        sql = sql.lower()
        sql = re.sub(re_interval, '?', sql)
        return sql

    @staticmethod
    def remove_force_index(sql):
        sql = sql.lower()
        sql = re.sub(re_force_index, '', sql)
        return sql

    @staticmethod
    def remove_cast(sql):
        sql = sql.lower()
        sql = re.sub(re_cast_1, '?', sql)
        sql = re.sub(re_cast_2, '?', sql)
        return sql

    @staticmethod
    def remove_now_in_insert(sql):
        sql = sql.lower().lstrip()
        if sql.startswith('insert'):
            sql = re.sub(re_now, '?', sql)
        return sql

    @staticmethod
    def get_db_id(database_alias, user_id):
        return database_alias + '-' + user_id


def fun_diff_secs(date1, date2):
    """ calculate the second gap for date2 - date1, datetime type """
    secs = int(round((date2 - date1).seconds))
    days = int((date2 - date1).days * 24 * 60 * 60)
    return days + secs


def div_list(ls, n):
    """ Shard list group to sub_list """
    ls_len = len(ls)
    if n <= 0 or 0 == ls_len:
        return []
    if n > ls_len:
        return []
    elif n == ls_len:
        return [[i] for i in ls]
    else:
        j = int(ls_len / n)
        k = ls_len % n
        ls_return = []
        for i in range(0, (n - 1) * j, j):
            ls_return.append(ls[i:i + j])
        ls_return.append(ls[(n - 1) * j:])
        return ls_return
