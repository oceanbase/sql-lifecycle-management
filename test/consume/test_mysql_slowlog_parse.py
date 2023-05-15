# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest

from consume.mysql_slowlog_parse import SlowQueryParser


class MyTestCase(unittest.TestCase):

    def test_antgroup_56(self):
        f_path = os.getcwd()
        file_list = [
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_56_1.txt',
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_56_2.txt',
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_56_3.txt',
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_56_4.txt'
        ]
        # file_list = [ f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_56_3.txt' ]
        for log_file in file_list:
            print('log_file:',log_file)
            sort = 'total_time'
            query_parser = SlowQueryParser(log_file, '5.6', sort)
            sql_list = query_parser.parser_from_log()
            for q in sql_list:
                print('''%s, sql_id: %s, count: %s, avg_time: %.2fs, max_time: %.2fs, fist_exec: %s, query: %s''' % (
                    log_file,q['sql_id'],q['count'],q['avg_query_time'],
                    q['max_query_time'],q['first_execute_time'],q['sql_text']))
            print('####################################################################\n')
            #assert len(sql_list) == 1
            #assert sql_list[0]['sql_id'] == '375D9291343188CA3679208D1068CC4C'

    def test_community_57(self):
        f_path = os.getcwd()
        file_list = [
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_57_1.txt',
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_57_2.txt',
            f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_57_3.txt'
        ]
        # file_list = [ f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_57_1.txt' ]
        for log_file in file_list:
            print('log_file:',log_file)
            sort = 'avg_query_time'
            query_parser = SlowQueryParser(log_file, '5.7', sort)
            sql_list = query_parser.parser_from_log()
            for q in sql_list:
                print('''sql_id: %s, count: %s, avg_time: %.2fs, max_time: %.2fs, fist_exec: %s, query: %s''' % (
                    q['sql_id'],q['count'],q['avg_query_time'],
                    q['max_query_time'],q['first_execute_time'],q['sql_text']))
            print('total=',len(sql_list))
            print('####################################################################\n')
            #assert len(sql_list) == 1
            #assert sql_list[0]['sql_id'] == '4299FE1CADB5EF18E8455DF24FC49F59'


if __name__ == '__main__':
    unittest.main()
