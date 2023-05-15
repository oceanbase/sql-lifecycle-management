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

from consume.file_parse_common import get_encoding
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils
from common.utils import Utils


class MyTestCase(unittest.TestCase):

    def test_simple_sql(self):
        """ Batch run case file to test sql parse """
        f_path = os.getcwd()
        file_name = f_path + '/test/parser/mysql_testcase/sql_test_case_1.txt'
        read_encoding = get_encoding(file_name)
        try:
            with open(file_name, 'r', encoding=read_encoding) as f:
                lines = f.readlines()
                cnt = 0
                # 0 is unlimit
                stop_cnt = 3000
                for line in lines:
                    cnt += 1
                    if cnt > stop_cnt:
                        break
                    try:
                        sql_text = line.strip()[1:-1]
                        if sql_text.startswith('"'):
                            sql_text = sql_text[1:]
                        sql_text = Utils.remove_sql_text_affects_parser(sql_text)
                        # test parse
                        result = parser.parse(sql_text)
                        # test format
                        visitor = ParserUtils.format_statement(result)
                    except Exception as e:
                        if line.strip():
                            print(line)
        except UnicodeDecodeError:
            error_msg = f"invalid UnicodeDecodeError {read_encoding}"
            print(error_msg)


if __name__ == '__main__':
    unittest.main()
