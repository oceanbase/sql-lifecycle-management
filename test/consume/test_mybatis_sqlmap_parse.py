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
from lxml.etree import tostring
from consume.mybatis_xmlparse_base import MybatisXmlFile, get_include_define
from consume.mybatis_sqlmap_parse import MybatisXmlParser


class MyTestCase(unittest.TestCase):

    def test_include_define(self):
        f_path = os.getcwd()
        file_list = [f_path + '/test/consume/mybatis_xml/AffEventMapper.xml']
        for file_name in file_list:
            xml_parse = MybatisXmlFile(file_name)
            is_valid, tree, error_msg = xml_parse.load_xml_file()
            assert is_valid == True
            if not is_valid:
                print(f"skip invalid mybatis xml file: {file_name}")
                continue
            elif not tree:
                print(f"load xml file content failed: {file_name} error: {error_msg}")
                continue
            else:
                print(f"load xml file [{file_name}] success.")
            include_sql_dict = get_include_define(tree)
            if include_sql_dict:
                print('include_sql_dict:',include_sql_dict)
                for i in include_sql_dict.keys():
                    print('include_node: ',i,tostring(include_sql_dict[i]).decode())
            print('\n###############################################################\n')

    def test_xml_file(self):
        f_path = os.getcwd()
        file_list = [f_path + '/test/consume/mybatis_xml/InfoSchemaMapper.xml',
                     f_path + '/test/consume/mybatis_xml/AffEventMapper.xml',
                     f_path + '/test/consume/mybatis_xml/ObTopsqlBaselineMapper.xml']
        #file_list = [f_path + '/test/consume/mybatis_xml/AffEventMapper.xml']
        #file_list = [f_path + '/test/consume/mybatis_xml/InfoSchemaMapper.xml']
        for file_name in file_list:
            xml_parse = MybatisXmlFile(file_name)
            is_valid, tree, error_msg = xml_parse.load_xml_file()
            assert is_valid == True
            if not is_valid:
                print(f"skip invalid mybatis xml file: {file_name}")
                continue
            elif not tree:
                print(f"load xml file content failed: {file_name} error: {error_msg}")
                continue
            else:
                print(f"load xml file [{file_name}] success.")
            # include_sql_dict = get_include_define(tree)
            # if include_sql_dict:
            #     print('include_sql_dict:',include_sql_dict)
            #     for i in include_sql_dict.keys():
            #         print('include_node: ',i,tostring(include_sql_dict[i]).decode())
            sql_list = xml_parse.parse_xml_content(tree)
            for per_sql in sql_list:
                print(per_sql)
                print('sql_id: ',per_sql['sql_id'])
                print('error_msg: ',per_sql['error_msg'])
                print('xml: ',per_sql['xml'])
                print('sql_text: ',per_sql['sql_text'])
                print('\n**************************\n')
            print('\n###############################################################\n')

    def test_parse_xml_file(self):
        f_path = os.getcwd()
        file_list = [f_path + '/test/consume/mybatis_xml/InfoSchemaMapper.xml',
                     f_path + '/test/consume/mybatis_xml/AffEventMapper.xml',
                     f_path + '/test/consume/mybatis_xml/ObTopsqlBaselineMapper.xml']
        #file_list = [f_path + '/test/consume/mybatis_xml/AffEventMapper.xml']
        #file_list = [f_path + '/test/consume/mybatis_xml/InfoSchemaMapper.xml']
        xml_parse = MybatisXmlParser()
        for file_name in file_list:
            sql_list = xml_parse.parse_mybatis_xml_file(file_name)
            for per_sql in sql_list:
                print(per_sql)
                print('sql_id: ',per_sql['sql_id'])
                print('error_msg: ',per_sql['error_msg'])
                print('xml: ',per_sql['xml'])
                print('sql_text: ',per_sql['sql_text'])
                print('\n**************************\n')
            print('\n###############################################################\n')

    def test_parse_path(self):
        f_path = os.getcwd() + '/test/consume/mybatis_xml'
        xml_parse = MybatisXmlParser()
        sql_list = xml_parse.glob_path_file_and_parse(f_path)
        for per_sql in sql_list:
            print(per_sql)
            print('sql_id: ',per_sql['sql_id'])
            print('error_msg: ',per_sql['error_msg'])
            print('xml: ',per_sql['xml'])
            print('sql_text: ',per_sql['sql_text'])
            print('\n**************************\n')
        print('\n###############################################################\n')


if __name__ == '__main__':
    unittest.main()
