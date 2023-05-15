# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from glob import glob
from typing import List

from tqdm import tqdm

from consume.mybatis_xmlparse_base import MybatisXmlFile


class MybatisXmlParser(object):
    """ parse mybatis xml sqlmap file to a sql list
        :param
            file_name: mybatis sqlmap xml file name
        :return
            sql_list: sql list, {"line":'', "sql_id":"", "xml":"", "sql_text":"", "error_msg":""}
    """

    def __init__(self):
        pass

    def parse_mybatis_xml_file(self, file_name: str) -> List:
        """
        parse xml get sql list
        """
        xml_parse = MybatisXmlFile(file_name)
        # load xml
        is_valid, tree, error_msg = xml_parse.load_xml_file()
        if not is_valid:
            raise ValueError(f"skip invalid mybatis xml file: {file_name}")
        elif not tree:
            raise ValueError(f"load xml file content failed: {file_name} error: {error_msg}")
        # parse xml
        sql_list = xml_parse.parse_xml_content(tree)
        return sql_list

    def glob_path_file_and_parse(self, file_path: str) -> List:
        sql_list = []
        scan_pattern = file_path + '/*.xml'
        pbar = tqdm(glob(scan_pattern, recursive=True))
        desc_count = 0
        for file_name in pbar:
            desc_count += 1
            sub_list = self.parse_mybatis_xml_file(file_name)
            if sub_list:
                sql_list.extend(sub_list)
        return sql_list
