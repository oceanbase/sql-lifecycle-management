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

import unittest

from parser.mysql_parser import parser


class MyTestCase(unittest.TestCase):

    def test_create_table(self):
        result = parser.parse("""CREATE TABLE `tars_user_sql_hash_level` (
  `obregion_group` varchar(32) NOT NULL,
  `tenant_group` varchar(64) NOT NULL,
  `sql_hash` varchar(128) NOT NULL,
  `pure_dbname` varchar(128) DEFAULT NULL,
  `level` varchar(4) DEFAULT NULL COMMENT 'sql级别，当前分为三级：H, M, L',
  `limit_threshold` int(11) DEFAULT NULL COMMENT '限流阈值',
  `emp_id` varchar(16) DEFAULT NULL,
  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `cluster` varchar(32) DEFAULT NULL,
  `tenant_name` varchar(128) DEFAULT NULL,
  `sql_id` varchar(128) DEFAULT NULL,
  `can_limit_eleven` varchar(8) DEFAULT NULL,
  PRIMARY KEY (`obregion_group`, `tenant_group`, `sql_hash`),
  UNIQUE KEY `sql_id` (`cluster`, `tenant_name`, `sql_id`) BLOCK_SIZE 16384,
  KEY `pure_dbname` (`pure_dbname`) BLOCK_SIZE 16384) ENGINE=InnoDB DEFAULT CHARSET=utf8
  """)
        assert result['index_list'][0][0].value == '1.primary'
        assert result['index_list'][0][1] == 'PRIMARY'
        assert result['index_list'][0][2] == ['obregion_group', 'tenant_group', 'sql_hash']
        assert result['index_list'][1][0].value == '2.unique'
        assert result['index_list'][1][1] == 'sql_id'
        assert result['index_list'][1][2] == ['cluster', 'tenant_name', 'sql_id']
        assert result['index_list'][2][0].value == '3.normal'
        assert result['index_list'][2][1] == 'pure_dbname'
        assert result['index_list'][2][2] == ['pure_dbname']


if __name__ == '__main__':
    unittest.main()
