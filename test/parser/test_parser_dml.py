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

from src.common.utils import Utils
from src.parser.mysql_parser import parser as mysql_parser
from src.parser.oceanbase_parser import parser as oceanbase_parser
from src.parser.tree.expression import *
from src.parser.tree.statement import *


class MyTestCase(unittest.TestCase):

    def test_and_in_update(self):
        sql = """
        update foo set t1 = '1' and t2 = '2' where t3 = '3'
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_update_1(self):
        result = oceanbase_parser.parse("""
        update jss_alarm_def     
        set                
        scope_id = 5,         
        alarm_name = 'sparkmeta-jss同步刷新任务告警',         
        create_operator = '0005292026',
        is_delete = 0,
        gmt_modify = '2019-08-13 17:11:56.979'
        where alarm_id = 2000003
        """)
        assert isinstance(result, Statement)

    def test_update_set(self):
        result = oceanbase_parser.parse("""
        update t set a = 1, b = 2 where c = 3
        """)
        assert isinstance(result.set_list, list)
        assert isinstance(result.table, list)
        assert isinstance(result.where, ComparisonExpression)

    def test_insert(self):
        sql = """
              insert into t1 values(?,?,?)
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

        sql = """
              insert into t1(c1,c2,c3) values(?,?,?)
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

        sql = """
              insert into t1 select * from t2
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_lock_in_share_mode(self):
        sql = """
        INSERT IGNORE INTO ilimitcenter05.tp_48246_ogt_fc_lc_day (`id`, `tnt_inst_id`, `principal_id`, `principal_type`, `cumulate_code` , `stat_time`, `amount`, `day_count`, `reverse_amount`, `reverse_count` , `max_value`, `min_value`, `cumulate_properties`, `p1`, `p2` , `p3`, `p4`, `p5`, `p6`, `p7` , `p8`, `p9`, `p10`, `p11`, `p12` , `p13`, `p14`, `p15`, `properties_md5`, `gmt_create` , `gmt_modified`, `currency`, `version`) SELECT `id`, `tnt_inst_id`, `principal_id`, `principal_type`, `cumulate_code` , `stat_time`, `amount`, `day_count`, `reverse_amount`, `reverse_count` , `max_value`, `min_value`, `cumulate_properties`, `p1`, `p2` , `p3`, `p4`, `p5`, `p6`, `p7` , `p8`, `p9`, `p10`, `p11`, `p12` , `p13`, `p14`, `p15`, `properties_md5`, `gmt_create` , `gmt_modified`, `currency`, `version` FROM ilimitcenter05.fc_lc_day FORCE INDEX (`PRIMARY`) WHERE `id` > ? AND (`id` < ? OR `id` = ?) LOCK IN SHARE MODE
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = mysql_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_insert_now(self):
        sql = """
INSERT IGNORE INTO bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) VALUES (now(), now(), ?, ?)
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_delete_1(self):
        result = oceanbase_parser.parse("""
delete from execution_log          where                (                             record_id = 2000006         and           sub_job_id = -3                                             )
        """)
        assert isinstance(result, Statement)


if __name__ == '__main__':
    unittest.main()
