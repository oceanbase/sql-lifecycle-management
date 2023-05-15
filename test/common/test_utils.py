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

from common.utils import Utils


class MyTestCase(unittest.TestCase):

    def test_remove_hint_and_annotate(self):
        sql = """ /* table=pmt_ar_node_12,part_key=03 */ SELECT /* index(a,b) */      count(DISTINCT ID) as total   FROM OS_ROLE WHERE TNT_INST_ID = 'ALIPW3CN'   AND    (NM like CONCAT('%', 'CMR-LEADS', '%') or CODE like CONCAT('%','CMR-LEADS','%'))                AND    (TYPE_CODE = 'ROLE' or TYPE_CODE is null )             AND    st !='DELETE'      AND    (apply_mode in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    or (type_code = 'ROLE' AND 'PUBLIC' in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    AND apply_mode IS NULL))                         and                 isolation_key = 'TENANT_ALIPW3CN'"""
        sql = Utils.remove_hint_and_annotate(sql)
        assert sql.find('*') == -1

    def test_replace_interval_day(self):
        sql = """
        SELECT biz_id, operator, MAX(gmt_create) AS gmt_create FROM log WHERE type = ? AND gmt_create > date_sub(now(), INTERVAL ? DAY) GROUP BY biz_id
        """
        sql = Utils.replace_interval_day(sql)
        assert sql == """
        select biz_id, operator, max(gmt_create) as gmt_create from log where type = ? and gmt_create > date_sub(now(), ?) group by biz_id
        """

        sql = """
                SELECT interval,day FROM log WHERE type = ? AND gmt_create > date_sub(now(), INTERVAL ? DAY) 
                and gmt_create < date_sub(now(), INTERVAL ? DAY) GROUP BY biz_id
                """
        sql = Utils.replace_interval_day(sql)
        assert sql == """
                select interval,day from log where type = ? and gmt_create > date_sub(now(), ?) 
                and gmt_create < date_sub(now(), ?) group by biz_id
                """

        sql = """
        select * from a where b > now() - interval ? hour
        """
        sql = Utils.replace_interval_day(sql)
        assert sql == """
        select * from a where b > now() - ?
        """

    def test_remove_force_index(self):
        sql = """
        SELECT order_id, inst_apply_order_id FROM fund_trade_order FORCE INDEX (n_apply_order) WHERE order_status IN (?) AND switch_flag = ?
        """
        sql = Utils.remove_force_index(sql)
        assert sql == """
        select order_id, inst_apply_order_id from fund_trade_order  where order_status in (?) and switch_flag = ?
        """

    def test_remove_now_in_insert(self):
        sql = """INSERT IGNORE INTO bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) VALUES (now(), now(), ?, ?)"""
        sql = Utils.remove_now_in_insert(sql)
        assert sql == """insert ignore into bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) values (?, ?, ?, ?)"""

        sql = """ INSERT IGNORE INTO bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) VALUES (now(), now(), ?, ?)"""
        sql = Utils.remove_now_in_insert(sql)
        assert sql == """insert ignore into bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) values (?, ?, ?, ?)"""

    def test_hint(self):
        sql = """
        /* trace_id=0b7c9b6f16766093900011105128699,rpc_id=0.9ef939e.8.5 */                      
        SELECT /*+ index(midas_record_value idx_tenant_time) */                 DISTINCT(trace_id)             FROM                 midas_record_value where tenant='insttrade' and is_expired=1  order by gmt_modified asc limit 500        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        assert sql == """select                  distinct(trace_id)             from                 midas_record_value where tenant=\'insttrade\' and is_expired=1  order by gmt_modified asc limit 500        """


if __name__ == '__main__':
    unittest.main()
