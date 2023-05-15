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

import json
import unittest

from metadata.metadata_utils import MetaDataUtils
from optimizer.optimizer import Optimizer


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.catalog_json = """{"columns": [{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"cluster","type":"varchar(32)","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"tenant_name","type":"varchar(64)","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"svr_ip","type":"varchar(64)","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"sql_id","type":"varchar(128)","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"batch_time","type":"datetime","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"start_time","type":"datetime","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"end_time","type":"datetime","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"exections","type":"bigint(20)","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"fail_times","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"rpc_count","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"remote_plans","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"miss_plans","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"elapsed_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"execute_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"cpu_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"queue_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"netwait_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"iowait_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"getplan_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"return_rows","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"logical_reads","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"gmt_create","type":"timestamp","nullable":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"affected_rows","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"sql_type","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"max_elapsed_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"retry_cnt","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"decode_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"total_wait_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"app_wait_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"concurrency_wait_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"schedule_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"row_cache_hit","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"bloom_filter_cache_hit","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"block_cache_hit","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"block_index_cache_hit","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"disk_reads","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"memstore_read_row_count","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"ssstore_read_row_count","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"event","type":"varchar(80)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"client_ip","type":"varchar(40)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"user_name","type":"varchar(80)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"db_name","type":"varchar(40)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"section","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"plan_id","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"max_cpu_time","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"table_scan","type":"tinyint(4)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"request_id","type":"bigint(20)","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"exec_ps","type":"double","nullable":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"plan_hash","type":"bigint(20) unsigned","nullable":true}], "indexes": [{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"cluster",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"tenant_name",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"end_time",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"sql_id",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"svr_ip",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"PRIMARY","column":"start_time",
    "cardinality":0,"unique":true},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"idx_sql_id","column":"sql_id",
    "cardinality":0,"unique":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"idx_sql_id","column":"end_time",
    "cardinality":0,"unique":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"idx_cluster_time","column":"cluster",
    "cardinality":0,"unique":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"idx_cluster_time","column":"end_time",
    "cardinality":0,"unique":false},{"schema":"luli1","table":"ob_topsql_baseline",
    "name":"idx_end_time","column":"end_time",
    "cardinality":0,"unique":false}], "tables":[{"schema":"luli1","table":"ob_topsql_baseline","rows":0,"engine":"InnoDB"}],
    "version": "5.7.36"}"""
        self.sql = """SELECT 
                sum(exections) exections,sum(section) section,count(*) points,
                (case when sum(section)=0 then round(avg(exec_ps),2) else round(sum(exections)/sum(section),2) end) exec_ps,
                round(sum(elapsed_time*exections)/sum(exections)) elapsed_time,
                round(sum(cpu_time*exections)/sum(exections)) cpu_time,
                max(cpu_time) max_cpu_time,
                round(sum(execute_time*exections)/sum(exections)) execute_time,
                round(sum(return_rows*exections)/sum(exections)) return_rows,
                round(sum(affected_rows*exections)/sum(exections)) affected_rows,
                round(sum(logical_reads*exections)/sum(exections)) logical_reads,
                round(sum(memstore_read_row_count*exections)/sum(exections)) memstore_read,
                round(sum(ssstore_read_row_count*exections)/sum(exections)) ssstore_read,
                max(user_name) user_name,
                max(client_ip) client_ip,
                max(db_name) db_name,
                max(table_scan) table_scan,
                max(svr_ip) svr_ip
                FROM ob_topsql_baseline
                WHERE cluster = ?
                and tenant_name = ?
                and sql_id = ?
                and end_time >= ?
                and end_time <  ? """
        self.catalog_object = MetaDataUtils.json_to_catalog(json.loads(self.catalog_json))
        self.optimizer = Optimizer()

    def test_optimize(self):
        index_optimization_recommendation_list, development_specification_recommendation_list, after_sql_rewrite_formatter = self.optimizer.optimize(
            self.sql, self.catalog_object)
        assert index_optimization_recommendation_list == [{
            'index_recommendation': "Among the existing indexes, the optimal index is: PRIMARY(['cluster', 'tenant_name', 'end_time', 'sql_id', 'svr_ip', 'start_time'])",
            'diagnosis_reason': "Query Range : ['cluster', 'tenant_name', 'end_time'] , Index Back : False , Interesting Order : False"},
            {
                'index_recommendation': 'alter table ob_topsql_baseline add index idx_sqless_cluster_tenant_name_sql_id_end_time(cluster,tenant_name,sql_id,end_time)',
                'diagnosis_reason': 'This is a better query range index'}]

    def test_optimize_like(self):
        catalog_json = """
        {   
            "columns": 
                [
                    {"schema":"sqless_test","table":"sqless_base","name":"a","type":"int(2)","nullable":false},
                    {"schema":"sqless_test","table":"sqless_base","name":"b","type":"int(2)","nullable":false}
                ], 
            "indexes": 
                [
                    {"schema":"sqless_test","table":"sqless_base","name":"PRIMARY","column":"c","cardinality":1,"unique":true},
                    {"schema":"sqless_test","table":"sqless_base","name":"test","column":"d","cardinality":1,"unique":true}
                ], 
            "tables":
                [
                    {"schema":"sqless_test","table":"sqless_base","rows":1,"engine":"InnoDB"}
                ],
            "version": "5.7.36"}
        """
        optimizer = Optimizer()
        catalog_object = MetaDataUtils.json_to_catalog(json.loads(catalog_json))
        sql1 = """select * from sqless_base where d like 'a%' """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql1, catalog_object)
        assert index_optimization_recommendation_list == [{
            'index_recommendation': 'Among the existing indexes, the optimal index is: test(d)',
            'diagnosis_reason': "Query Range : ['d'] , Index Back : True , Interesting Order : False"}]
        sql2 = """select * from sqless_base where d like '%a%' """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql2, catalog_object)
        assert index_optimization_recommendation_list == [
            {'index_recommendation': 'Among the existing indexes, the optimal index is: PRIMARY(c)',
             'diagnosis_reason': 'Query Range : [] , Index Back : False , Interesting Order : False'}]
        sql3 = """select * from sqless_base where d like '%a' """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql3, catalog_object)
        assert index_optimization_recommendation_list == [
            {'index_recommendation': 'Among the existing indexes, the optimal index is: PRIMARY(c)',
             'diagnosis_reason': 'Query Range : [] , Index Back : False , Interesting Order : False'}]

    def test_optimize_qm(self):
        catalog_json = """
        {
        "columns": 
            [
                {"schema":"luli1","table":"cm_relation","name":"db_id","type":"bigint(20)","nullable":false,"collation":""},
                {"schema":"luli1","table":"cm_relation","name":"tnt_inst_id","type":"varchar(8)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"primary_id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"slave_id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"rel_type","type":"varchar(6)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"rel_biz_type","type":"varchar(6)","nullable":false,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"ext_info","type":"varchar(512)","nullable":true,"collation":"utf8_general_ci"},
                {"schema":"luli1","table":"cm_relation","name":"gmt_create","type":"datetime","nullable":false,"collation":""},
                {"schema":"luli1","table":"cm_relation","name":"gmt_modified","type":"datetime(3)","nullable":false,"collation":""},
                {"schema":"luli1","table":"cm_relation","name":"status","type":"varchar(1)","nullable":false,"collation":"utf8_general_ci"}
            ], 
        "indexes": 
            [
                {"schema":"luli1","table":"cm_relation","name":"PRIMARY","column":"db_id","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"uk_id","column":"id","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"uk_psrr","column":"primary_id","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"uk_psrr","column":"slave_id","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"uk_psrr","column":"rel_type","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"uk_psrr","column":"rel_biz_type","index_type":"btree","cardinality":0,"unique":true},
                {"schema":"luli1","table":"cm_relation","name":"idx_primary","column":"primary_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_slave","column":"slave_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_status","column":"status","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_status","column":"primary_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_status","column":"slave_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_prr","column":"primary_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_prr","column":"rel_type","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_prr","column":"rel_biz_type","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_srsi","column":"slave_id","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_srsi","column":"rel_type","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_srsi","column":"status","index_type":"btree","cardinality":0,"unique":false},
                {"schema":"luli1","table":"cm_relation","name":"idx_srsi","column":"id","index_type":"btree","cardinality":0,"unique":false}
            ], 
        "tables":
            [
                {"schema":"luli1","table":"cm_relation","rows":10000, "type":"BASE TABLE","engine":"InnoDB","collation":"utf8_general_ci"}
            ],
        "server_name": "dbadmin.eu95", "version": "5.7.36"}
"""
        sql = """select * FROM cm_relation    WHERE status = ?               AND primary_id = ?                     AND rel_type = ?                AND rel_biz_type = ?"""
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = Optimizer().optimize(sql, MetaDataUtils.json_to_catalog(json.loads(catalog_json)))
        assert index_optimization_recommendation_list == [{
            'index_recommendation': "Among the existing indexes, the optimal index is: idx_prr(['primary_id', 'rel_type', 'rel_biz_type'])",
            'diagnosis_reason': "Query Range : ['primary_id', 'rel_type', 'rel_biz_type'] , Index Back : True , Interesting Order : False"}]

    def test_optimize_update_delete(self):
        optimizer = Optimizer()
        sql1 = """update sqless_base set nick=1231 where a = 1 and b = 2 """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql1, None)

        assert index_optimization_recommendation_list == [
            {'index_recommendation': 'alter table sqless_base add index idx_sqless_a_b(a,b)',
             'diagnosis_reason': 'This is a better query range index'}]

        sql2 = """delete from sqless_base where a = 1 and b = 2 """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql2, None)

        assert index_optimization_recommendation_list == [
            {'index_recommendation': 'alter table sqless_base add index idx_sqless_a_b(a,b)',
             'diagnosis_reason': 'This is a better query range index'}]

    def test_optimize_index_recommendation(self):
        optimizer = Optimizer()
        sql1 = """
        SELECT out_trade_no FROM `scardcenter02`.offlinepay_order_log WHERE actual_order_time > ? AND card_type = ? GROUP BY out_trade_no HAVING COUNT(*) > ? LIMIT ?
        """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql1, None)

        assert index_optimization_recommendation_list == [{
                                                              'index_recommendation': 'alter table offlinepay_order_log add index idx_sqless_card_type_actual_order_time(card_type,actual_order_time)',
                                                              'diagnosis_reason': 'This is a better query range index'}]


    def test_optimize_index_recommendation2(self):
        optimizer = Optimizer()
        sql1 = """
        SELECT * FROM t1 WHERE  t1.c1 IN (?) AND t1.c2 = ? AND t1.c3 > ? and t1.c4 not in (select t2.c5 from t2 where t2.c5 in (?,?))
        """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql1, None)

        assert index_optimization_recommendation_list == [{'index_recommendation': 'alter table t1 add index idx_sqless_c1_c2_c3(c1,c2,c3)', 'diagnosis_reason': 'This is a better query range index'}, {'index_recommendation': 'alter table t2 add index idx_sqless_c5(c5)', 'diagnosis_reason': 'This is a better query range index'}]


    def test_1(self):
        optimizer = Optimizer()
        sql1 = """
        SELECT * FROM t1 WHERE  t1.c1 IN (?) AND t1.c2 = ? AND t1.c3 > ? and t1.c4 not in (select t2.c5 from t2 where t2.c5 in (?,?))
        """
        index_optimization_recommendation_list, \
        development_specification_recommendation_list, \
        after_sql_rewrite_formatter = optimizer.optimize(sql1, None)

        assert index_optimization_recommendation_list == [{'index_recommendation': 'alter table t1 add index idx_sqless_c1_c2_c3(c1,c2,c3)', 'diagnosis_reason': 'This is a better query range index'}, {'index_recommendation': 'alter table t2 add index idx_sqless_c5(c5)', 'diagnosis_reason': 'This is a better query range index'}]


if __name__ == '__main__':
    unittest.main()
