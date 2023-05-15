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

from metadata.catalog import Catalog
from metadata.metadata_utils import MetaDataUtils
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils


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
            and end_time <  ?"""
        self.catalog_object = MetaDataUtils.json_to_catalog(json.loads(self.catalog_json))
        visitor = ParserUtils.format_statement(parser.parse(self.sql))
        self.table_list = visitor.table_list
        self.projection_column_list = visitor.projection_column_list
        self.order_list = visitor.order_list
        self.min_max_list = visitor.min_max_list

    def test_json_to_catalog(self):
        assert isinstance(self.catalog_object, Catalog)

    def test_is_index_back(self):
        catalog_object = self.catalog_object
        is_index_back_list = []
        for _schema in catalog_object.table_list:
            _table_name = _schema.table_name
            _index_list = _schema.index_list
            for _table in self.table_list:
                filter_column_list = _table['filter_column_list']
                if _table['table_name'] == _table_name:
                    for _index in _index_list:
                        is_index_back = MetaDataUtils.is_index_back(_index.column_list, filter_column_list,
                                                                    self.projection_column_list,
                                                                    self.order_list, _index.index_type)
                        is_index_back_list.append(is_index_back)
        assert is_index_back_list == [False, True, True, True]

    def test_extract_range(self):
        catalog_object = self.catalog_object
        extract_range_list = []
        for _schema in catalog_object.table_list:
            _table_name = _schema.table_name
            _index_list = _schema.index_list
            for _table in self.table_list:
                filter_column_list = _table['filter_column_list']
                if _table['table_name'] == _table_name:
                    for _index in _index_list:
                        extract_range = MetaDataUtils.extract_range(_index.column_list, filter_column_list)
                        extract_range_list.append(extract_range)
        assert extract_range_list == [['cluster', 'tenant_name', 'end_time'], ['sql_id', 'end_time'],
                                      ['cluster', 'end_time'], ['end_time']]

    def test_has_interesting_order(self):
        catalog_object = self.catalog_object
        interesting_order_list = []
        extract_range_list = [['cluster', 'tenant_name', 'end_time'], ['sql_id', 'end_time'],
                              ['cluster', 'end_time'], ['end_time']]
        for _schema in catalog_object.table_list:
            _table_name = _schema.table_name
            _index_list = _schema.index_list
            for _table in self.table_list:
                filter_column_list = _table['filter_column_list']
                if _table['table_name'] == _table_name:
                    for _index in _index_list:
                        interesting_order = MetaDataUtils.has_interesting_order(_index.column_list, self.order_list,
                                                                                self.min_max_list, extract_range_list,
                                                                                filter_column_list)
                        interesting_order_list.append(interesting_order)
        assert interesting_order_list == [False, False, False, False]


if __name__ == '__main__':
    unittest.main()
