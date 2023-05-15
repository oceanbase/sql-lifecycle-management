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

from optimizer.formatter import format_sql
from parser.mysql_parser import parser
from parser.parser_utils import ParserUtils


class MyTestCase(unittest.TestCase):

    def test_get_filter_column(self):
        sql = "select name,max(age),count(*),avg(age) from blog b join a on a.id = blog.id " \
              "where a.b = 1 and b.c = 2 group by name,age " \
              "having count(*)>2 and avg(age)<20 order by a asc,b desc limit 1,10"
        visitor = ParserUtils.format_statement(parser.parse(sql))
        table_list = visitor.table_list
        projection_column_list = visitor.projection_column_list
        order_list = visitor.order_list
        min_max_list = visitor.min_max_list
        limit_number = visitor.limit_number
        assert table_list == [{'table_name': 'blog', 'alias': 'b',
                               'filter_column_list': [{'column_name': 'id', 'opt': '='},
                                                      {'column_name': 'c', 'opt': '='}]},
                              {'table_name': 'a', 'alias': '', 'filter_column_list': [{'column_name': 'id', 'opt': '='},
                                                                                      {'column_name': 'b',
                                                                                       'opt': '='}]}]
        assert projection_column_list == ['name', 'age', 'count(*)', 'age']
        assert order_list == [{'ordering': 'asc', 'column_name': 'a'}, {'ordering': 'desc', 'column_name': 'b'}]
        assert min_max_list == ['age']
        assert limit_number == '10'

    def test_get_filter_column2(self):
        sql = """SELECT
                                    tars_sqldiag_all.cluster,
                                    tars_sqldiag_all.tenant_name,
                                    tars_sqldiag_all.sql_id,
                                    tars_sqldiag_all.diag_type,
                                    max(tars_sqldiag_all.diag) diag,
                                    max(tars_sqldiag_all.sql_text) sql_text,
                                    max(tars_sqldiag_all.svr_ip) svr_ip,
                                    substr(max(tars_sqldiag_all.request_time),
                                    1,
                                    19) request_time,
                                    sum(tars_sqldiag_all.exections) exections,
                                    avg(tars_sqldiag_all.elapsed_time) elapsed_time,
                                    avg(tars_sqldiag_all.execute_time) execute_time,
                                    (CASE 
                                        WHEN avg(tars_sqldiag_all.cpu_time) < 0 THEN 0 
                                        ELSE avg(tars_sqldiag_all.cpu_time) END) cpu_time,
                                    (CASE 
                                        WHEN max(tars_sqldiag_all.max_cpu_time) < 0 THEN 0 
                                        ELSE max(tars_sqldiag_all.max_cpu_time) END) max_cpu_time,
                                    max(tars_sqldiag_all.user_name) user_name,
                                    max(tars_sqldiag_all.client_ip) client_ip,
                                    max(tars_sqldiag_all.db_name) db_name,
                                    max(tars_sqldiag_all.plan_info) plan_info,
                                    max(tars_sqldiag_all.table_name) table_name,
                                    max(tars_sqldiag_all.sql_mode) sql_mode,
                                    max(tars_sqldiag_all.sql_hash) sql_hash 
                                FROM
                                tars_sqldiag_all 
                                WHERE
                                tars_sqldiag_all.diag_type IN (
                                    ?,?
                                ) 
                                AND tars_sqldiag_all.request_time >= ?
                                AND tars_sqldiag_all.request_time <= ? 
                                AND EXISTS (
                                    SELECT
                                        1 
                                    FROM
                                        tars_obdeploy_group 
                                    WHERE
                                        (
                                            tars_obdeploy_group.deploy_group = ?
                                        ) 
                                        AND (
                                            tars_sqldiag_all.cluster = tars_obdeploy_group.cluster
                                        )
                                ) 
                                GROUP BY
                                tars_sqldiag_all.cluster,
                                tars_sqldiag_all.tenant_name,
                                tars_sqldiag_all.sql_id,
                                tars_sqldiag_all.diag_type 
                                ORDER BY
                                tars_sqldiag_all.cluster,
                                tars_sqldiag_all.tenant_name,
                                tars_sqldiag_all.sql_id,
                                tars_sqldiag_all.diag_type """
        visitor = ParserUtils.format_statement(parser.parse(sql))
        table_list = visitor.table_list
        projection_column_list = visitor.projection_column_list
        order_list = visitor.order_list
        min_max_list = visitor.min_max_list
        in_count_list = visitor.in_count_list
        assert table_list == [{'table_name': 'tars_sqldiag_all', 'alias': '',
                               'filter_column_list': [{'column_name': 'diag_type', 'opt': 'in'},
                                                      {'column_name': 'request_time', 'opt': '>='},
                                                      {'column_name': 'request_time', 'opt': '<='},
                                                      {'column_name': 'cluster', 'opt': '='}]},
                              {'table_name': 'tars_obdeploy_group', 'alias': '',
                               'filter_column_list': [{'column_name': 'deploy_group', 'opt': '='},
                                                      {'column_name': 'cluster', 'opt': '='}]}]
        assert projection_column_list == ['cluster', 'tenant_name', 'sql_id', 'diag_type', 'diag', 'sql_text', 'svr_ip',
                                          'exections', 'elapsed_time', 'execute_time', 'user_name', 'client_ip',
                                          'db_name', 'plan_info', 'table_name', 'sql_mode', 'sql_hash']
        assert order_list == [{'ordering': 'asc', 'column_name': 'cluster'},
                              {'ordering': 'asc', 'column_name': 'tenant_name'},
                              {'ordering': 'asc', 'column_name': 'sql_id'},
                              {'ordering': 'asc', 'column_name': 'diag_type'}]
        assert min_max_list == ['diag', 'sql_text', 'svr_ip', 'user_name', 'client_ip', 'db_name', 'plan_info',
                                'table_name', 'sql_mode', 'sql_hash']
        assert in_count_list == [2]

    def test_parameterized_query(self):
        sql = "select name,max(age),count(*),avg(age) from blog b join a on a.id = blog.id " \
              "where a.b = 1 and b.c = 2 and a.d in ('2','3','6') group by name,age " \
              "having count(*)>2 and avg(age)<20 order by a asc,b desc limit 3,10"
        statement_node = ParserUtils.parameterized_query(parser.parse(sql))
        statement = format_sql(statement_node, 0)

    def test_parameterized_query2(self):
        sql = """
        SELECT          
        server_release_repo.weight,    
        server_release_repo.integrate,    
        server_release_repo.create_tag_flag,    
        server_release_repo.merge_record_id,    
        case server_release_repo.merge_record_id       
        when 0 then 0       
        when -1 then 1       
        when -2 then 15       
        else merge_record.merge_result       END as merge_result,   server_release_repo.completed,    server_release_repo.create_time,    server_release_repo.update_time      FROM server_release_repo left join merge_record on server_release_repo.merge_record_id = merge_record.id     WHERE      1 = 1                and            integrate = 0              and            completed = 1             and            deleted = 0          and       merge_record_id != -1
        """
        statement_node = ParserUtils.parameterized_query(parser.parse(sql))
        statement = format_sql(statement_node, 0)

    def test_parameterized_query3(self):
        sql = """select  id,gmt_create,gmt_modified,proj_code,matter_code,'ATUSER' act_type,content,operator,operator_no,status,biz_code,biz_id,content_detail   from lc_opr_biz_activity    where id in (    select max(id) id from lc_opr_biz_activity t1    join  (     select act_type ,biz_activity_id,task_id from lc_opr_schedule      where user_id = '291909' and status = '00' and act_type = 'ATUSER' and      matter_code  = 'M210713P0689I00007' and task_id is not null     ) t2        on t1.id = t2.biz_activity_id         group by t2.task_id    )      union     select id,gmt_create,gmt_modified,proj_code,matter_code,act_type,content,operator,operator_no,status,biz_code,biz_id,content_detail from lc_opr_biz_activity where id in(   select max(id) id   from lc_opr_biz_activity    where  matter_code = 'M210713P0689I00007' and biz_code = 'TASK'     and biz_id not in      (       select distinct task_id from lc_opr_schedule        where user_id = '291909' and status = '00' and act_type = 'ATUSER' and        matter_code  = 'M210713P0689I00007' and task_id is not null           )     group by biz_id     )"""
        statement_node = ParserUtils.parameterized_query(parser.parse(sql))
        statement = format_sql(statement_node, 0)

    def test_subquery_expression(self):
        sql = """
        SELECT COUNT(*) FROM ( SELECT * FROM customs_script_match_history LIMIT ? ) a
        """
        statement_node = ParserUtils.format_statement(parser.parse(sql))

    def test_sql_1(self):
        sql = """
        SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk 
        FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 
        WHERE 
        t2.node_name = t1.node 
        AND t2.gmt_create = ? 
        AND t1.idc IS NOT NULL 
        AND t1.cluster_name NOT IN (?) 
        AND t2.ds = ? 
        AND t1.idc IN (?) 
        AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION 
        SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk 
        FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 
        WHERE 
        t2.node_name = t1.node 
        AND t2.gmt_create = ? 
        AND t1.idc IS NOT NULL 
        AND t1.cluster_name NOT IN (?) 
        AND t2.ds = ? 
        AND t1.idc IN (?) 
        AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) 
        UNION 
        SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk 
        FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 
        WHERE 
        t2.node_name = t1.node 
        AND t2.gmt_create = ? 
        AND t1.idc IS NOT NULL 
        AND t1.cluster_name NOT IN (?) 
        AND t2.ds = ? AND t1.idc IN (?) 
        AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? ) UNION SELECT t1.idc, t2.ds AS ds, SUM(t2.yhat) AS disk FROM `sync_mt_mysql_meta` t1, space_used_forecast_per_inst t2 WHERE t2.node_name = t1.node AND t2.gmt_create = ? AND t1.idc IS NOT NULL AND t1.cluster_name NOT IN (?) AND t2.ds = ? AND t1.idc IN (?) AND t1.nc_ip NOT IN ( SELECT DISTINCT ip FROM yusuan_unires_docker_nc_host WHERE pool LIKE ? )
        """
        statement_node = ParserUtils.format_statement(parser.parse(sql))

    def test_recursion_error(self):
        sql = """SELECT id, `table_name`, version, primary_id, template , template_md5, security_level, `nullable`, status, `param_group` , description, operator, global_id, govern_type, utc_create , utc_modified FROM param_template WHERE (table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ? OR table_name = ? AND version = ?) AND status IN (?) ORDER BY table_name ASC, utc_create DESC LIMIT ?, ?"""
        statement = parser.parse(sql)
        statement_node = ParserUtils.format_statement(statement)

    def test_in_subquery(self):
        sql = 'select sum(cost) from costs where eventtype = \'treatment\' and eventid in ' \
              '(select treatmentid from treatment where treatmentname = \'bleeding scan\')'
        visitor = ParserUtils.format_statement(parser.parse(sql))
        table_list = visitor.table_list
        assert table_list == [{'table_name': 'costs', 'alias': '',
                               'filter_column_list': [{'column_name': 'eventtype', 'opt': '='},
                                                      {'column_name': 'eventid', 'opt': 'in'}]},
                              {'table_name': 'treatment', 'alias': '',
                               'filter_column_list': [{'column_name': 'treatmentname', 'opt': '='}]}]


if __name__ == '__main__':
    unittest.main()
