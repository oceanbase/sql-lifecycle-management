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
from src.parser.mysql_parser.parser import parser as mysql_parser
from src.parser.oceanbase_parser.parser import parser as oceanbase_parser
from src.parser.tree.expression import LikePredicate, ExistsPredicate
from src.parser.tree.relation import Join
from src.parser.tree.set_operation import Union
from src.parser.tree.statement import Statement


class MyTestCase(unittest.TestCase):
    def test_simple_sql(self):
        result = oceanbase_parser.parse(
            "select name,age,count(*),avg(age) from blog join a on a.id = blog.id "
            "where a.b = 1 and blog.c = 2 group by name,age "
            "having count(*)>2 and avg(age)<20 order by a asc,b desc limit 1 OFFSET 3",
        )
        assert isinstance(result, Statement)
        assert isinstance(result.query_body.from_, Join)

    def test_no_filter(self):
        result = oceanbase_parser.parse("select distinct name from a.blog")
        query_body = result.query_body
        assert (
            query_body is not None
            and query_body.limit == 0
            and query_body.where is None
        )

    def test_question_mark(self):
        result = oceanbase_parser.parse("select n from b where a = ?")
        assert isinstance(result, Statement)

    def test_like(self):
        result = oceanbase_parser.parse("SELECT name from blog where a like 'a' ")
        query_body = result.query_body
        assert isinstance(query_body.where, LikePredicate)

    def test_exists(self):
        result = oceanbase_parser.parse(
            """select name from blog where EXISTS (
                                    SELECT
                                        1 
                                    FROM
                                        c 
                                    WHERE
                                        d = ?
                                    )
                                        """
        )
        query_body = result.query_body
        assert isinstance(query_body.where, ExistsPredicate)

    def test_simple_sql2(self):
        result = oceanbase_parser.parse(
            """SELECT
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
        )
        assert isinstance(result, Statement)

    def test_inner_join(self):
        sql = """select 
       obe.event_id,
       obe.object as event_object,
       obe.event_descp,
       obe.level,
       obe.owner_id,
       obe.event_start_time,
	   TIMESTAMPDIFF(SECOND, obe.event_start_time,  obe.event_time)  as event_duration,
       obe.event_time,
       obe.event_summary,
       obe2.cnt as event_count
  from obevent obe
  inner join(
select max(id)  as id
  from obevent
 where event_id in (?,?)
 group by event_id) as obe2 on obe.id= obe2.id"""
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_subquery(self):
        result = oceanbase_parser.parse(
            """SELECT * 
     FROM CUSTOMERS 
     WHERE ID IN (SELECT ID 
                  FROM CUSTOMERS
                  WHERE SALARY > 4500)"""
        )
        assert isinstance(result, Statement)

    def test_distinct(self):
        result = oceanbase_parser.parse(
            """select max(id)  as id, COUNT(distinct uuid) as cnt
  from obevent"""
        )
        assert isinstance(result, Statement)

    def test_union(self):
        result = oceanbase_parser.parse(
            "SELECT country FROM Websites UNION SELECT country FROM apps ORDER BY country"
        )
        assert isinstance(result.query_body, Union)
        result = mysql_parser.parse(
            "SELECT country FROM Websites UNION SELECT country FROM apps ORDER BY country"
        )
        assert isinstance(result.query_body, Union)

    def test_union_all(self):
        result = oceanbase_parser.parse(
            "SELECT country FROM Websites UNION ALL SELECT country FROM apps ORDER BY country"
        )
        assert isinstance(result.query_body, Union)
        result = mysql_parser.parse(
            "SELECT country FROM Websites UNION ALL SELECT country FROM apps ORDER BY country"
        )
        assert isinstance(result.query_body, Union)

    def test_sql_1(self):
        result = oceanbase_parser.parse(
            """
        SELECT  role.ID, role.NM,         role.CODE,role.ORG_ID,role.domain_id,role.ADMINS,role.SCD_ADMINS,role.PRN_ID,role.PATH,role.TYPE_CODE,         role.DSC,role.ST,role.EXPR_TM,role.CRT_ID,role.CRT_NM,role.property,         role.MOD_ID, role.MOD_NM,role.GMT_CREATE,role.GMT_MODIFIED,role.TNT_INST_ID,role.MNG_MODE,role.APPLY_MODE, role.risk_memo         FROM OS_ROLE role         WHERE         role.TNT_INST_ID='ALIPW3CN'         AND     (role.TYPE_CODE = 'ROLE' or role.TYPE_CODE is null )             AND    role.st !='DELETE'      AND    (role.apply_mode in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    or (role.type_code = 'ROLE' AND 'PUBLIC' in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    AND role.apply_mode IS NULL))                         and                 role.isolation_key = 'TENANT_ALIPW3CN'                         order by role.id desc limit 0, 10
        """
        )
        assert isinstance(result, Statement)

    def test_sql_2(self):
        result = oceanbase_parser.parse(
            """
        SELECT      count(DISTINCT ID) as total   FROM OS_ROLE WHERE TNT_INST_ID = 'ALIPW3CN'   AND    (NM like CONCAT('%', 'CMR-LEADS', '%') or CODE like CONCAT('%','CMR-LEADS','%'))                AND    (TYPE_CODE = 'ROLE' or TYPE_CODE is null )             AND    st !='DELETE'      AND    (apply_mode in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    or (type_code = 'ROLE' AND 'PUBLIC' in    (     'PUBLIC'    ,     'PUBLIC_COMMON'    )    AND apply_mode IS NULL))                         and                 isolation_key = 'TENANT_ALIPW3CN'
        """
        )
        assert isinstance(result, Statement)

    def test_sql_3(self):
        result = oceanbase_parser.parse(
            """
                         SELECT
  p.id,
  count(DISTINCT c.id)
FROM
  posts AS p
  LEFT JOIN comments AS c ON c.PostId = p.id
WHERE
  p.AnswerCount > 3
  AND p.title LIKE '%optimized%'
  AND DATE(p.CreationDate) >= '2017-01-01'
GROUP BY
  p.id
ORDER BY
  p.CreationDate
LIMIT
  100
                        """
        )
        assert isinstance(result, Statement)

    def test_sql_4(self):
        result = oceanbase_parser.parse(
            """
                  SELECT oprn.* , b   FROM OS_OPRN oprn    WHERE oprn.TNT_INST_ID = 'ALIPW3CN'                   AND      oprn.OPT_CODE like CONCAT('%', 'GT_MESSAGE_RECORD_QUERY', '%')                                                                and                     oprn.isolation_key = 'TENANT_ALIPW3CN'                     order by oprn.id desc    limit 0, 5      
                """
        )
        assert isinstance(result, Statement)

    def test_sql_5(self):
        result = oceanbase_parser.parse(
            """
                         select * from sqless_base where a = 'sqless_1' or b = 'sqless_2'     
                        """
        )
        assert isinstance(result, Statement)

    def test_sql_6(self):
        result = oceanbase_parser.parse(
            """
        SELECT           
         *     
         FROM `client_package`            
         left join `node_info` on ((`client_package`.`node_id` = `node_info`.`node_id`))
         WHERE     1=1                         
         and           client_package.type in                (           'test'      ,           'release'      )                                             
         and           client_package.version like concat('10.2.26.8000',"%")          
         and           client_package.state = 'success'                                                                                                                             
         order by client_package.id desc
         limit 0,10     
                        """
        )
        assert isinstance(result, Statement)

    def test_sql_7(self):
        result = oceanbase_parser.parse(
            """
SELECT          server_release_repo.server_release_repo_id,    server_release_repo.instance_id,    server_release_repo.repos_name,    server_release_repo.branch_url,    server_release_repo.revision_enter,    server_release_repo.deleted,    server_release_repo.weight,    server_release_repo.integrate,    server_release_repo.create_tag_flag,    server_release_repo.merge_record_id,    case server_release_repo.merge_record_id       when 0 then 0       when -1 then 1       when -2 then 15       else merge_record.merge_result       END as merge_result,   server_release_repo.completed,    server_release_repo.create_time,    server_release_repo.update_time      FROM server_release_repo left join merge_record on server_release_repo.merge_record_id = merge_record.id     WHERE      1 = 1                and            integrate = 0              and            completed = 1             and            deleted = 0          and       merge_record_id != -1                        """
        )
        assert isinstance(result, Statement)

    def test_union_and_union_all(self):
        result = mysql_parser.parse("select a from b union select a from b")
        assert isinstance(result.query_body, Union)
        assert not result.query_body.all
        result = oceanbase_parser.parse("select a from b union select a from b")
        assert isinstance(result.query_body, Union)
        assert not result.query_body.all

        result = oceanbase_parser.parse("select a from b union all select a from b")
        assert isinstance(result.query_body, Union)
        assert result.query_body.all
        result = mysql_parser.parse("select a from b union all select a from b")
        assert isinstance(result.query_body, Union)
        assert result.query_body.all

    def test_limit_question_mark(self):
        result = oceanbase_parser.parse(
            """
        SELECT * FROM `antinvoice93`.einv_base_info WHERE einv_source = ? ORDER BY gmt_create DESC LIMIT ?
        """
        )
        assert result.query_body.limit == '?'

        result = oceanbase_parser.parse(
            """
        SELECT * FROM `antinvoice93`.einv_base_info WHERE einv_source = ? ORDER BY gmt_create DESC LIMIT 1,?
        """
        )
        assert result.query_body.limit == '?'

    def test_subquery_limit(self):
        result = oceanbase_parser.parse(
            """
        SELECT COUNT(*) FROM ( SELECT * FROM customs_script_match_history LIMIT ? ) a
        """
        )
        assert isinstance(result, Statement)

    def test_current_timestamp(self):
        result = oceanbase_parser.parse(
            """
SELECT device_id, msg_id, short_msg_key, third_msg_id, mission_id , mission_coe, app_id, payload, template_code, business , ruleset_id, strategy, principal_id, tag, priority , expire_time, gmt_create, status, uriextinfo, sub_templates , immediate_product_version, biz_id, immediate_language_type FROM pushcore_msg WHERE device_id = ? AND principal_id = ? AND status = ? AND expire_time > current_timestamp()
        """
        )
        assert isinstance(result, Statement)

    def test_select_for_update(self):
        result = oceanbase_parser.parse(
            """
SELECT id, gmt_create, gmt_modified, match_id, match_record_id , user_id, complete_status, notice_push_status, result_push_status, reward_status , join_cost, reward, odps_reward, step_number, gmt_complete , gmt_send_reward, match_type, join_stat_bill_id, complete_stat_bill_id, ext_info FROM sports_user_match_record WHERE match_record_id IN (?) FOR UPDATE
        """
        )
        assert isinstance(result, Statement)
        assert result.query_body.for_update is True
        assert result.query_body.nowait_or_wait is False
        result = oceanbase_parser.parse(
            """
        SELECT id, gmt_create, gmt_modified, match_id, match_record_id , user_id, complete_status, notice_push_status, result_push_status, reward_status , join_cost, reward, odps_reward, step_number, gmt_complete , gmt_send_reward, match_type, join_stat_bill_id, complete_stat_bill_id, ext_info FROM sports_user_match_record WHERE match_record_id IN (?) FOR UPDATE NOWAIT
                """
        )
        assert isinstance(result, Statement)
        assert result.query_body.for_update is True
        assert result.query_body.nowait_or_wait is True
        result = oceanbase_parser.parse(
            """
                SELECT id, gmt_create, gmt_modified, match_id, match_record_id , user_id, complete_status, notice_push_status, result_push_status, reward_status , join_cost, reward, odps_reward, step_number, gmt_complete , gmt_send_reward, match_type, join_stat_bill_id, complete_stat_bill_id, ext_info FROM sports_user_match_record WHERE match_record_id IN (?) FOR UPDATE WAIT 6
                        """
        )
        assert isinstance(result, Statement)
        assert result.query_body.for_update is True
        assert result.query_body.nowait_or_wait is True

    def test_interval(self):
        sql = """
        SELECT biz_id, operator, MAX(gmt_create) AS gmt_create FROM log WHERE type = ? AND gmt_create > date_sub(now(), INTERVAL ? DAY) GROUP BY biz_id
        """
        result = oceanbase_parser.parse(Utils.remove_sql_text_affects_parser(sql))
        assert isinstance(result, Statement)

    def test_force_index(self):
        sql = """
        SELECT /*+read_consistency(weak) index(fund_trade_order_01 n_apply_order)*/ 
        t1.convert_out_product_id, t1.convert_out_apply_share 
        FROM fund_trade_order T1 INNER JOIN 
        ( 
        SELECT order_id, inst_apply_order_id FROM fund_trade_order FORCE INDEX (n_apply_order) 
        WHERE order_status IN (?) AND switch_flag = ? AND ta_code = ? AND scene_type <> ? 
        AND 
        (
            order_type IN (?) AND transaction_date = ? AND product_id IN (?) OR order_type = ? AND transaction_date = ? AND product_id IN (?)
        ) 
        ORDER BY inst_apply_order_id LIMIT ?, ? ) T2 WHERE t1.order_id = t2.order_id
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_interval2(self):
        sql = """
        SELECT h.site, h.ip, h.sm_name, h.pre_group, h.nodegroup , host_name, mount, used_pct, size, used , free, m.node FROM ( SELECT host_name, mount, MAX(used_pct) AS used_pct, MAX(size) AS size, MAX(used) AS used , MIN(free) AS free, MAX(check_time) AS check_time FROM host_disk_used h FORCE INDEX (idx_ct_up_m) WHERE check_time > now() - INTERVAL ? HOUR AND mount IN (?) AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? AND host_name NOT LIKE ? GROUP BY host_name, mount ORDER BY MAX(used) ) i, mt_armory_host h, ( SELECT ip, GROUP_CONCAT(node) AS node FROM mt_mysql_meta WHERE ip IS NOT NULL AND gmt_alive > now() - INTERVAL ? HOUR GROUP BY ip ) m WHERE i.host_name = h.hostname AND h.pre_group = ? AND m.ip = h.ip ORDER BY used"""
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_regexp(self):
        sql = """
        SELECT * FROM file_moving_serial WHERE serial_no REGEXP ?
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_distinct_2(self):
        sql = """
            /* trace_id=0b7cad2e168016361004041132631,rpc_id=0.5c88b07f.9.1 */                      SELECT /*+ index(midas_record_value idx_tenant_time) */                 DISTINCT(trace_id)             FROM                 midas_record_value where tenant='fascore' and is_expired=0  order by gmt_modified asc limit 500        
            """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_quoted(self):
        sql = "SELECT Original_artist FROM table_15383430_1 WHERE Theme = 'year'"
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)
        sql = '''SELECT Original_artist FROM table_15383430_1 WHERE Theme = "year"'''
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = mysql_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_chinese_character(self):
        sql = "SELECT `净值` FROM FundTable WHERE `销售状态` = \"正常申购\""
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)
        sql = "SELECT 净值 FROM FundTable WHERE 销售状态 = \"正常申购\""
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)
        sql = '''SELECT 赎回状态a FROM FundTable WHERE 重b仓 like \"北部湾港%\"'''
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_current_date(self):
        sql = """
        select 
            t1,t2
        from 
            foo 
        where 
            t1 > (CURRENT_DATE() - INTERVAL 30 day)+'0' 
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_double_type(self):
        sql = """
        SELECT Winner FROM table_11621915_1 WHERE Purse > 964017.2297960471 AND Date_ds = "may 28"
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_union_has_order_limit(self):
        sql = """
        ( select 球员id from 球员夺冠次数 order by 冠军次数 asc limit 3 ) union ( select 球员id from 球员夺冠次数 order by 亚军次数 desc limit 5 )
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = mysql_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_order_by_has_parentheses(self):
        sql = """
SELECT channel_code , contact_number FROM customer_contact_channels WHERE active_to_date - active_from_date = (SELECT active_to_date - active_from_date FROM customer_contact_channels ORDER BY (active_to_date - active_from_date) DESC LIMIT 1)        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_boolean_expression(self):
        sql = """
        SELECT T1.list_followers, T2.user_subscriber = 1 FROM lists AS T1 INNER JOIN lists_users AS T2 ON T1.user_id = T2.user_id AND T2.list_id = T2.list_id WHERE T2.user_id = 4208563 ORDER BY T1.list_followers DESC LIMIT 1
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_date_add(self):
        sql = """
        select date_format(date_format(date_add(biz_date, interval -1 day), '%y%m%d'), '%y%m%d') from t
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_as(self):
        sql = """
        select t2.biz_date as biz_date, f1.calculate_field / t2.calculate_field1 as d_remain_rate from ( select t1.biz_date as biz_date , count(DISTINCT if(t1.biz_date_is_visit = '1', t1.user_id, null)) as calculate_field1 from ( select * from pets_user_miaowa_galileo_visit_user_di ) t1 where t1.appname in ('AppPetWXSS', 'HelloPet') and t1.biz_date between date_format(date_sub(date_format(date_sub(curdate(), interval 1 day), '%Y%m%d'), interval 1 day), '%Y%m%d') and date_format(date_sub(curdate(), interval 1 day), '%Y%m%d') group by t1.biz_date ) t2 left join ( select date_format(date_format(date_add(t1.biz_date, interval -1 day), '%Y%m%d'), '%Y%m%d') as biz_date , count(DISTINCT if(datediff(t1.biz_date, t1.last_visit_date) = 1 and t1.biz_date_is_visit = '1', t1.user_id, null)) as calculate_field from ( select * from pets_user_miaowa_galileo_visit_user_di ) t1 where t1.appname in ('AppPetWXSS', 'HelloPet') and t1.biz_date between date_format(date_sub(date_format(date_sub(curdate(), interval 1 day), '%Y%m%d'), interval 1 day), '%Y%m%d') and date_format(date_sub(curdate(), interval 1 day), '%Y%m%d') group by date_format(date_format(date_add(t1.biz_date, interval -1 day), '%Y%m%d'), '%Y%m%d') ) f1 on t2.biz_date = f1.biz_date order by biz_date asc limit 0, 1000
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_single_quote_escape(self):
        sql = """
        SELECT director_id FROM movies WHERE movie_title = 'It''s Winter'
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

        sql = """
        SELECT director_id FROM movies WHERE movie_title = "It''s Winter"
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_operator(self):
        sql = """
        select         count(1)         from train_order_info where  occupy_type in              (                   ?              )                                  and order_serial_no =  ?                                 and connect_type =  ?                                     and merchant_id = ''                                 and order_state in              (                   ?              )                                 and ticket_machine_id in              (                   ?              )                                 and lock_state =  ?                                 and phone_verify_status =  ?                                 and window_no =  ?                                 and passenger_info like concat('%',concat( ?,'%'))                                 and departure_station like concat('%',concat( ?,'%'))                                 and arrival_station like concat('%',concat( ?,'%'))                                 and merchant_id =  ?                                 and createtime >=  ?                                 and createtime <=  ?                                 and gmt_booked >=  ?                                 and gmt_booked <=  ?                                 and gmt_departure >=  ?                                 and gmt_departure <=  ?                                 and train_code =  ?                                 and pay_serial_no =  ?                                 and env =  ?                                  and gmt_distribute >=  ?                                 and gmt_distribute <=  ?                                  and inquire_type in              (                   ?              )                                 and merchant_business_type =  ?                                 and ability_require &  ? =  ?
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)


if __name__ == '__main__':
    unittest.main()
