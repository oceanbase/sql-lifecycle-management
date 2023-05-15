# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import hashlib
import itertools
import os
import sys

from pymysql import escape_string

from common.const import *
from common.db_pool import ConnDBOperate
from common.db_query import DealMetaDBInfo
from common.logger import Logger

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)


class DealUserInfoOceanbase():
    """ read approved data from user database-oceanbase """

    def __init__(self, conn_info, get_retry):
        self.conn_info = conn_info
        self.db_conn = ConnDBOperate(self.conn_info, get_retry=get_retry)

    def get_unit_list(self):
        """ get schedule_task task to run """
        result = []
        try:
            table_name = 'gv$unit'
            if self.conn_info['version'] >= '4':
                table_name = 'GV$OB_UNITS'
            sql = """
                SELECT 
                SVR_IP,SVR_PORT,TENANT_ID 
                FROM {table_name} 
                WHERE STATUS = 'NORMAL'
                """.format(table_name=table_name)
            result = self.db_conn.func_select_storedb(sql)
        except Exception as e:
            log.exception(e)
        return result

    def get_plan_list(self, svr_ip, svr_port, last_time=''):
        plan_list = []
        try:
            time_sql = ''
            if last_time:
                time_sql = '''AND a.first_load_time >= \'{last_time}\''''.format(last_time=last_time)
            inner_tab_db = '__all_database'
            tenant_db = 'gv$tenant'
            plan_cache_table = 'gv$plan_cache_plan_stat'

            if self.conn_info['version'] >= '4':
                tenant_db = 'DBA_OB_TENANTS'
                plan_cache_table = 'GV$OB_PLAN_CACHE_PLAN_STAT'

            sql = '''SELECT /*+ use_hash(a,b,c),READ_CONSISTENCY(weak),QUERY_TIMEOUT(60000000) */
            a.tenant_id, b.tenant_name, a.sql_id, a.type plan_type, a.plan_hash,
            max(a.svr_ip) svr_ip,max(svr_port) svr_port,max(a.plan_id) plan_id,
            max(a.statement) statement,max(a.query_sql) sql_text,max(c.database_name) db_name,
            substr(min(a.first_load_time),1,19) first_load_time,substr(max(a.last_active_time),1,19) last_active_time,
            max(a.table_scan) table_scan,max(a.outline_id) outline_id, max(a.outline_data) outline_data,
            sum(a.hit_count) hit_count, sum(a.slow_count) slow_count, sum(a.executions) executions, max(avg_exe_usec) avg_exe_usec,
            round(avg(case when a.executions=0 then 0 else round(a.elapsed_time/a.executions) end)) elapsed_time,
            round(avg(case when a.executions=0 then 0 else round(a.cpu_time/a.executions) end)) cpu_time,
            round(avg(case when a.executions=0 then 0 else round(a.disk_reads/a.executions) end)) disk_reads,
            round(avg(case when a.executions=0 then 0 else round(a.direct_writes/a.executions) end)) direct_writes,
            round(avg(case when a.executions=0 then 0 else round(a.buffer_gets/a.executions) end)) buffer_gets,
            round(avg(case when a.executions=0 then 0 else round(a.rows_processed/a.executions) end)) rows_processed,
            round(avg(case when a.executions=0 then 0 else round(a.application_wait_time/a.executions) end)) application_wait_time,
            round(avg(case when a.executions=0 then 0 else round(a.concurrency_wait_time/a.executions) end)) concurrency_wait_time,
            round(avg(case when a.executions=0 then 0 else round(a.user_io_wait_time/a.executions) end)) user_io_wait_time
            FROM {plan_cache_table} a,{tenant_db} b,{inner_tab_db} c
            WHERE a.tenant_id=b.tenant_id AND a.db_id=c.database_id
            AND a.svr_ip='{svr_ip}' and a.svr_port='{svr_port}'
            AND b.tenant_name!='sys' AND b.tenant_name is not null
            AND c.database_name NOT IN ('oceanbase','information_schema','mysql','__recyclebin','test','DWEXP','OBMIGRATE','OMC','SYS','IDB_DDL','ODC_DDL','ODC_RND','__public')
            AND lower(a.statement) NOT LIKE 'select 1%'
            AND lower(a.statement) NOT LIKE 'commit%'
            AND lower(a.statement) NOT LIKE 'rollback%'
            AND lower(a.statement) NOT LIKE 'show %'
            AND lower(a.statement) NOT LIKE 'set %'
            AND lower(a.statement) NOT LIKE '%select @@global%'
            AND lower(a.statement) NOT LIKE '%select @@session%'
            AND lower(a.statement) NOT LIKE '%query_timeout%parallel%read_cluster%read_consistency%weak%'
            AND lower(a.statement) NOT LIKE '%oceanbase.%'
            AND (lower(a.statement) NOT LIKE 'insert%' or lower(a.statement) LIKE 'insert%select%')
            AND lower(a.statement) NOT LIKE 'replace%'
            {time_sql}
            GROUP BY a.tenant_id, b.tenant_name, a.sql_id, a.type, a.plan_hash;'''.format(
                plan_cache_table=plan_cache_table,
                tenant_db=tenant_db,
                inner_tab_db=inner_tab_db,
                time_sql=time_sql,
                svr_ip=svr_ip,
                svr_port=svr_port)
            result = self.db_conn.func_select_storedb(sql)
            if result:
                plan_list = list(result)
        except Exception as e:
            log.exception(e)
        return plan_list

    def get_plan_detail(self, svr_ip, svr_port, tenant_id, plan_id):
        """ 根据四元组获取执行计划详情 """
        detail_list = []
        try:
            table_name = 'gv$plan_cache_plan_explain'
            ip_column_name = 'ip'
            port_column_name = 'port'
            if self.conn_info['version'] >= '4':
                table_name = 'GV$OB_PLAN_CACHE_PLAN_EXPLAIN'
                ip_column_name = 'SVR_IP'
                port_column_name = 'SVR_PORT'

            check_sql = '''
            SELECT
            operator, name, rows, cost, property 
            FROM {table_name}
            WHERE 
            {ip_column_name}  = '{svr_ip}'
            AND {port_column_name}      = {svr_port}
            AND tenant_id = {tenant_id}
            AND plan_id   = {plan_id} ;'''.format(ip_column_name=ip_column_name, port_column_name=port_column_name,
                                                  table_name=table_name,
                                                  svr_port=svr_port, svr_ip=svr_ip, tenant_id=tenant_id,
                                                  plan_id=plan_id)
            result = self.db_conn.func_select_storedb(check_sql)
            if result:
                detail_list = list(result)
        except Exception as e:
            log.exception(e)
        return detail_list

    def disconn_storedb(self):
        try:
            self.db_conn.disconn_storedb()
        except Exception as e:
            pass


def schedule_plan_ob(db_conf, queue_info, schedule_type):
    """
        get sql plan from oceanbase
    """
    db_id = db_conf['db_id']
    # connect user oceanbase and metadb
    meta_conn = DealMetaDBInfo(DB_CONNECT_RETRY)
    user_conn = DealUserInfoOceanbase(db_conf, DB_CONNECT_RETRY)
    # get database deploy info
    unit_list = user_conn.get_unit_list()

    directly_run_sql_list = []
    update_plan_sql_param_list = []
    text_sql_param_list = []
    done_list = []
    max_deal_time = ''

    # circular process every deploy unit
    for per_unit in unit_list:
        unit_info = {
            'svr_ip': per_unit['SVR_IP'],
            'svr_port': per_unit['SVR_PORT'],
            'tenant_id': per_unit['TENANT_ID'],
            'db_id': db_conf['db_id'],
            'db_name': db_conf['app_dbname'],
            'cluster_name': db_conf['cluster_name'],
            'tenant_name': db_conf['tenant_name']
        }

        queue_key = '{0}:{1}:{2}'.format(db_id, unit_info['svr_ip'], unit_info['svr_port'])

        check_point = queue_info.get('check_point', '')

        plan_list = user_conn.get_plan_list(per_unit['SVR_IP'], per_unit['SVR_PORT'], check_point)

        if plan_list:
            data_list = sorted(plan_list, key=lambda item: (item['tenant_id'], item['tenant_name'], item['sql_id']))
            for (tenant_id, tenant_name, sql_id), group_data in itertools.groupby(data_list, key=lambda item: (
                    item['tenant_id'], item['tenant_name'], item['sql_id'])):
                # get sql plan baseline
                exist_list = meta_conn.get_exist_plans(db_id, sql_id)
                exist_dict = tenant_plan_groupby(exist_list)

                # compare sql plans one by one
                for per_plan in group_data:
                    try:
                        plan_type = int(per_plan['plan_type'])
                        plan_hash = int(per_plan['plan_hash'])
                        svr_ip = str(per_plan['svr_ip'])
                        svr_port = int(per_plan['svr_port'])
                        plan_id = int(per_plan['plan_id'])
                        statement = str(per_plan['statement'])
                        sql_text = str(per_plan['sql_text'])
                        first_load_time = str(per_plan['first_load_time'])
                        last_active_time = str(per_plan['last_active_time'])
                        table_scan = int(per_plan['table_scan'])
                        outline_id = int(per_plan['outline_id'])
                        outline_data = str(per_plan['outline_data'])
                        hit_count = int(per_plan['hit_count'])
                        avg_exe_usec = int(per_plan['avg_exe_usec'])

                        if not max_deal_time or max_deal_time < first_load_time:
                            max_deal_time = first_load_time

                        is_need_insert = True

                        # determine whether the online sql plan is already in the baseline
                        if exist_dict and sql_id in exist_dict and plan_hash in exist_dict[sql_id]:
                            # if it is already in the baseline,
                            # determine whether it is the same plan as the latest baseline
                            is_need_insert = False
                            _exist_plan_list = list(exist_dict[sql_id].values())
                            _exist_plan_list = sorted(_exist_plan_list, reverse=True,
                                                      key=lambda item: (item['first_load_time']))

                            if len(_exist_plan_list) > 1 and _exist_plan_list[0]['plan_hash'] != plan_hash:
                                last_plan_hash = _exist_plan_list[0]['plan_hash']
                                per_param = (outline_data, sql_text, db_id, sql_id, last_plan_hash)
                                update_plan_sql_param_list.append(per_param)

                            # If the comparison between the plan and the baseline is consistent,
                            # but the generation time of the version and plan has advanced,
                            # update the existing baseline data
                            if exist_dict[sql_id][plan_hash]['first_load_time'][:-3] != first_load_time[:-3]:
                                store_sql = ''' 
                                UPDATE monitor_sql_plan_oceanbase 
                                SET 
                                svr_ip=\'%s\', first_load_time=\'%s\', last_active_time=\'%s\', 
                                avg_exe_usec=%d, hit_count=%d, gmt_modify=now()
                                WHERE db_id=\'%s\' AND sql_id=\'%s\' AND plan_hash=%d;
                                ''' % (svr_ip, first_load_time, last_active_time,
                                       avg_exe_usec, hit_count, db_id, sql_id, plan_hash)
                                directly_run_sql_list.append(store_sql)
                            # If the plan is generated after binding, update the binding id
                            if exist_dict[sql_id][plan_hash]['outline_id'] != outline_id:
                                store_sql = '''
                                UPDATE monitor_sql_plan_oceanbase 
                                SET outline_id=%d, outline_time=now(), gmt_modify=now(), outline_data=%s, query_sql=%s
                                WHERE db_id=\'%s\' AND sql_id=\'%s\' AND plan_hash=%d;
                                ''' % (outline_id, outline_data, sql_text, db_id, sql_id, plan_hash)
                                directly_run_sql_list.append(store_sql)
                            # -1 means abnormal;
                            # 0 means there is no data and needs to be inserted;
                            # 1 means some fields need to be empty and needs to be updated;
                            # 2 means no processing, just skip
                            exist_code = meta_conn.get_exist_text(db_id, sql_id)
                            if exist_code == 1 and sql_id not in done_list:
                                done_list.append(sql_id)
                                text_sql_param_list.append((
                                    statement,
                                    db_id,
                                    sql_id
                                ))
                        # 获取执行计划详情
                        plan_detail = user_conn.get_plan_detail(svr_ip, svr_port, tenant_id, plan_id)
                        if not plan_detail:
                            continue
                        plan_info = ''
                        plan_full = ''
                        offset_cnt = 0
                        for per_line in plan_detail:
                            operator = per_line['operator']
                            opt_name = per_line['name']
                            opt_cost = int(per_line['cost'])
                            opt_prop = str(per_line['property'])[:400]
                            # deal operator type 2
                            if operator and plan_type == 2:
                                if 'PHY_ROOT_TRANSMIT' not in operator and (
                                        '_TRANSMIT' in operator or '_RECEIVE' in operator):
                                    offset_cnt = offset_cnt + 1
                                    continue
                            # deal operator type 3
                            if operator and plan_type == 3:
                                if 'PHY_ROOT_TRANSMIT' not in operator and (
                                        '_TRANSMIT' in operator or 'DIRECT_RECEIVE' in operator or 'FIFO_RECEIVE' in operator or 'ROOT_RECEIVE' in operator or 'DISTRIBUTED_RECEIVE' in operator):
                                    offset_cnt = offset_cnt + 1
                                    continue
                            if operator and offset_cnt > 0:
                                operator = operator[offset_cnt:]
                            if operator:
                                if opt_name:
                                    plan_info = plan_info + operator + ' , ' + opt_name.lower() + '|\n'
                                    plan_full = plan_full + operator + ' | ' + opt_name.lower() + ' | ' + str(
                                        opt_cost) + ' | ' + str(opt_prop) + '\n'
                                else:
                                    plan_info = plan_info + operator + ' , ' + 'null' + '|\n'
                                    plan_full = plan_full + operator + ' | ' + 'null' + ' | ' + str(
                                        opt_cost) + ' | ' + str(
                                        opt_prop) + '\n'
                        # end loop of plan_detail
                        # store_info
                        if plan_info and is_need_insert:
                            plan_info = plan_info[:-2].replace('\'', '')
                            plan_full = plan_full.replace('\'', '')[:60000]
                            outline_hash = hashlib.md5(outline_data.encode("utf8")).hexdigest()
                            # result_sql
                            store_sql = '''
                            INSERT IGNORE INTO monitor_sql_plan_oceanbase 
                            (
                                svr_ip,db_id,sql_id,tenant_name,plan_hash,plan_type,
                                first_load_time,last_active_time,avg_exe_usec,hit_count,
                                table_scan,plan_info,plan_detail,outline_hash,outline_id,outline_data,
                                gmt_create
                            )
                            VALUES(
                                '{svr_ip}','{db_id}','{sql_id}','{tenant_name}',{plan_hash},'{plan_type}',
                                '{first_load_time}', '{last_active_time}',{avg_exe_usec},{hit_count},
                                {table_scan},'{plan_info}','{plan_detail}','{outline_hash}','{outline_id}','{outline_data}',
                                now()
                            );'''.format(svr_ip=svr_ip, db_id=db_id, sql_id=sql_id, tenant_name=tenant_name,
                                         plan_hash=plan_hash, plan_type=plan_type,
                                         first_load_time=first_load_time,
                                         last_active_time=last_active_time, avg_exe_usec=avg_exe_usec,
                                         hit_count=hit_count, table_scan=table_scan, plan_info=escape_string(plan_info),
                                         plan_detail=escape_string(plan_full), outline_hash=outline_hash,
                                         outline_id=outline_id, outline_data=escape_string(outline_data))
                            directly_run_sql_list.append(store_sql)
                            if len(sql_text) <= 40000:
                                per_param = (outline_data, sql_text, db_id, sql_id, plan_hash)
                                update_plan_sql_param_list.append(per_param)
                            if sql_id not in done_list:
                                done_list.append(sql_id)
                                text_sql_param_list.append((
                                    statement,
                                    db_id,
                                    sql_id
                                ))
                    except Exception as e:
                        log.exception(e)

        if text_sql_param_list:
            update_sql = '''
            UPDATE monitor_sql_text 
            SET statement = %s
            where db_id = %s and sql_id= %s'''
            meta_conn.func_write_storedb(text_sql_param_list, update_sql)

        if directly_run_sql_list:
            meta_conn.func_write_storedb(directly_run_sql_list)

        if update_plan_sql_param_list:
            update_sql = '''
            UPDATE monitor_sql_plan_oceanbase
            SET outline_data=%s, query_sql=%s
            WHERE db_id=%s and sql_id=%s and plan_hash=%s'''
            meta_conn.func_write_storedb(update_plan_sql_param_list, update_sql)

        # update checkpoint
        queue_sql = '''REPLACE INTO schedule_queue(db_id,run_type,object_info,check_point,gmt_create)
                    VALUES(%s,%s,%s,%s,now());'''
        queue_list = [(db_conf['db_id'], schedule_type, queue_key, max_deal_time)]
        meta_conn.func_write_storedb(queue_list, queue_sql)

    # close connection
    meta_conn.disconn_storedb()
    user_conn.disconn_storedb()


def tenant_plan_groupby(list_dict):
    """ 按租户进行分组计算 """
    rt_dict = {}
    data_list = sorted(list_dict, key=lambda item: (item['sql_id']))
    for sql_id, group_data in itertools.groupby(data_list, key=lambda item: (item['sql_id'])):
        rt_dict[sql_id] = {}
        for per_plan in group_data:
            plan_hash = int(per_plan['plan_hash'])
            outline_id = int(per_plan['outline_id'])
            first_load_time = str(per_plan['first_load_time'])
            rt_dict[sql_id][plan_hash] = {}
            rt_dict[sql_id][plan_hash]['plan_hash'] = plan_hash
            rt_dict[sql_id][plan_hash]['outline_id'] = outline_id
            rt_dict[sql_id][plan_hash]['first_load_time'] = first_load_time
    return rt_dict
