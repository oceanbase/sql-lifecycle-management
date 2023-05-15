# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import datetime
import itertools
import math
import os
import sys
import time
import traceback

from common.db_pool import ConnDBOperate
from common.db_query import DealMetaDBInfo
from common.logger import Logger
from common.utils import Utils
from common.utils import fun_diff_secs, div_list
from optimizer.oceanbase_engine import OceanBaseEngine
from parser.parser_utils import ParserUtils

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)

GET_RETRY = 0
SILENT_SECONDS = 10
SILENT_IDS = 10000
SKIP_THRESHOLD = 300000
SKIP_RANGE = 100000
BATCH_RANGE = 10000

GROUP_KEY_LIST = ['tenant_name', 'svr_ip', 'sql_id', 'db_name', 'get_minute']
SUM_KEY_LIST = ['executions', 'fail_times', 'rpc_count', 'remote_plans', 'miss_plans', 'retry_cnt']
AVG_KEY_LIST = ['elapsed_time', 'cpu_time', 'queue_time', 'netwait_time', 'iowait_time', 'getplan_time',
                'return_rows', 'affected_rows', 'logical_reads', 'total_wait_time']
MAX_INT_LIST = ['table_scan', 'sql_type']
MAX_CHAR_LIST = ['request_time']
MIN_CHAR_LIST = []
LAST_KEY_LIST = ['query_sql', 'user_name', 'db_name', 'client_ip', 'plan_hash']
TOP_LIST = ['executions', 'fail_times', 'rpc_count', 'remote_plans', 'miss_plans', 'retry_cnt',
            'elapsed_time', 'cpu_time', 'queue_time', 'netwait_time', 'iowait_time', 'getplan_time',
            'return_rows', 'affected_rows', 'logical_reads', 'total_wait_time']

QUEUE_TABLE = 'schedule_queue'
TEXT_TABLE = 'monitor_sql_text'
AUDIT_TABLE = 'monitor_sql_auidt_oceanbase'
PLAN_TABLE = 'monitor_sql_plan_oceanbase'
INDEX_TABLE = 'meta_table_index'
STATIS_TABLE = 'meta_table_statistics'

config_dict = {
    "elapsed_time_limit": 300,
    "getplan_time_limit": 300,
    "return_rows_limit": 30,
    "affected_rows_limit": 20,
    "logical_reads_limit": 30,
    "batch_cnt_limit": 10,
    "top_n": 20,
    "batch_text": 100,
    "batch_insert": 30,
    'executions_floor': 3,
    'fail_times_floor': 1,
    'rpc_count_floor': 1,
    'remote_plans_floor': 1,
    'miss_plans_floor': 1,
    'retry_cnt_floor': 1,
    'elapsed_time_floor': 400,
    'getplan_time_floor': 400,
    'return_rows_floor': 10,
    'affected_rows_floor': 10,
    'logical_reads_floor': 20
}


class DealUserInfoOceanbase():
    """ read approved data from user database-oceanbase """

    def __init__(self, conn_info, get_retry):
        self.conn_info = conn_info
        self.db_conn = ConnDBOperate(self.conn_info, get_retry=get_retry)

    def get_unit_list(self):
        """ get schedule_task task to run """
        result = []
        try:
            sql = """
                SELECT /*+ READ_CONSISTENCY(weak),QUERY_TIMEOUT(10000000) */
                SVR_IP,SVR_PORT,TENANT_ID 
                FROM oceanbase.GV$OB_UNITS 
                WHERE STATUS = 'NORMAL'
                """
            result = self.db_conn.func_select_storedb(sql)
        except Exception as e:
            log.exception(e)
        return result

    def get_current_maxid(self, unit_info):
        """ get max request_id of database """
        max_id = 0
        max_time = 0
        try:
            sql = """
                SELECT /*+ READ_CONSISTENCY(weak),QUERY_TIMEOUT(10000000) */ 
                MAX(request_id) request_id
                FROM oceanbase.GV$OB_SQL_AUDIT 
                WHERE svr_ip = %s
                AND svr_port = %s
                AND tenant_id = %s
                AND db_name = %s
                """
            param = (unit_info['svr_ip'], unit_info['svr_port'],
                     unit_info['tenant_id'], unit_info['db_name'])
            result = self.db_conn.func_select_storedb(sql, param)
            if result and 'request_id' in result[0] and result[0]['request_id']:
                max_id = int(result[0]['request_id'])
                max_time = int(time.mktime(datetime.datetime.now().timetuple())) * 1000000
        except Exception as e:
            log.exception(e)
        return max_id, max_time

    def func_get_sqlaudit(self, unit_info, min_id, next_id):
        """ get sql audit """
        rt_list = []
        if self.conn_info['version'] >= '4':
            sql_audit_table = "GV$OB_SQL_AUDIT"
            logical_reads_equation = """round(avg(row_cache_hit*2+bloom_filter_cache_hit*2+
                block_cache_hit+disk_reads)) logical_reads,"""
        else:
            sql_audit_table = "gv$$sql_audit"
            logical_reads_equation = """round(avg(row_cache_hit*2+bloom_filter_cache_hit*2+
                block_cache_hit+block_index_cache_hit+disk_reads)) logical_reads,"""
        sql = '''SELECT /*+ READ_CONSISTENCY(weak),QUERY_TIMEOUT(10000000) */
        svr_ip,tenant_name,sql_id,db_name,get_minute,sql_type,request_time,executions,
        query_sql,client_ip,plan_hash,user_name,table_scan,fail_times,rpc_count,retry_cnt,
        remote_plans,miss_plans,elapsed_time,cpu_time,queue_time,netwait_time,iowait_time,
        getplan_time,total_wait_time,logical_reads,return_rows,affected_rows
        FROM
        (SELECT svr_ip,tenant_name,sql_id,db_name,concat(substr(usec_to_time((request_time)),1,16),':00') get_minute,
        max(case when lower(query_sql) like '%%select%%for%%update%%' and lower(query_sql) REGEXP 'for[^a-z]update' then 2
                 when lower(query_sql) like '%%insert%%into%%' then 3
                 when lower(query_sql) like '%%select%%' then 1
                 when lower(query_sql) like '%%update%%' then 4
                 when lower(query_sql) like '%%delete%%' then 5
                 when lower(query_sql) like '%%replace%%' then 6
                 else 0 end) sql_type,
        substr(usec_to_time(max(request_time)),1,19) request_time,
        count(*) executions,
        max(query_sql) query_sql,
        max(client_ip) client_ip,
        max(plan_hash) plan_hash,
        max(user_name) user_name,
        max(table_scan) table_scan,
        sum(case when ret_code=0 then 0 else 1 end) fail_times,
        sum(rpc_count) rpc_count,
        sum(retry_cnt) retry_cnt,
        sum(case when plan_type=2 then 1 else 0 end) remote_plans,
        sum(case when is_hit_plan=1 then 0 else 1 end) miss_plans,
        round(avg(elapsed_time)) elapsed_time,
        round(avg((execute_time-total_wait_time_micro+get_plan_time))) cpu_time,
        round(avg(queue_time)) queue_time,
        round(avg(net_wait_time)) netwait_time,
        round(avg(user_io_wait_time)) iowait_time,
        round(avg(get_plan_time)) getplan_time,
        round(avg(total_wait_time_micro)) total_wait_time,
        {logical_reads_equation}
        round(avg(return_rows)) return_rows,
        round(avg(affected_rows)) affected_rows
        FROM {audit_table}
        WHERE svr_ip=%s AND svr_port=%s AND tenant_id=%s AND db_name = %s
        AND request_id>%s AND request_id<=%s
        AND lower(QUERY_SQL) not like 'select 1%%'
        AND lower(QUERY_SQL) not like 'rollback%%'
        AND lower(QUERY_SQL) not like 'set%%'
        AND lower(QUERY_SQL) not like 'show%%'
        AND lower(QUERY_SQL) not like '%%show variables%%'
        AND lower(QUERY_SQL) not like '%%select%%@@session.%%'
        GROUP BY svr_ip,tenant_name,sql_id,db_name,concat(substr(usec_to_time((request_time)),1,16),':00')
        ) sub
        WHERE executions  >= 3
        OR fail_times    >= 1
        OR rpc_count     >= 1
        OR remote_plans  >= 1
        OR miss_plans    >= 1
        OR retry_cnt     >= 1
        OR elapsed_time  >= %s
        OR getplan_time  >= %s
        OR return_rows   >= %s
        OR affected_rows >= %s
        OR logical_reads >= %s
        ORDER BY tenant_name,get_minute,sql_id,request_time;
        '''.format(audit_table=sql_audit_table,
                   logical_reads_equation=logical_reads_equation)
        param = (unit_info['svr_ip'], unit_info['svr_port'],
                 unit_info['tenant_id'], unit_info['db_name'],
                 min_id, next_id,
                 str(config_dict.get("elapsed_time_limit", 300)),
                 str(config_dict.get("getplan_time_limit", 300)),
                 str(config_dict.get("return_rows_limit", 30)),
                 str(config_dict.get("affected_rows_limit", 20)),
                 str(config_dict.get("logical_reads_limit", 30)))
        try:
            result = self.db_conn.func_select_storedb(sql, param)
            if result:
                rt_list = list(result)
        except Exception as e:
            log.exception(e)
        return rt_list

    def disconn_storedb(self):
        try:
            self.db_conn.disconn_storedb()
        except Exception as e:
            pass


def deal_queue_checkpoint(min_id, min_time, max_id, max_time):
    """ deal start and end request_id """
    rt_dict = {'min_id': 0, 'max_id': 0, 'min_time': 0, 'max_time': 0}
    # If the queue point is empty, set start_id = current_id - SKIP_RANGE
    if not min_id and max_id:
        min_id = max_id - SKIP_RANGE
        min_time = (int(time.mktime(datetime.datetime.now().timetuple())) - 600) * 1000000
    # judge the id gap and time gap
    if min_time and max_time and max_id and min_id:
        min_time_dt = datetime.datetime.fromtimestamp(int(min_time / 1000000))
        max_time_dt = datetime.datetime.fromtimestamp(int(max_time / 1000000))
        diff_secs = fun_diff_secs(min_time_dt, max_time_dt)
        diff_ids = max_id - min_id
        # interval is too small, skip capture to reduce online DB request frequency and stored datasize
        if diff_secs < SILENT_SECONDS and diff_ids < SILENT_IDS:
            return rt_dict
        # if id gap is too large, min_id skip to current_id - SKIP_RANGE to prevent delays continues to increase
        if diff_ids > SKIP_THRESHOLD:
            min_id = max_id - SKIP_RANGE
        # In some bug cases as restart or upgrade observer, request_id may be reset to zero
        if min_id > max_id:
            min_id = max_id - SKIP_RANGE
    rt_dict['min_id'] = min_id
    rt_dict['max_id'] = max_id
    rt_dict['min_time'] = min_time
    rt_dict['max_time'] = max_time
    return rt_dict


def func_group_by_key(data_list):
    """ aggregate sql_list by observer/tenant_name/db_name/get_minute/sql_id, to make a downsampling """
    rt_list = []
    if data_list:
        # order by groupkey and divide into groups
        data_list = sorted(data_list, key=lambda item: (item['db_name'], item['get_minute'], item['sql_id']))
        for (db_name, get_minute, sql_id), group_data in itertools.groupby(data_list,
                                                                           key=lambda item: (
                                                                                   item['db_name'],
                                                                                   item['get_minute'],
                                                                                   item['sql_id'])):
            try:
                # invalid sql check
                if not sql_id.isalnum():
                    continue
                per_dict = {}
                per_dict['db_name'] = db_name
                per_dict['get_minute'] = get_minute
                per_dict['sql_id'] = sql_id
                # init item value
                for sum_key in SUM_KEY_LIST:
                    locals()['sum_' + str(sum_key)] = 0
                for avg_key in AVG_KEY_LIST:
                    locals()['sum_' + str(avg_key)] = 0
                for max_int in MAX_INT_LIST:
                    locals()['max_' + str(max_int)] = 0
                for max_char in MAX_CHAR_LIST:
                    locals()['max_' + str(max_char)] = ''
                for min_char in MIN_CHAR_LIST:
                    locals()['min_' + str(min_char)] = '9999-12-31 23:59:59'
                # group aggregation
                init_mark = 0
                for per_key in group_data:
                    for sum_key in SUM_KEY_LIST:
                        locals()['sum_' + str(sum_key)] += int(per_key[sum_key])
                    for avg_key in AVG_KEY_LIST:
                        locals()['sum_' + str(avg_key)] += int(per_key[avg_key]) * int(per_key['executions'])
                    for max_int in MAX_INT_LIST:
                        if per_key[max_int] > locals()['max_' + str(max_int)]:
                            locals()['max_' + str(max_int)] = per_key[max_int]
                    for max_char in MAX_CHAR_LIST:
                        if per_key[max_char] > locals()['max_' + str(max_char)]:
                            locals()['max_' + str(max_char)] = per_key[max_char]
                    for min_char in MIN_CHAR_LIST:
                        if per_key[min_char] < locals()['min_' + str(min_char)]:
                            locals()['min_' + str(min_char)] = per_key[min_char]
                    if init_mark == 0:
                        init_mark = 1
                        for last_key in LAST_KEY_LIST:
                            per_dict[last_key] = per_key[last_key]
                # calcute item fianal value
                if locals()['sum_' + str('executions')] == 0:
                    locals()['sum_' + str('executions')] = 1
                for sum_key in SUM_KEY_LIST:
                    per_dict[sum_key] = locals()['sum_' + str(sum_key)]
                for avg_key in AVG_KEY_LIST:
                    per_dict[avg_key] = round(locals()['sum_' + str(avg_key)] / locals()['sum_' + str('executions')])
                for max_int in MAX_INT_LIST:
                    per_dict[max_int] = locals()['max_' + str(max_int)]
                for max_char in MAX_CHAR_LIST:
                    per_dict[max_char] = locals()['max_' + str(max_char)]
                for min_char in MIN_CHAR_LIST:
                    per_dict[min_char] = locals()['min_' + str(min_char)]
                rt_list.append(per_dict)
            except Exception as e:
                log.error('func_group_by_key error: {err1}, {err2}'.format(err1=type(e), err2=str(e)))
                traceback.print_exc()
                continue
    return rt_list


def get_sqlaudit_rst(user_conn, unit_info, min_id, max_id):
    """ batch get SQL audit """
    audit_list = []
    batch_cnt = 0
    while (min_id < max_id):
        if min_id + BATCH_RANGE < max_id:
            next_id = min_id + BATCH_RANGE
        else:
            next_id = max_id
        sqllist = user_conn.func_get_sqlaudit(unit_info, min_id, next_id)
        if sqllist:
            batch_cnt = batch_cnt + 1
            audit_list.extend(sqllist)
            # aggregation batch to summary
            if batch_cnt % int(config_dict.get("batch_cnt_limit", 10)) == 0:
                audit_list = func_group_by_key(audit_list)
        min_id = min_id + BATCH_RANGE
    # if not exact division, deal the lastest batch
    if batch_cnt % int(config_dict.get("batch_cnt_limit", 10)) != 0:
        audit_list = func_group_by_key(audit_list)
    return audit_list


def func_get_topn(input_list):
    """ get top-n sqls for every key dimensionality """
    rt_id_list = []
    rt_dt_list = []
    # if total amount less than top-n*5，then return list directly
    if len(input_list) <= config_dict.get("top_n", 20) * 5:
        return input_list
    # get top-n by topkey
    for top_key in TOP_LIST:
        # elapsed_time is the most important
        if top_key == 'elapsed_time':
            get_top_n = config_dict.get("top_n", 20) * 3
        else:
            get_top_n = config_dict.get("top_n", 20)
        try:
            # sort list by topkey upside down
            st_list = sorted(input_list, reverse=True, key=lambda item: item[top_key])
            for rank in range(0, get_top_n):
                if rank >= len(st_list):
                    break
                sql_id = str(st_list[rank]['sql_id'])
                top_value = int(st_list[rank][top_key])
                # If item value is too small, sql can be ignored
                if str(top_key) + '_floor' in config_dict and top_value < config_dict[str(top_key) + '_floor']:
                    break
                # pop by sql_id to prevent the repeat
                if sql_id not in rt_id_list:
                    rt_id_list.append(sql_id)
                    rt_dt_list.append(st_list[rank])
        except Exception as e:
            log.error('func_get_topn error: {top_key} {err}'.format(top_key=top_key, err=str(e)))
            continue
    return rt_dt_list


def get_topn_rst(meta_conn, unit_info, audit_list):
    """ get top-n sqls sorted by all kinds of performance item, aggregate result and compare SQL Text """
    rt_list = []
    txt_list = []
    value_list = []
    sub_txt_list = []
    sql_id_list = []
    detail_pre = '''INSERT INTO {audit_table}(db_id,sql_id,request_time,sql_type,executions,
                elapsed_time,cpu_time,queue_time,getplan_time,netwait_time,iowait_time,total_wait_time,
                logical_reads,return_rows,affected_rows,fail_times,retry_cnt,rpc_count,remote_plans,miss_plans,
                cluster,tenant_name,svr_ip,client_ip,user_name,plan_hash,table_scan,gmt_create)
                VALUES '''.format(audit_table=AUDIT_TABLE)
    if not audit_list:
        return rt_list, txt_list
    # group by database name and minute time zone, get top-n list for every item
    st_list = sorted(audit_list, key=lambda item: (item['db_name'], item['get_minute']))
    for (db_name, get_minute), group_data in itertools.groupby(st_list,
                                                               key=lambda item: (
                                                                       item['db_name'],
                                                                       item['get_minute'])):
        try:
            # get top-n by key
            group_list = list(group_data)
            dt_list = func_get_topn(group_list)
            # sql_baseline
            for per_dt in dt_list:
                detail_value = '''(\'%s\',\'%s\',\'%s\',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,
                \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%d,%d,now())''' % (
                    unit_info['db_id'], per_dt['sql_id'], per_dt['request_time'], per_dt['sql_type'],
                    per_dt['executions'], per_dt['elapsed_time'], per_dt['cpu_time'], per_dt['queue_time'],
                    per_dt['getplan_time'], per_dt['netwait_time'], per_dt['iowait_time'],
                    per_dt['total_wait_time'], per_dt['logical_reads'], per_dt['return_rows'],
                    per_dt['affected_rows'], per_dt['fail_times'], per_dt['retry_cnt'],
                    per_dt['rpc_count'], per_dt['remote_plans'], per_dt['miss_plans'],
                    unit_info['cluster_name'], unit_info['tenant_name'], unit_info['svr_ip'],
                    per_dt['client_ip'], per_dt['user_name'], per_dt['plan_hash'], per_dt['table_scan']
                )
                value_list.append(detail_value)
                # compare sql text, aggregate sql_id to batch deal
                sql_id = per_dt['sql_id']
                if sql_id not in sql_id_list:
                    if per_dt['query_sql'] and 'datediff(' not in per_dt['query_sql']:
                        # For kv scenario
                        if sql_id.startswith('TABLEAPI'):
                            query_sql = per_dt['query_sql'][:500]
                        # For insert scenario
                        elif 'insert' in per_dt['query_sql']:
                            query_sql = per_dt['query_sql'][:1000]
                        # sql text is too long, then cut off
                        elif len(per_dt['query_sql']) > 20000:
                            query_sql = per_dt['query_sql'][:19800] + per_dt['query_sql'][-200:]
                        else:
                            query_sql = per_dt['query_sql']
                        table_list = []
                        try:
                            if per_dt['sql_type'] == 1 or per_dt['sql_type'] == 2 or per_dt['sql_type'] == 4 \
                                    or per_dt['sql_type'] == 5:
                                sql = Utils.remove_sql_text_affects_parser(query_sql)
                                visitor = ParserUtils.format_statement(OceanBaseEngine().parse(sql))
                                for _table in visitor.table_list:
                                    table_list.append(_table['table_name'])
                        except Exception as e:
                            log.exception(e)
                        txt_param = (
                            unit_info['db_id'], sql_id, per_dt['sql_type'], query_sql, per_dt['user_name'],
                            ','.join(table_list))
                        sub_txt_list.append(txt_param)
                        sql_id_list.append(sql_id)
        except Exception as e:
            log.exception(e)
            continue
    # batch compare sql text is exists
    if sql_id_list:
        comp_list = div_list(sql_id_list, math.ceil(len(sql_id_list) / config_dict.get("batch_text", 100)))
        for per_comp in comp_list:
            sql_id_str = '\'' + '\', \''.join(per_comp) + '\''
            txt_exist = meta_conn.func_check_text(unit_info['db_id'], sql_id_str)
            for sql_id in per_comp:
                if sql_id not in txt_exist:
                    txt_list.append(sub_txt_list[sql_id_list.index(sql_id)])
    # aggregate audit list to batch insert
    if value_list:
        new_list = div_list(value_list, math.ceil(len(value_list) / config_dict.get("batch_insert", 50)))
        for per_batch in new_list:
            batch_value = ', '.join(per_batch)
            detail_sql = detail_pre + batch_value
            rt_list.append(detail_sql)
    return rt_list, txt_list


def schedule_topsql_ob(db_conf, queue_info, schedule_type):
    """ get sql audit from user oceanbase
        design principles:
            1.In order to reduce the performance loss of online database, scheduler is single thread
                for single observer, sql audit is sampling, rather than the full workload
            2.In order to reduce the performance loss of online database, each interval must be greater than
                a period of time or range threshold, avoid frequent reading library
            3.In order to guarantee the real time data, when consumption scheduling found checkpoint
                delay is too large, according to the current largest request_id to jump
    """
    db_id = db_conf['db_id']
    # connect user oceanbase and metadb
    meta_conn = DealMetaDBInfo(GET_RETRY)
    user_conn = DealUserInfoOceanbase(db_conf, GET_RETRY)
    # get database deploy info
    unit_list = user_conn.get_unit_list()
    # circular process every deploy unit
    for per_unit in unit_list:
        unit_info = {}
        unit_info['svr_ip'] = per_unit['SVR_IP']
        unit_info['svr_port'] = per_unit['SVR_PORT']
        unit_info['tenant_id'] = per_unit['TENANT_ID']
        unit_info['db_id'] = db_conf['db_id']
        unit_info['db_name'] = db_conf['app_dbname']
        unit_info['cluster_name'] = db_conf['cluster_name']
        unit_info['tenant_name'] = db_conf['tenant_name']

        # get current checkpoint
        max_id, max_time = user_conn.get_current_maxid(unit_info)
        # get queue checkpoint
        queue_key = '{0}:{1}:{2}'.format(db_id, unit_info['svr_ip'], unit_info['svr_port'])
        min_id = queue_info.get(queue_key, {}).get('request_id', 0)
        min_time = queue_info.get(queue_key, {}).get('request_time', 0)
        # deal start and end request_id
        point_dict = deal_queue_checkpoint(min_id, min_time, max_id, max_time)
        # skip capture batch
        if not point_dict['max_id']:
            continue

        # get sqlaudit
        audit_list = get_sqlaudit_rst(user_conn, unit_info, point_dict['min_id'], point_dict['max_id'])
        if not audit_list:
            continue
        # filtrate sqlaudit
        dt_list, txt_list = get_topn_rst(meta_conn, unit_info, audit_list)

        # write result to metadb
        # write sql text
        if txt_list:
            txt_sql = '''INSERT INTO {text_table}(db_id,sql_id,sql_type,sql_text,user_name,table_list,gmt_create)
                VALUES(%s,%s,%s,%s,%s,%s,now());'''.format(text_table=TEXT_TABLE)
            meta_conn.func_write_storedb(txt_list, txt_sql)
        # write sql audit
        if dt_list:
            meta_conn.func_write_storedb(dt_list)
        # update checkpoint
        queue_sql = '''REPLACE INTO {queue_table}(db_id,run_type,object_info,check_point,gmt_create)
                VALUES(%s,%s,%s,%s,now());'''.format(queue_table=QUEUE_TABLE)
        check_point = """{{'request_id':{}, 'request_time':{}}}""".format(point_dict['max_id'],
                                                                          point_dict['max_time'])
        queue_list = [(db_conf['db_id'], schedule_type, queue_key, check_point)]
        meta_conn.func_write_storedb(queue_list, queue_sql)

    # close connection
    meta_conn.disconn_storedb()
    user_conn.disconn_storedb()
