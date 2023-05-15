# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import calendar
import hashlib
import itertools
import json
import os
import sys
import time

from common.db_decrypt_pswd import decrypt_password
from common.db_pool import ConnDBOperate
from common.db_pool import DBPool
from common.logger import Logger
from common.utils import Utils

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)

# local db config
cfg_file = os.getcwd().split('sqless')[0] + 'sqless/db.cfg'

f = open(cfg_file, 'r', encoding='utf-8')
cfg = f.read()
cfg = eval(cfg)
metadb = cfg['db']
env = cfg['env']

# encryption and decryption
need_decrypt = cfg.get('need_decrypt', 0)
if need_decrypt:
    metadb['pswd'] = decrypt_password(metadb['pswd'])

f.close()

hl = hashlib.md5()

SQLESS_DEFULT_PASSWORD = '*e9c2bcdc178a99b7b08dd25db58ded2ee5bff05'


def get_user_database(user_id, current_page=None, page_size=None):
    sql = """
    SELECT 
    a.database_alias databaseAlias,a.database_name databaseName,a.engine,a.version,a.platform,
    b.approve_type approveType,b.approve_scope approveScope,b.host_ip hostIp,b.host_port hostPort,
    b.user_name userName
    FROM
    database_asset a left join monitor_database b on a.db_id = b.db_id
    WHERE
    a.user_id = %s
    """

    # pagination
    if current_page and page_size:
        sql = sql + """ limit %d,%d """
        param = (user_id, (current_page - 1) * page_size, page_size)
    else:
        param = user_id

    db_info = ConnDBOperate(metadb)

    try:
        get_rst = db_info.func_select_storedb(sql, param)
        if get_rst:
            return list(get_rst)
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return []


def insert_user_database(user_id, db_alias, type, version, platform, database_name):
    db_info = ConnDBOperate(metadb)

    try:
        sql = """
                SELECT COUNT(*) as c FROM database_asset WHERE db_id = %s
                """
        param = Utils.get_db_id(db_alias, user_id)
        res = db_info.func_select_storedb(sql, param)

        if res and res[0]['c'] > 0:
            return False, "databaseAlias : {db_alias} is repetitive".format(db_alias=db_alias)

        sql = """
                INSERT IGNORE INTO database_asset(db_id,user_id,database_alias,database_name,engine,version,platform)
                VALUES('{db_id}','{user_id}','{database_alias}','{database_name}','{engine}','{version}','{platform}')
                """.format(db_id=Utils.get_db_id(db_alias, user_id), user_id=user_id, database_alias=db_alias,
                           database_name=database_name, engine=type,
                           version=version,
                           platform=platform)

        db_info.func_write_storedb([sql])
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
        return False, str(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return True, None


def update_user_database(db_alias, engine, version, platform, user_id):
    db_info = ConnDBOperate(metadb)
    try:
        sql = """
                UPDATE database_asset 
                SET 
                version = '{version}', platform = '{platform}',  engine = '{engine}'
                WHERE 
                db_id = '{db_id}'
                """.format(db_id=Utils.get_db_id(db_alias, user_id),
                           version=version,
                           platform=platform,
                           engine=engine)

        db_info.func_write_storedb([sql])
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
        return False, str(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return True, None


def get_user_optimization(user_id, engine, start_time, end_time):
    sql = """
    SELECT 
    database_alias AS dbAlias,tag,sql_text_list AS sqlText,type,status,
    optimization_detail as report,UNIX_TIMESTAMP(deal_time) as dealTime,is_read as isRead
    FROM user_optimization
    WHERE 
    user_id = %s
    AND engine = %s
    AND gmt_create >= FROM_UNIXTIME({start_time})
    AND gmt_create <= FROM_UNIXTIME({end_time})
    order by dealTime desc
    """.format(start_time=start_time, end_time=end_time)

    param = (user_id, engine)

    db_info = ConnDBOperate(metadb)

    try:
        res = db_info.func_select_storedb(sql, param)
        sql = """
            SELECT 
            count(1) c
            FROM user_optimization
            WHERE 
            user_id = %s
            AND engine = %s
            """
        count_res = db_info.func_select_storedb(sql, param)
        return list(res), count_res[0]['c'] if count_res else 0
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return [], 0


def insert_user_optimization(user_id, database_alias, sql_text_list, optimization_detail, type):
    sql = """
        SELECT 
        database_alias,database_name,engine,version,platform
        FROM
        database_asset
        WHERE
        db_id = %s
        """

    param = Utils.get_db_id(database_alias, user_id)

    db_info = ConnDBOperate(metadb)
    try:
        get_rst = db_info.func_select_storedb(sql, param)
        if get_rst:
            engine = get_rst[0]['engine']
            hl.update(str(sql_text_list + str(user_id) + str(calendar.timegm(time.gmtime()))).encode(encoding='utf-8'))
            tag = hl.hexdigest()
            sql = """
                INSERT IGNORE INTO user_optimization(tag,user_id,engine,type,status,database_alias,is_read,sql_text_list,optimization_detail)
                VALUES(%s, %s, %s, %s,'done', %s, 1, %s, %s)
                """
            param = (
                tag,
                user_id,
                engine,
                type,
                database_alias,
                sql_text_list,
                str(json.dumps(optimization_detail))
            )
            db_info.func_write_storedb([param], sql)

    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return None


def get_db_info(user_id, database_alias):
    sql = """
            SELECT 
            database_alias,database_name,engine,version,platform
            FROM
            database_asset
            WHERE
            db_id = %s
          """

    param = Utils.get_db_id(database_alias, user_id)

    db_info = ConnDBOperate(metadb)

    try:
        get_rst = db_info.func_select_storedb(sql, param)
        if get_rst:
            return get_rst[0]
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return None


def read_user_optimization(tag, user_id):
    db_info = None
    try:
        sql = """
                UPDATE user_optimization SET is_read = 1 WHERE tag = '{tag}' AND user_id = '{user_id}'
                """.format(tag=tag, user_id=user_id)
        db_info = ConnDBOperate(metadb)
        db_info.func_write_storedb([sql])
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()

    return None


def table_index_groupby(list_dict):
    rt_dict = {}
    data_list = sorted(list_dict, key=lambda item: (item['table_name']))
    for (table_name), group_data in itertools.groupby(data_list, key=lambda item: (item['table_name'])):
        rt_dict[table_name] = {}
        for per_idx in group_data:
            index_name = str(per_idx['index_name'])
            index_status = str(per_idx['index_status'])
            index_type = str(per_idx['index_type'])
            column_list = str(per_idx['column_list'])
            # The primary key is all uppercase by default, and the others are uniformly lowercase
            if index_name.lower() == 'primary':
                index_name = 'PRIMARY'
            else:
                index_name = index_name.lower()
            rt_dict[table_name][index_name] = {}
            rt_dict[table_name][index_name]['index_status'] = index_status
            rt_dict[table_name][index_name]['index_type'] = index_type
            rt_dict[table_name][index_name]['column_list'] = column_list
    return rt_dict


def table_stats_groupby(list_dict):
    """
    Group and summarize statistics by table
    :param list_dict:
    :return:
    """
    rt_dict = {}
    data_list = sorted(list_dict, key=lambda item: (item['table_name']))
    for (table_name), group_data in itertools.groupby(data_list, key=lambda item: (
            item['table_name'])):
        rt_dict[table_name] = {}
        for per_col in group_data:
            column_name = str(per_col['column_name'])
            rt_dict[table_name][column_name] = {}
            rt_dict[table_name][column_name]['ndv_count'] = int(per_col['ndv_count'])
            rt_dict[table_name][column_name]['table_rows'] = int(per_col['table_rows'])
    return rt_dict


def monitor_database_connection_check(host_ip, host_port, user_name, password, database_name):
    """
    connect the database
    :param host_ip:
    :param host_port:
    :param user_name:
    :param password:
    :param database_name:
    :return:
    """
    # TODO: Need to check the database type when connecting,
    #  and the connection methods of different databases will be different,
    #  now only consider the oceanbase type

    message = ''
    grant_action = ''
    success = True

    db_conf = {
        'host': host_ip,
        'port': host_port,
        'dbname': database_name,
        'user': user_name,
        'pswd': password,
        'charset': 'utf8'
    }

    try:
        conn = DBPool(db_conf)
        if not conn:
            message = 'Database connection error, please check the database configuration'
            grant_action = '''
            CREATE USER sqless IDENTIFIED BY password '{password}' ;
            GRANT SELECT ON *.* to sqless;
            '''.format(password=SQLESS_DEFULT_PASSWORD)
            success = False
        else:
            result = conn.get_all("show grants for {user_name}".format(user_name=user_name))
            if result and result[0] != "GRANT SELECT ON *.* TO '{user_name}'".format(user_name=user_name):
                message = 'The current user query permission is limited, ' \
                          'please click the copy button to query the authorization statement'
                grant_action = '''
                GRANT SELECT ON *.* to {user_name};
                '''.format(user_name=user_name)
                success = False

    except Exception as e:
        log.exception(e)
        message = 'Database connection error, please check the database configuration'
        grant_action = '''
                    CREATE USER sqless IDENTIFIED BY password '{password}' ;
                    GRANT SELECT ON *.* to {user_name};
                    '''.format(password=SQLESS_DEFULT_PASSWORD, user_name=user_name)
        success = False

    return message, grant_action, success


def insert_monitor_database(db_alias, user_id, approve_type, approve_scope, host_ip, host_port, user_name, password):
    db_info = ConnDBOperate(metadb)
    try:
        sql = """
                REPLACE INTO 
                monitor_database
                (db_id, approve_type, approve_scope, host_ip, host_port, user_name, password, gmt_create)
                values('{db_id}', '{approve_type}', '{approve_scope}', '{host_ip}', '{host_port}', '{user_name}',
                '{password}', now())
                """.format(db_id=Utils.get_db_id(db_alias, user_id),
                           approve_type=approve_type,
                           approve_scope=approve_scope,
                           host_ip=host_ip,
                           host_port=host_port,
                           user_name=user_name,
                           password=password)

        db_info.func_write_storedb([sql])
    except Exception as e:
        if db_info:
            db_info.disconn_storedb()
        log.exception(e)
    finally:
        if db_info:
            db_info.disconn_storedb()


class DealMetaDBInfo():
    """ read and write meta database """

    def __init__(self, get_retry=1):
        self.meta_conn = ConnDBOperate(metadb, get_retry=get_retry)

    def get_schedule_task(self, schedule_type):
        """ get schedule_task task to run """
        rt_list = []
        try:
            sql = """
                    SELECT ass.db_id,ass.database_name dbname,ass.engine,ass.version,ass.platform,
                    mon.approve_scope approve_scope,mon.host_ip host,mon.host_port port,mon.user_name user,mon.password pswd
                    FROM database_asset ass, monitor_database mon
                    WHERE ass.db_id = mon.db_id
                    AND mon.approve_type = '{schedule_type}'
                    ORDER BY ass.engine,ass.version
                    """.format(schedule_type=schedule_type)
            rt_list = self.meta_conn.func_select_storedb(sql)
            # decrypt password
            for per_line in rt_list:
                per_line['pswd'] = decrypt_password(per_line['pswd'])
                per_line['charset'] = 'utf8'
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_schedule_queue(self, db_id, schedule_type):
        """ get schedule_task queue point """
        rt_dict = {}
        try:
            sql = """
                    SELECT db_id,run_type,object_info,check_point
                    FROM schedule_queue
                    WHERE run_type = '{schedule_type}'
                    AND db_id = '{db_id}'
                    """.format(schedule_type=schedule_type, db_id=db_id)
            result = self.meta_conn.func_select_storedb(sql)
            for per_line in result:
                if per_line['check_point']:
                    if per_line['run_type'] == 'sql':
                        rt_dict[per_line['object_info']] = eval(per_line['check_point'])
                    else:
                        rt_dict[per_line['object_info']] = per_line['check_point']
        except Exception as e:
            log.exception(e)
        return rt_dict

    def func_check_text(self, db_id, sql_id_str):
        """ check sql text exists, table_list as mark """
        rt_dict = {}
        try:
            sql = '''SELECT sql_id,table_list FROM monitor_sql_text
            WHERE db_id = '{db_id}' AND sql_id in ({sql_id_str});
            '''.format(db_id=db_id, sql_id_str=sql_id_str)
            result = self.meta_conn.func_select_storedb(sql)
            for per_sql in result:
                rt_dict[per_sql['sql_id']] = per_sql['table_list']
        except Exception as e:
            log.exception(e)
        return rt_dict

    def get_exist_plans(self, db_id, sql_id):
        """ Get the execution plan baseline and compare it with the existing one """
        exist_list = []
        try:
            sql = '''
            SELECT
            sql_id,plan_hash,outline_id,substr(first_load_time,1,19) first_load_time
            FROM monitor_sql_plan_oceanbase
            WHERE 
            db_id = '{db_id}' 
            AND sql_id = '{sql_id}'
            ORDER BY sql_id,plan_hash;'''.format(db_id=db_id, sql_id=sql_id)
            result = self.meta_conn.func_select_storedb(sql)
            if result:
                exist_list = list(result)
        except Exception as e:
            log.exception(e)
        return exist_list

    def get_exist_text(self, db_id, sql_id):
        """
        -1 means abnormal
        0 means there is no data and needs to be inserted
        1 means some fields need to be empty and needs to be updated
        2 means no processing, just skip
        """
        rt_code = 0
        try:
            check_sql = '''
            SELECT 
            sql_id, sql_type, sql_text, statement, table_list            
            FROM monitor_sql_text
            WHERE db_id=\'%s\' AND sql_id=\'%s\' ''' % (db_id, sql_id)
            result = self.meta_conn.func_select_storedb(check_sql)
            if result:
                statement = '' if not result[0].get('statement', '') else result[0]['statement']
                if not statement:
                    rt_code = 1
                else:
                    rt_code = 2
        except Exception as e:
            rt_code = -1
            log.exception(e)
        return rt_code

    def get_exist_index(self, db_id):
        exist_dict = {}
        try:
            check_sql = '''
            SELECT
                table_name,index_name,index_type,index_status,column_list
            FROM meta_table_index
            where db_id = '{db_id}'
            order by table_name'''.format(db_id=db_id)
            result = self.meta_conn.func_select_storedb(check_sql)
            if result:
                exist_dict = table_index_groupby(result)
        except Exception as e:
            log.exception(e)
        return exist_dict

    def get_exist_stats(self, db_id):
        exist_dict = {}
        try:
            check_sql = '''
            SELECT
            column_name, ndv_count, table_rows, table_name, min_value, max_value
            FROM meta_table_statistics
            WHERE db_id = '{db_id}'
            '''.format(db_id=db_id)
            result = self.meta_conn.func_select_storedb(check_sql)

            if result:
                exist_dict = table_stats_groupby(result)

        except Exception as e:
            log.exception(e)
        return exist_dict

    def func_write_storedb(self, sql_list, store_sql=''):
        try:
            self.meta_conn.func_write_storedb(sql_list, store_sql)
        except Exception as e:
            log.exception(e)

    def disconn_storedb(self):
        try:
            self.meta_conn.disconn_storedb()
        except Exception as e:
            pass


class MonitorDBInfo():
    """ read meta database for monitor query"""

    def __init__(self, get_retry=1):
        self.meta_conn = ConnDBOperate(metadb, get_retry=get_retry)

    def get_top_sql(self, database_alias, user_id, start_time, end_time, search_sql_text, search_context, search_name,
                    search_symbol):
        """ get top sql """
        rt_list = []

        try:

            if search_name and search_symbol and search_context:
                search_condition = """{search_name} {search_symbol} '{search_context}'""".format(
                    search_name=search_name,
                    search_symbol=search_symbol,
                    search_context=search_context)
            else:
                search_condition = """ 1 = 1 """

            if search_sql_text:
                sql = """ 
                SELECT
                    sqlId, sqlType, userName, affectedRows, executions, failTimes, rpcCount, remotePlans, missPlans,
                    returnRows, logicalReads, retryCnt, elapsedTime, queueTime, cpuTime, netwaitTime,
                    iowaitTime, getplanTime, totalWaitTime
                FROM ( 
                    SELECT 
                        a.sql_id as sqlId,
                        max(a.sql_type) as sqlType,
                        max(a.user_name) as userName,
                        round(avg(a.affected_rows)) affectedRows,
                        sum(a.executions) executions,
                        sum(a.fail_times) failTimes,
                        sum(a.rpc_count) rpcCount,
                        sum(a.remote_plans) remotePlans,
                        sum(a.miss_plans) missPlans,
                        round(avg(a.return_rows)) returnRows,
                        round(avg(a.logical_reads)) logicalReads,
                        sum(a.retry_cnt) retryCnt,
                        round(avg(a.elapsed_time))/1000 elapsedTime,
                        round(avg(a.queue_time))/1000 queueTime,
                        round(avg(a.cpu_time))/1000 cpuTime,
                        round(avg(a.netwait_time))/1000 netwaitTime,
                        round(avg(a.iowait_time))/1000 iowaitTime,
                        round(avg(a.getplan_time))/1000 getplanTime,
                        round(avg(a.total_wait_time))/1000 totalWaitTime
                    FROM
                        monitor_sql_auidt_oceanbase a
                        LEFT JOIN monitor_sql_text b
                    ON 
                        a.db_id=b.db_id and a.sql_id=b.sql_id
                    WHERE
                        a.db_id = %s
                        AND a.request_time >= FROM_UNIXTIME({start_time})
                        AND a.request_time <= FROM_UNIXTIME({end_time})
                        AND b.sql_text like %s
                    GROUP BY sqlId) sub
                WHERE
                    {search_condition}
                ORDER BY elapsedTime + getplanTime * 10 + returnRows * 10 + logicalReads * 10 DESC
                """.format(start_time=start_time, end_time=end_time, search_condition=search_condition)
                param = (Utils.get_db_id(database_alias, user_id), '%' + search_sql_text + '%')
            else:
                sql = """ 
                SELECT
                    sqlId, sqlType, userName, affectedRows, executions, failTimes, rpcCount, remotePlans, missPlans,
                    returnRows, logicalReads, retryCnt, elapsedTime, queueTime, cpuTime, netwaitTime,
                    iowaitTime, getplanTime, totalWaitTime
                FROM ( 
                    SELECT
                        sql_id as sqlId,
                        max(sql_type) as sqlType,
                        max(user_name) as userName,
                        round(avg(affected_rows)) affectedRows,
                        sum(executions) executions,
                        sum(fail_times) failTimes,
                        sum(rpc_count) rpcCount,
                        sum(remote_plans) remotePlans,
                        sum(miss_plans) missPlans,
                        round(avg(return_rows)) returnRows,
                        round(avg(logical_reads)) logicalReads,
                        sum(retry_cnt) retryCnt,
                        round(avg(elapsed_time))/1000 elapsedTime,
                        round(avg(queue_time))/1000 queueTime,
                        round(avg(cpu_time))/1000 cpuTime,
                        round(avg(netwait_time))/1000 netwaitTime,
                        round(avg(iowait_time))/1000 iowaitTime,
                        round(avg(getplan_time))/1000 getplanTime,
                        round(avg(total_wait_time))/1000 totalWaitTime
                    FROM
                        monitor_sql_auidt_oceanbase
                    WHERE
                        db_id = %s
                        AND request_time >= FROM_UNIXTIME({start_time})
                        AND request_time <= FROM_UNIXTIME({end_time})
                    GROUP BY sqlId) sub
                WHERE
                    {search_condition}
                ORDER BY elapsedTime + getplanTime * 10 + returnRows * 10 + logicalReads * 10 DESC
                """.format(start_time=start_time, end_time=end_time, search_condition=search_condition)
                param = (Utils.get_db_id(database_alias, user_id))
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_sql_plan(self, database_alias, user_id, sql_id):
        """ get sql plan """
        rt_list = []
        try:
            sql = """ 
            SELECT
                sql_type sqlType, query_sql querySql, first_load_time firstLoadTime, 
                avg_exe_usec/1000 avgExeMs, hit_count hitCount, plan_info planInfo, plan_detail planDetail, 
                outline_id outlineId, svr_ip svrIp
            FROM 
                monitor_sql_plan_oceanbase
            WHERE
                db_id = %s
                AND sql_id = %s
            """
            param = (Utils.get_db_id(database_alias, user_id), sql_id)
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_sql_text(self, database_alias, user_id, sql_id):
        """ get sql text """
        rt_list = []
        try:
            sql = """ 
            SELECT
                sql_text sqlText, user_name userName, table_list tableNameList
            FROM 
                monitor_sql_text
            WHERE
                db_id = %s
                AND sql_id = %s
            """
            param = (Utils.get_db_id(database_alias, user_id), sql_id)
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_sql_detail(self, database_alias, user_id, sql_id, start_time, end_time):
        """ get sql detail """
        rt_list = []
        try:
            sql = """ 
            SELECT
                sql_id as sqlId,
                sql_type as sqlType,
                user_name as userName,
                affected_rows affectedRows,
                executions executions,
                fail_times failTimes,
                rpc_count rpcCount,
                remote_plans remotePlans,
                miss_plans missPlans,
                return_rows returnRows,
                logical_reads logicalReads,
                retry_cnt retryCnt,
                elapsed_time/1000 elapsedTime,
                queue_time/1000 queueTime,
                cpu_time/1000 cpuTime,
                netwait_time/1000 netwaitTime,
                iowait_time/1000 iowaitTime,
                getplan_time/1000 getplanTime,
                total_wait_time/1000 totalWaitTime,
                request_time endTimeTs,
                svr_ip svrIP
            FROM 
                monitor_sql_auidt_oceanbase
            WHERE
                db_id = %s
                AND sql_id = %s
                AND request_time >= FROM_UNIXTIME({start_time})
                AND request_time <= FROM_UNIXTIME({end_time})
            ORDER BY request_time DESC
            """.format(start_time=start_time, end_time=end_time)
            param = (Utils.get_db_id(database_alias, user_id), sql_id)
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_table_index(self, database_alias, user_id, table_name):
        """ get sql detail """
        rt_list = []
        try:
            sql = """ 
            SELECT
                index_name indexName, index_type indexType, index_status indexStatus, column_list columnList
            FROM 
                meta_table_index
            WHERE
                db_id = %s
                AND table_name = %s
            ORDER BY table_name, index_type
            """
            param = (Utils.get_db_id(database_alias, user_id), table_name)
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_table_statistics(self, database_alias, user_id, table_name):
        """ get sql detail """
        rt_list = []
        try:
            sql = """ 
            SELECT
                column_name columnName, ndv_count ndvCount, table_rows tableRows
            FROM 
                meta_table_statistics
            WHERE
                db_id = %s
                AND table_name = %s
            ORDER BY column_name
            """
            param = (Utils.get_db_id(database_alias, user_id), table_name)
            rt_list = self.meta_conn.func_select_storedb(sql, param)
        except Exception as e:
            log.exception(e)
        return rt_list

    def func_write_storedb(self, sql_list, store_sql=''):
        try:
            self.meta_conn.func_write_storedb(sql_list, store_sql)
        except Exception as e:
            log.exception(e)

    def disconn_storedb(self):
        try:
            self.meta_conn.disconn_storedb()
        except Exception as e:
            pass
