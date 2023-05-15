# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import os
import sys

from common.const import *
from common.db_pool import ConnDBOperate
from common.db_query import DealMetaDBInfo
from common.logger import Logger
from pymysql import escape_string

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)

# If the number of table rows is less than table_rows_limit, it will not be collected
table_rows_limit = 100000


class DealUserInfoOceanbase():
    """ read approved data from user database-oceanbase """

    def __init__(self, conn_info, get_retry):
        self.conn_info = conn_info
        self.db_conn = ConnDBOperate(self.conn_info, get_retry=get_retry)

    def get_tenant_list(self):
        """
        Obtain the list of tenants and process them according to tenants, sys tenant need to be excluded
        :return:
        """
        rt_dict = {}
        try:
            tenant_db = 'gv$tenant'
            if self.conn_info['version'] >= '4':
                tenant_db = 'DBA_OB_TENANTS'
            check_sql = '''
            SELECT 
            tenant_id,tenant_name 
            FROM {tenant_db} 
            WHERE tenant_id >= 1000
            '''.format(tenant_db=tenant_db)
            result = self.db_conn.func_select_storedb(check_sql)
            if result:
                for per_tnt in result:
                    rt_dict[per_tnt['tenant_id']] = per_tnt['tenant_name']
        except Exception as e:
            log.exception(e)
        return rt_dict

    def get_table_list(self):
        """
        Obtain a list of tables that need to be processed by tenant,
        the number of table rows must be greater than the threshold before processing,
        and exclude recycle bins and some backup tables
        :return:
        """
        rt_list = []
        table_stat = '__all_table_stat_v2'

        if self.conn_info['version'] >= '4':
            table_stat = '__all_table_stat'

        try:
            check_sql = '''
            SELECT 
            a.tenant_id,a.database_id,a.table_id,a.table_name,sum(b.row_cnt) table_rows
            FROM __all_virtual_table a,{table_stat} b
            WHERE a.table_type=3 and a.table_id=b.table_id
            and lower(a.table_name) not like 'recycle_%'
            and lower(a.table_name) not like '__recycle_%'
            and lower(a.table_name) not like 'tmp%'
            and lower(a.table_name) not like '%_bak'
            and lower(a.table_name) not like '%_back'
            and lower(a.table_name) not like '%_backup'
            GROUP BY a.tenant_id,a.database_id,a.table_id,a.table_name
            HAVING sum(b.row_cnt)>{table_rows_limit}
            ORDER BY a.tenant_id,a.database_id,a.table_id,a.table_name;
            '''.format(table_stat=table_stat, table_rows_limit=table_rows_limit)
            result = self.db_conn.func_select_storedb(check_sql)
            if result:
                rt_list = list(result)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_database_list(self):
        """
        Get the comparison information of database id and name
        """
        rt_dict = {}
        try:
            check_sql = '''
            SELECT  
            database_id,database_name 
            FROM __all_database
            ORDER BY tenant_id,database_id;'''
            result = self.db_conn.func_select_storedb(check_sql)
            if result:
                for per_db in result:
                    rt_dict[per_db['database_id']] = per_db['database_name']
        except Exception as e:
            log.exception(e)
        return rt_dict

    def get_column_list(self, table_id):
        """
        Get field information by table id
        """
        rt_list = []
        try:
            column_statistic_table = '__all_column_stat_v2'
            if self.conn_info['version'] >= '4':
                column_statistic_table = '__all_column_stat'
            check_sql = '''SELECT  
            b.column_name column_name, max(a.distinct_cnt) ndv_count
            FROM {column_statistic_table} a,__all_column b
            WHERE a.table_id=b.table_id and a.column_id=b.column_id
            and b.column_name not like '%__substr%'
             and a.table_id={table_id}
            GROUP BY b.column_name
            ORDER BY b.column_name'''.format(column_statistic_table=column_statistic_table,
                                             table_id=table_id)
            result = self.db_conn.func_select_storedb(check_sql)
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


def schedule_statistics_ob(db_conf):
    """
        get sql plan from oceanbase
    """
    db_id = db_conf['db_id']
    # connect user oceanbase and metadb
    meta_conn = DealMetaDBInfo(DB_CONNECT_RETRY)
    user_conn = DealUserInfoOceanbase(db_conf, DB_CONNECT_RETRY)
    baseline = meta_conn.get_exist_stats(db_id)
    deal(user_conn, baseline)

    # Summarize local and baseline data
    result_list = []
    for table_name in baseline:
        for column_name in baseline[table_name]:
            update_sql = '''
            REPLACE INTO
            meta_table_statistics
            (
                db_id, table_name, column_name, ndv_count, table_rows, gmt_create
            )VALUES 
            (
                '{db_id}', '{table_name}', '{column_name}', '{ndv_count}', '{table_rows}', now()
            )
            '''.format(db_id=db_id,
                       table_name=table_name,
                       column_name=column_name,
                       ndv_count=baseline[table_name][column_name]['ndv_count'],
                       table_rows=baseline[table_name][column_name]['table_rows'])
            result_list.append(update_sql)
    if result_list:
        meta_conn.func_write_storedb(result_list)

    # close connection
    meta_conn.disconn_storedb()
    user_conn.disconn_storedb()


def deal(user_conn, baseline):
    """
    Group and summarize local data by tenant
    """
    try:
        tab_list = user_conn.get_table_list()
        if not tab_list:
            return
        db_dict = user_conn.get_database_list()
        for per_tab in tab_list:
            database_id = per_tab['database_id']
            table_id = per_tab['table_id']
            table_name = per_tab['table_name']
            table_rows = per_tab['table_rows']
            db_name = db_dict[database_id]
            if db_name.lower().startswith('__recycle_') or db_name.lower().startswith('recycle_'):
                continue
            if table_name not in baseline:
                baseline[table_name] = {}
            # Obtain the column statistics of the table
            col_list = user_conn.get_column_list(table_id)
            for per_col in col_list:
                column_name = per_col['column_name']
                ndv_count = per_col['ndv_count']
                update_mark = 0
                if column_name in baseline[table_name]:
                    # If the local baseline ndv is less than or equal to the current value,
                    # and the number of rows in the local baseline value table is less than the current value,
                    # it will be updated. The larger the cardinality of the table, the more representative it is
                    if baseline[table_name][column_name]['ndv_count'] <= ndv_count \
                            and baseline[table_name][column_name]['table_rows'] < table_rows:
                        update_mark = 1
                else:
                    # If the local baseline does not have this value, update
                    update_mark = 1
                if update_mark:
                    baseline[table_name][column_name] = {}
                    baseline[table_name][column_name]['table_name'] = str(table_name)
                    baseline[table_name][column_name]['ndv_count'] = int(ndv_count)
                    baseline[table_name][column_name]['table_rows'] = int(table_rows)
    except Exception as e:
        log.exception(e)
