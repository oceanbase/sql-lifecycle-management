# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import itertools
import os
import sys

from common.const import *
from common.db_pool import ConnDBOperate
from common.db_query import DealMetaDBInfo
from common.logger import Logger

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)

idxtype_map = {0: '1.primary', 5: '1.primary', 1: '3.normal', 2: '2.unique',
               3: '3.normal', 4: '2.unique', 7: '3.normal', 8: '2.unique'}

idxstat_map = {1: 'creating', 2: 'normal', 3: 'uk_merge_twice'}


class DealUserInfoOceanbase():
    """ read approved data from user database-oceanbase """

    def __init__(self, conn_info, get_retry):
        self.conn_info = conn_info
        self.db_conn = ConnDBOperate(self.conn_info, get_retry=get_retry)

    def get_database_list(self):
        rt_list = []
        try:
            tenant_table = 'gv$tenant'
            database_table = '__all_database'
            if self.conn_info['version'] >= '4':
                tenant_table = 'DBA_OB_TENANTS'

            check_sql = '''
            SELECT 
            a.tenant_id,a.tenant_name,
            b.database_id,b.database_name
            FROM {tenant_table} a,{database_table} b
            WHERE a.tenant_id!=1
            AND b.database_name not in ('oceanbase','information_schema','mysql','__recyclebin','test','DWEXP',
            'OBMIGRATE','OMC','SYS','IDB_DDL','ODC_DDL','ODC_RND','__public')
            AND b.database_name not like '__recycle_%'
            ORDER BY tenant_id,database_name;'''.format(tenant_table=tenant_table, database_table=database_table)
            result = self.db_conn.func_select_storedb(check_sql)
            if result:
                rt_list = list(result)
        except Exception as e:
            log.exception(e)
        return rt_list

    def get_index_list(self, database_id):
        idx_list = []
        try:
            inner_tab_table = '__all_table'
            inner_tab_column = '__all_column'

            if self.conn_info['version'] >= '4':
                inner_tab_table = '__all_virtual_table'

            # get_primary
            get_sql = '''
                SELECT /*+ QUERY_TIMEOUT(60000000), READ_CONSISTENCY(WEAK), leading(a,d) use_hash(a,d) */
                a.table_id,a.table_name,a.index_type,d.column_name,d.orig_default_value_v2 orig_default_value,
                d.rowkey_position rank
                FROM {inner_tab_table} a,{inner_tab_column} d
                WHERE a.table_type=3 and a.table_id=d.table_id and d.rowkey_position!=0
                AND a.database_id = {database_id}
                ORDER BY a.table_id,a.table_name,rank
                '''.format(inner_tab_table=inner_tab_table, inner_tab_column=inner_tab_column,
                           database_id=database_id)
            result = self.db_conn.func_select_storedb(get_sql)
            pk_list, table_dict = self.deal_primary_key(list(result))
            idx_list.extend(pk_list)
            # get_other
            get_sql = '''
                SELECT /*+ QUERY_TIMEOUT(60000000), READ_CONSISTENCY(WEAK), leading(c,d) use_hash(c,d) */
                c.table_name index_name,c.data_table_id,c.index_status,c.index_type,d.column_name,
                d.orig_default_value_v2 orig_default_value,d.rowkey_position rank
                FROM {inner_tab_table} c, {inner_tab_column} d
                WHERE c.table_type=5 and c.table_id=d.table_id and d.index_position!=0
                AND c.database_id = {database_id}
                ORDER BY c.data_table_id,c.table_name,rank
                '''.format(inner_tab_table=inner_tab_table, inner_tab_column=inner_tab_column,
                           database_id=database_id)
            result = self.db_conn.func_select_storedb(get_sql)
            nk_list = self.deal_other_key(list(result), table_dict)
            idx_list.extend(nk_list)
        except Exception as e:
            log.exception(e)
        return idx_list

    def deal_primary_key(self, deal_list):
        rt_list = []
        table_dict = {}
        if deal_list:
            data_list = sorted(deal_list, key=lambda item: (item['table_id'], item['table_name'], item['rank']))
            for (table_id, table_name), group_data in itertools.groupby(data_list, key=lambda item: (
                    item['table_id'], item['table_name'])):
                try:
                    if not table_name:
                        continue
                    if table_name.startswith('__recycle_'):
                        continue
                    if table_id not in table_dict:
                        table_dict[table_id] = table_name
                    index_name = 'PRIMARY'
                    index_status = 'normal'
                    index_type = ''
                    tmp_list = []
                    tmp_dict = {}
                    for per_idx in group_data:
                        inner_type = int(per_idx['index_type'])
                        column_name = str(per_idx['column_name'])
                        orig_name = '' if not per_idx.get('orig_default_value', '') else per_idx['orig_default_value']
                        if not index_type:
                            index_type = idxtype_map.get(inner_type, '3.normal')
                        if orig_name and column_name.lower().startswith('__substr'):
                            try:
                                column_name = orig_name.replace('`', '').split('(')[1].split(',')[0].strip()
                            except:
                                pass
                        tmp_list.append(column_name)
                    column_list = ';'.join(tmp_list)
                    tmp_dict['table_name'] = table_name
                    tmp_dict['index_name'] = index_name
                    tmp_dict['index_status'] = index_status
                    tmp_dict['index_type'] = index_type
                    tmp_dict['column_list'] = column_list
                    rt_list.append(tmp_dict)
                except Exception as e:
                    log.exception(e)

        return rt_list, table_dict

    def deal_other_key(self, deal_list, table_dict):
        rt_list = []
        if deal_list:
            data_list = sorted(deal_list, key=lambda item: (item['data_table_id'], item['index_name'], item['rank']))
            for (data_table_id, index_name), group_data in itertools.groupby(data_list, key=lambda item: (
                    item['data_table_id'], item['index_name'])):
                try:
                    if not index_name:
                        continue
                    if data_table_id not in table_dict:
                        continue
                    table_name = table_dict[data_table_id]
                    if table_name.startswith('__recycle_'):
                        continue
                    index_name = index_name.split(str(data_table_id) + '_')[1].lower()
                    index_status = ''
                    index_type = ''
                    tmp_list = []
                    tmp_dict = {}
                    for per_idx in group_data:
                        inner_type = int(per_idx['index_type'])
                        inner_stat = int(per_idx['index_status'])
                        column_name = str(per_idx['column_name'])
                        orig_name = '' if not per_idx.get('orig_default_value', '') else per_idx['orig_default_value']
                        if not index_type:
                            index_type = idxtype_map.get(inner_type, '3.normal')
                        if not index_status:
                            index_status = idxstat_map.get(inner_stat, 'invalid')
                        if orig_name and column_name.lower().startswith('__substr'):
                            try:
                                table_id = per_idx['data_table_id']
                                substr_sql = '''
                                SELECT /*+ QUERY_TIMEOUT(60000000), READ_CONSISTENCY(WEAK) */
                                orig_default_value_v2 orig_default_value from __all_column
                                WHERE 
                                table_id = {table_id} AND column_name = '{column_name}';
                                '''.format(table_id=table_id,
                                           column_name=column_name)
                                result = self.db_conn.func_select_storedb(substr_sql)
                                orig_name = result[0].get('orig_default_value')
                                if orig_name:
                                    column_name = orig_name.replace('`', '').split('(')[1].split(',')[0].strip()
                            except:
                                pass
                        tmp_list.append(column_name)
                    column_list = ';'.join(tmp_list)
                    tmp_dict['table_name'] = table_name
                    tmp_dict['index_name'] = index_name
                    tmp_dict['index_status'] = index_status
                    tmp_dict['index_type'] = index_type
                    tmp_dict['column_list'] = column_list
                    rt_list.append(tmp_dict)
                except Exception as e:
                    log.exception(e)
        return rt_list

    def disconn_storedb(self):
        try:
            self.db_conn.disconn_storedb()
        except Exception as e:
            pass


def schedule_schema_ob(db_conf):
    """
        get sql plan from oceanbase
    """
    db_id = db_conf['db_id']
    # connect user oceanbase and metadb
    meta_conn = DealMetaDBInfo(DB_CONNECT_RETRY)
    user_conn = DealUserInfoOceanbase(db_conf, DB_CONNECT_RETRY)

    todo_list = user_conn.get_database_list()

    result_list = []

    for per_tnt in todo_list:
        database_id = int(per_tnt['database_id'])
        # get exist index from meta db
        exist_dict = meta_conn.get_exist_index(db_id)
        # get index info from user db
        idx_list = user_conn.get_index_list(database_id)
        if idx_list:
            data_list = sorted(idx_list, key=lambda item: (item['table_name']))
            for (table_name), group_data in itertools.groupby(data_list, key=lambda item: (item['table_name'])):
                if not table_name:
                    continue
                done_list = []
                sub_key = '{0}'.format(table_name)
                if exist_dict and sub_key in exist_dict:
                    st_idx_dict = exist_dict[sub_key]
                else:
                    st_idx_dict = {}
                # deal_per_index
                for per_idx in group_data:
                    index_name = str(per_idx['index_name'])
                    index_status = str(per_idx['index_status'])
                    index_type = str(per_idx['index_type'])
                    column_list = str(per_idx['column_list'])
                    done_list.append(index_name)
                    if st_idx_dict and index_name in st_idx_dict:
                        if index_status == st_idx_dict[index_name]['index_status'] \
                                and index_type == st_idx_dict[index_name]['index_type'] \
                                and column_list.lower() == st_idx_dict[index_name]['column_list'].lower():
                            continue
                        else:
                            store_sql = '''
                            UPDATE meta_table_index
                            SET 
                            index_status = '{index_status}', 
                            index_type = '{index_type}', 
                            column_list = '{column_list}', 
                            gmt_modify = now()
                            WHERE 
                            db_id = '{db_id}
                            AND table_name = '{table_name}'
                            AND index_name = '{index_name}'
                            '''.format(index_status=index_status, index_type=index_type, column_list=column_list,
                                       db_id=db_id, table_name=table_name, index_name=index_name)
                    else:
                        store_sql = '''
                        INSERT IGNORE INTO meta_table_index
                        (
                            db_id,table_name,index_name,index_type,index_status,column_list,gmt_create
                        )
                        VALUES
                        (
                            '{db_id}','{table_name}','{index_name}',
                            '{index_type}','{index_status}','{column_list}', now()
                        )'''.format(db_id=db_id, table_name=table_name, index_name=index_name, index_type=index_type,
                                    index_status=index_status, column_list=column_list)
                    if store_sql:
                        result_list.append(store_sql)

    if result_list:
        meta_conn.func_write_storedb(result_list)

    # close connection
    meta_conn.disconn_storedb()
    user_conn.disconn_storedb()
