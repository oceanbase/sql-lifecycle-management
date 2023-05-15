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
import time

import schedule

from common.const import *
from common.db_query import DealMetaDBInfo
from common.enum import *
from common.logger import Logger
from schedule_task.oceanbase.schedule_plan_oceanbase import schedule_plan_ob
from schedule_task.oceanbase.schedule_schema_oceanbase import schedule_schema_ob
from schedule_task.oceanbase.schedule_statistics_oceanbase import schedule_statistics_ob
from schedule_task.oceanbase.schedule_topsql_oceanbase import schedule_topsql_ob

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)


def schedule_func():
    # connect metadb
    meta_conn = DealMetaDBInfo(DB_CONNECT_RETRY)
    # get list to be scheduled
    todo_list = meta_conn.get_schedule_task('pull')
    for db_conf in todo_list:
        log.info('Schedule task: {} {}-{} {}:{} {}'.format(db_conf['db_id'], db_conf['engine'],
                                                           db_conf['version'], db_conf['host'],
                                                           db_conf['port'], db_conf['user']))
        # schedule_task oceanbase task
        if db_conf['engine'] == 'oceanbase':
            # Currently only support 3.x and 4.x version
            if db_conf['version'] < '3':
                log.error("oceanbase unsupport version: {}".format(db_conf['version']))
                continue
            # set connect infomation db, not user database
            db_conf['app_dbname'] = db_conf['dbname']
            db_conf['dbname'] = 'oceanbase'
            # set cluster_name and tenant_name
            if '@' in db_conf['user'] and '#' in db_conf['user']:
                db_conf['cluster_name'] = db_conf['user'].split('#')[-1]
                db_conf['tenant_name'] = db_conf['user'].split('@')[-1].split('#')[0]
            elif ':' in db_conf['user'] and db_conf['user'].count(':') == 2:
                db_conf['cluster_name'] = db_conf['user'].split(':')[0]
                db_conf['tenant_name'] = db_conf['user'].split(':')[1]
            else:
                log.error("oceanbase connect user config error: {}".format(db_conf['user']))
                continue
            approve_scope = db_conf['approve_scope']

            for approved_type in approve_scope.split(APPROVE_SCOPE_DELIMITER):
                # get checkpoint
                queue_dict = meta_conn.get_schedule_queue(db_conf['db_id'], approved_type)
                if approved_type == ApproveScopeEunm.SQL.value:
                    schedule_topsql_ob(db_conf, queue_dict, approved_type)
                if approved_type == ApproveScopeEunm.PLAN.value:
                    schedule_plan_ob(db_conf, queue_dict, approved_type)
                if approved_type == ApproveScopeEunm.SCHEMA.value:
                    schedule_schema_ob(db_conf)
                if approved_type == ApproveScopeEunm.STATISTICS.value:
                    schedule_statistics_ob(db_conf)

    # close connection
    meta_conn.disconn_storedb()


if __name__ == '__main__':
    schedule.every(1).minutes.do(schedule_func)

    while True:
        schedule.run_pending()
        time.sleep(60)
