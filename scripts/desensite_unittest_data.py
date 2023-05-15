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

from consume.file_parse_common import get_encoding
from consume.mysql_logparser_base import MysqlSlowLogParse


def func_desensitize_slowlog(file_list, db_version):
    ''' parse sensitive info in test file, then desensitize '''
    f_path = os.path.abspath('..')
    file_n = 0
    for log_file in file_list:
        read_encoding = get_encoding(log_file)
        # get sensitive keywords, include ip address and database_name
        ip_list = []
        db_list = []
        with open(log_file, "r", encoding=read_encoding) as f:
            for e in MysqlSlowLogParse(f, db_version):
                if e.host and e.host not in ip_list:
                    ip_list.append(e.host)
                if e.user and e.user not in db_list and len(e.user) >= 4:
                    db_list.append(e.user)
                if db_version == '5.6' and e.schema and e.schema not in db_list and len(e.schema) >= 4:
                    db_list.append(e.schema)
                elif db_version == '5.7' and e.database and e.database not in db_list and len(e.database) >= 4:
                    db_list.append(e.database)
        print('log_file:',log_file)
        print('ip_list:',ip_list)
        print('db_list:',db_list)
        # update desensitizing file
        des_ip = '127.0.0.1'
        des_db = 'asdfasdf'
        file_n += 1
        if db_version == '5.6':
            f_v = '56'
        else:
            f_v = '57'
        new_file = f_path + '/test/consume/mysql_slowlog/mysql_slowlog_test_' + f_v + '_' + str(file_n) + '.txt'
        print('new_file:',new_file)
        with open(log_file, "r", encoding=read_encoding) as f1, open(new_file, "w", encoding="utf-8") as f2:
            for line in f1:
                # check ip address
                for ip_info in ip_list:
                    if ip_info in line:
                        line = line.replace(ip_info, des_ip)
                        break
                # check database name
                for db_info in db_list:
                    if db_info in line:
                        line = line.replace(db_info, des_db)
                # write to newfile
                f2.write(line)
        print('####################################################################\n')




if __name__ == '__main__':
    f_path = os.path.abspath('..')
    # file_list = [ f_path + '/test/consume/mysql_slowlog/mysql_slowlog_56.txt' ]

    file_list_56 = [
        f_path + '/test/consume/mysql_slowlog/mysql_slowlog_56.txt',
        f_path + '/test/consume/mysql_slowlog/mysql_slowlog_56_2.txt',
        f_path + '/test/consume/mysql_slowlog/mysql_slowlog_56_3.txt',
        f_path + '/test/consume/mysql_slowlog/mysql_slowlog_56_4.txt'
    ]
    func_desensitize_slowlog(file_list_56, '5.6')

    file_list_57 = [
        f_path + '/test/consume/mysql_slowlog/mysql_slowlog_57.txt',
        f_path + '/test/consume/mysql_slowlog/slow57_2.txt',
        f_path + '/test/consume/mysql_slowlog/slow57_3.txt'
    ]
    func_desensitize_slowlog(file_list_57, '5.7')
