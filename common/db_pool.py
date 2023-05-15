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

import pymysql
from DBUtils.PooledDB import PooledDB
from pymysql.cursors import DictCursor

from common.logger import Logger

logfile = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(logfile)


class DBPool(object):
    """
    Gets DB Connection from the pool
    Sample code:
        conn = DBPool(db_conf, pool_min_size, pool_max_size, autocommit)

        db_conf = {
            host: *,
            port: *,
            user: *,
            pswd: *,
            dbname: *,
            charset: *
        }
    """
    __pool = None

    def __init__(self, storeob, minsess=1, maxsess=5, autocommit=0):
        self.db_host = storeob['host']
        self.db_port = storeob['port']
        self.db_user = storeob['user']
        self.db_pwd = storeob['pswd']
        self.db_name = storeob['dbname']
        self.db_charset = storeob['charset']
        self.minsess = minsess
        self.maxsess = maxsess
        self.autocommit = autocommit
        self._conn = DBPool.__get_conn(self.db_host, self.db_port, self.db_user, self.db_pwd, self.db_name,
                                       self.db_charset, self.minsess, self.maxsess, self.autocommit)
        self._cursor = self._conn.cursor()

    @staticmethod
    def __get_conn(db_host, db_port, db_user, db_pwd, db_name, db_charset, minsess, maxsess, autocommit):
        if DBPool.__pool is None:
            __pool = PooledDB(creator=pymysql, mincached=minsess, maxcached=maxsess,
                              host=db_host, port=db_port, user=db_user, passwd=db_pwd,
                              db=db_name, use_unicode=True, charset=db_charset, cursorclass=DictCursor,
                              setsession=['SET AUTOCOMMIT = ' + str(autocommit)])
            return __pool.connection()
        else:
            return DBPool.__pool.connection()

    def get_all(self, sql, param=None):
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = self._cursor.fetchall()
            else:
                result = False
            return result
        except Exception as e:
            log.exception(e)
            return ''

    def get_one(self, sql, param=None):
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = self._cursor.fetchone()
            else:
                result = False
            return result
        except Exception as e:
            log.exception(e)
            return ''

    def insert_one(self, sql, param=None):
        if param is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, param)
        return 1

    def insert_many(self, sql, param=None):
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.executemany(sql, param)
            return count
        except Exception as e:
            log.exception(e)
            return 1062

    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    def update(self, sql, param=None):
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        return self.__query(sql, param)

    def begin(self):
        self._conn.autocommit(0)

    def end(self, option='commit'):
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    def commit(self):
        self._conn.commit()

    def ping(self):
        self._conn.ping(True)

    def rollback(self):
        self._conn.rollback()

    def disconn(self):
        self._cursor.close()
        self._conn.close()


class ConnDBOperate():
    """
        simple connection class
        add retry times and batch operate
    """

    def __init__(self, dbconf, dbpool_size=1, retry_times=3, get_retry=1, autocommit=0):
        self.dbconf = dbconf
        self.dbpool_size = dbpool_size
        self.retry_times = retry_times
        self.get_retry = get_retry
        self.autocommit = autocommit
        self.stconn_cnt = 0
        self.stconn_mark = 0
        while (self.stconn_mark == 0 and self.stconn_cnt <= self.retry_times):
            self.stconn_cnt = self.stconn_cnt + 1
            try:
                self.storeconn = DBPool(self.dbconf, self.dbpool_size, self.dbpool_size, self.autocommit)
                self.stconn_mark = 1
            except Exception as e:
                self.stconn_mark = 0
                log.exception(e)

    def func_select_storedb(self, check_sql, check_param=None):
        result = ''
        try:
            if self.stconn_mark == 1:
                self.storeconn.ping()
                if check_param is None:
                    result = self.storeconn.get_all(check_sql)
                else:
                    result = self.storeconn.get_all(check_sql, check_param)
                self.storeconn.commit()
                # retry
                get_cnt = 0
                get_mark = 0
                while (result == '' and get_mark == 0 and get_cnt <= self.get_retry):
                    get_cnt = get_cnt + 1
                    self.storeconn.ping()
                    if check_param is None:
                        result = self.storeconn.get_all(check_sql)
                    else:
                        result = self.storeconn.get_all(check_sql, check_param)
                    self.storeconn.commit()
                    if result and result != '':
                        get_mark = 1
        except Exception as e:
            log.exception(e)

        if result:
            result = list(result)
        else:
            result = []
        return result

    def func_write_storedb(self, sql_list, store_sql='', dml_operate='single'):
        if self.stconn_mark == 1:
            self.storeconn.commit()
            cnt = 0
            for per_rst in sql_list:
                if store_sql:
                    result = self.storeconn.insert_one(store_sql, per_rst)
                else:
                    result = self.storeconn.insert_one(per_rst)
                if result != -1:
                    cnt = cnt + 1
                if dml_operate == 'single' and self.autocommit == 0:
                    if cnt % 100 == 0:
                        self.storeconn.commit()
                elif dml_operate == 'batch':
                    self.storeconn.commit()
            self.storeconn.commit()

    def disconn_storedb(self):
        try:
            self.storeconn.disconn()
        except:
            pass
