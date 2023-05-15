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
import decimal
import os
import re
import sys
import traceback

from common.logger import Logger

log_file = os.path.basename(sys.argv[0]).split(".")[0] + '.log'
log = Logger(log_file)


""" MySQL multi-version SlowQueryLog log format：
MySQL 5.6 build:
/u01/mysql/bin/mysqld, Version: 5.6.16.12.7-20170607-log (Source distribution). started with:
Tcp port: 3306 Unix socket: /u01/my3306/run/mysql.sock
Time Id Command Argument
# Time: 210720 11:59:46
# User@Host: xxxxx[xxxxx] @  [xx.xx.xx.xx]  Id: 89512547
# Schema: fpdecision  Last_errno: 0  Killed: 0
# Query_time: 0.778688  Lock_time: 0.000038  Rows_sent: 3381  Rows_examined: 35617  Rows_affected: 0
# Bytes_sent: 217826539  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# InnoDB_trx_id: 0
# QC_Hit: No  Full_scan: Yes  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
#   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
#   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
#   InnoDB_pages_read: 83668
use fpdecision;
SET timestamp=1626753586;
SELECT /*MS-SELECT-BY-CONDITION*/ * FROM db_table_name WHERE 1=1 AND is_valid = 1;

MySQL 5.7 Community Edition:
/usr/local/mysql/bin/mysqld, Version: 5.7.36 (MySQL Community Server (GPL)). started with:
Tcp port: 3306 Unix socket: /u01/mysql/mysql.sock
Time Id Command Argument
# Time: 2022-08-01T06:22:21.148963Z
# User@Host: admin[admin] @  [xx.xx.xx.xx]  Id:  3987
# Query_time: 11.959699  Lock_time: 0.000188 Rows_sent: 0  Rows_examined: 843008
use luli1;
SET timestamp=1659334941;
insert into tb_slow select * from tb_slow;
"""

# MySQL slow query log parsing regular expression
# common regular
RE_COMMON_HEADER = re.compile(r"(.+), Version: (\d+)\.(\d+)\.(\d+)(?:-(\S+))?")
RE_COMMON_SERVER = re.compile(r"Tcp port:\s*(\d+)\s+Unix socket:\s+(.*)")

# Schema: fpdecision  Last_errno: 0  Killed: 0
RE_COMMON_SCHEMA = re.compile(r"#\sSchema:\s"
                              r"(?:([\w\d]+))?\s*"
                              r"Last_errno:\s(\d*)\s*"
                              r"Killed:\s(\d*)")
# Thread_id: 23733137  Schema: 69hzw9btqxbkg06g
RE_COMMON_THREAD = re.compile(r"#\sThread_id:\s(\d*)\s*"
                              r"Schema:\s(.*)")

# according to the MySQL version, the log format is different, and the difference between the regular
# MySQL 5.6
# Time: 210720 11:59:46
FMT_MYSQL56_DATE = r"\d{6}\s+\d{1,2}:\d{2}:\d{2}"
RE_MYSQL56_SLOW_TIMESTAMP = re.compile(r"#\s+Time:\s+(" + FMT_MYSQL56_DATE + r")")
# User@Host: fpdecision[fpdecision] @  [127.0.0.1]  Id: 89512547
RE_MYSQL56_SLOW_USERHOST = re.compile(r"#\s+User@Host:\s+"
                                   r"(?:([\w\d]+))?\s*"
                                   r"\[\s*([\w\d]+)\s*\]\s*"
                                   r"@\s*"
                                   r"([\w\d\.\-]*)\s*"
                                   r"\[\s*([\d.]*)\s*\]\s*"
                                   r"(?:Id\:\s*(\d+)?\s*)?")
# Query_time: 0.778688  Lock_time: 0.000038  Rows_sent: 3381  Rows_examined: 35617  Rows_affected: 0
RE_MYSQL56_SLOW_STATS = re.compile(r"#\sQuery_time:\s(\d*\.\d{1,6})\s*"
                                r"Lock_time:\s(\d*\.\d{1,6})\s*"
                                r"Rows_sent:\s(\d*)\s*"
                                r"Rows_examined:\s(\d*)\s*"
                                r"Rows_affected:\s(\d*)")
# general log
RE_MYSQL56_GENERAL_ENTRY = re.compile(
    r'(?:(' + FMT_MYSQL56_DATE + '))?\s*'
                             r'(\d+)\s([\w ]+)\t*(?:(.+))?$')

# MySQL 5.7
# Time: 2022-08-01T06:22:21.148963Z
FMT_UTC_TIME = "%Y-%m-%dT%H:%M:%S.%fZ"
FMT_MYSQL57_DATE = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.?\d+Z?"
RE_MYSQL57_SLOW_TIMESTAMP = re.compile(r"#\s+Time:\s+(" + FMT_MYSQL57_DATE + r")")
# User@Host: admin[admin] @  [127.0.0.1]  Id:  3987
RE_MYSQL57_SLOW_USERHOST = re.compile(r"#\s+User@Host:\s+"
                                   r"(?:([\w\d]+))?\s*"
                                   r"\[\s*([\w\d]+)\s*\]\s*"
                                   r"@\s*"
                                   r"([\w\d\.\-]*)\s*"
                                   r"\[\s*([\d.]*)\s*\]\s*"
                                   r"(?:Id\:\s*(\d+)?\s*)?")
# Query_time: 11.959699  Lock_time: 0.000188 Rows_sent: 0  Rows_examined: 843008
RE_MYSQL57_SLOW_STATS = re.compile(r"#\sQuery_time:\s(\d*\.\d{1,6})\s*"
                                r"Lock_time:\s(\d*\.\d{1,6})\s*"
                                r"Rows_sent:\s(\d*)\s*"
                                r"Rows_examined:\s(\d*)")
# general log
RE_MYSQL57_GENERAL_ENTRY = re.compile(
    r'(?:(' + FMT_MYSQL57_DATE + '))?\s*'
                             r'(\d+)\s([\w ]+)\t*(?:(.+))?$')


class MysqlLogParserBase(object):
    """ MySQL log parse foundation class,
        include slowqueyr log and general log(in the near future)

        Input: logfile stream, must check file type first
        Output: iterator to retrieve the next line from stream
    """

    def __init__(self, stream):
        """ stream: open("/path/to/mysql.log") """
        self._stream = None
        self._version = None
        self._program = None
        self._port = None
        self._socket = None
        self._start_time = None
        self._last_time = None
        # Check file type
        line = None
        try:
            self._stream = stream
            start_pos = stream.tell()
            line = self._get_next_line()
        except AttributeError:
            raise Exception("Invalid file type, Please check input file.")
        # check file header
        if line is not None and line.endswith('started with:'):
            self._parse_header(line)
        else:
            self._stream.seek(start_pos)

    def _get_next_line(self):
        """ Get next line from stream """
        try:
            line = self._stream.readline()
        except:
            traceback.print_exc()
            line = None
        if not line:
            return None
        return line.rstrip('\r\n')

    def _parse_header(self, line):
        """Parse the file header

        Example:
            /u01/mysql/bin/mysqld, Version: 5.6.16.12.7-20170607-log (Source distribution). started with:
            Tcp port: 3306 Unix socket: /u01/my3306/run/mysql.sock
            Time Id Command Argument
        """
        if line is None:
            return
        # get version info
        info = RE_COMMON_HEADER.match(line)
        if not info:
            raise Exception("Parse header to get version error: %s" % line)
        program, major, minor, patch, extra = info.groups()
        # get server info
        line = self._get_next_line()
        info = RE_COMMON_SERVER.match(line)
        if not info:
            raise Exception("Parse header to get server error: %s" % line)
        tcp_port, unix_socket = info.groups()
        # skip column line
        self._get_next_line()
        # format return info
        self._version = (int(major), int(minor), int(patch), extra)
        self._program = program
        self._port = int(tcp_port)
        self._socket = unix_socket

    @property
    def version(self):
        """ return version like: (major, minor, patch, extra) """
        return self._version

    @property
    def program(self):
        """ return executable like: /usr/local/mysql/bin/mysqld """
        return self._program

    @property
    def port(self):
        """ :return server port like: 3306 """
        return self._port

    @property
    def socket(self):
        """ return UNIX socket like: /u01/mysql/mysql.sock """
        return self._socket

    @property
    def start_time(self):
        """ return the first time like: 2022-08-01 06:22:21.148963 (datatime) """
        return self._start_time

    @property
    def last_time(self):
        """ return the last time like: 2022-08-01 06:22:21.148963 (datatime) """
        return self._last_time

    def __iter__(self):
        """ return iterator to read every line of the log file """
        return self

    def __next__(self):
        """ return the next line """
        entry = self._parse_entry()
        if entry is None:
            raise StopIteration
        return entry

    def __str__(self):
        """ return description(string) """
        return "<%(classsname)s, MySQL v%(version)s>" % dict(
            classsname=self.__class__.__name__,
            version='.'.join([str(v) for v in self._version[0:3]]) +
                    (self._version[3] or '')
        )


class MysqlSlowLogParse(MysqlLogParserBase):
    """ MySQL Slow Query Log parse class
        Input:
            logfile stream: file stream, must check file type and charset first
            MySQL version: 5.6/5.7, default is 5.6(8.0 in the near future)
        Output: get SQL text, performance data, request time
    """

    def __init__(self, stream, db_version='5.6'):
        """ Input:
                stream: open("/path/to/mysql.log")
                db_version: 5.6 5.7, default is 5.6(8.0 in the near future)
        """
        super(MysqlSlowLogParse, self).__init__(stream)
        self._cache_line = None
        self._current_db = None
        self.db_version = db_version

    def _parse_line(self, regex, line):
        """ parse every line to get formatted data
            Input:
                regex: Matching of regular expressions
                line: String of logfile
            Output: tuple, like: (Query_time, Lock_time, Rows_sent, Rows_examined)
        """
        info = regex.match(line)
        if info is None:
            raise Exception('Parse slowlog line error: %s' % line[:50])
        return info.groups()

    def _parse_connect_info(self, line, entry):
        """ parse connect info
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                # User@Host: admin[admin] @ [xx.xx.xx.xx] Id: 3987
        """
        if self.db_version == '5.6':
            (priv_user, unpriv_user, host, ip, sid) = self._parse_line(RE_MYSQL56_SLOW_USERHOST, line)
        else:
            (priv_user, unpriv_user, host, ip, sid) = self._parse_line(RE_MYSQL57_SLOW_USERHOST, line)
        entry['user'] = priv_user if priv_user else unpriv_user
        entry['host'] = host if host else ip
        entry['session_id'] = sid

    def _parse_thread_info(self, line, entry):
        """ parse connect info
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                # Thread_id: 123456  Schema: asdgawergxxx
        """
        (thread_id, schema) = self._parse_line(RE_COMMON_THREAD, line)
        entry['thread_id'] = thread_id
        entry['schema'] = schema

    def _parse_schema_info(self, line, entry):
        """ parse connect info
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                # Schema: db_name  Last_errno: 0  Killed: 0
        """
        (schema, last_errno, killed) = self._parse_line(RE_COMMON_SCHEMA, line)
        entry['schema'] = schema
        entry['last_errno'] = last_errno
        entry['killed'] = killed

    def _parse_timestamp(self, line, entry):
        """parse connect info
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                # MySQL 5.6
                # Time: 210720 11:59:46
                # MySQL 5.7
                # Time: 2022-08-01T06:22:21.148963Z
                # MySQL 5.7 include timezone
                # Time: 2021-03-11T00:50:08.177158+08:00
        """
        if self.db_version == '5.6':
            info = self._parse_line(RE_MYSQL56_SLOW_TIMESTAMP, line)
            entry['datetime'] = datetime.datetime.strptime(info[0], "%y%m%d %H:%M:%S")
        else:
            info = self._parse_line(RE_MYSQL57_SLOW_TIMESTAMP, line)
            # Time: 2021-03-11T00:50:08.177158+08:00
            if not info[0].endswith('Z'):
                entry['datetime'] = datetime.datetime.strptime(info[0], FMT_UTC_TIME[:-1]) + datetime.timedelta(hours=8)
            # Time: 2022-08-01T06:22:21.148963Z
            else:
                entry['datetime'] = datetime.datetime.strptime(info[0], FMT_UTC_TIME) + datetime.timedelta(hours=8)
        if self._start_time is None:
            self._start_time = entry['datetime']
            self._last_time = entry['datetime']

    def _parse_performance(self, line, entry):
        """ parse SQL running performance data
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                # MySQL 5.6
                Query_time: 0.778688  Lock_time: 0.000038  Rows_sent: 33  Rows_examined: 617  Rows_affected: 0
                # MySQL 5.7
                Query_time: 11.959699  Lock_time: 0.000188 Rows_sent: 0  Rows_examined: 843008
        """
        if self.db_version == '5.6':
            result = self._parse_line(RE_MYSQL56_SLOW_STATS, line)
        else:
            result = self._parse_line(RE_MYSQL57_SLOW_STATS, line)
        entry['query_time'] = decimal.Decimal(result[0])
        entry['lock_time'] = decimal.Decimal(result[1])
        entry['rows_sent'] = int(result[2])
        entry['rows_examined'] = int(result[3])
        # entry['rows_affected'] = int(result[4])
        # entry['rows_read'] = int(result[5])

    def _parse_query(self, line, entry):
        """ parse SQL statement
            Input:
                line: String of logfile
                entry: slowlog instance
            Example:
                use INFORMATION_SCHEMA  -- switch database
                SET timestamp=1323169459;  -- set actual request time
                SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
                   WHERE SCHEMA_NAME = 'mysql';  -- SQL statement
        """
        query = []
        while True:
            if line is None:
                break
            if line.startswith('use'):
                entry['database'] = self._current_db = line.split(' ')[1]
            elif line.startswith('SET timestamp='):
                entry['datetime'] = datetime.datetime.fromtimestamp(
                    int(line[14:].strip(';')))
            elif (line.startswith('# Time:') or line.startswith("# User@Host")
                  or line.endswith('started with:')):
                break
            # 去除部分引擎自带的一些无用信息
            if not line.startswith('# '):
                query.append(line)
            line = self._get_next_line()

        # some uncommon scenario, use logined database name
        if 'database' in entry:
            if entry['database'] is None and self._current_db is not None:
                entry['database'] = self._current_db
        entry['query'] = '\n'.join(query)
        self._cache_line = line

    def _parse_entry(self):
        """ parse entry enumeration:
            1. request time, starts with '#'
            2. user info, starts with '#'
            3. schema info, starts with '#'
            4. thread info, starts with '#'
            5. performance info, starts with '#'
            6. SQL statement
                use <database>;
                SET timestamp=<request_time>;
                SET session variables;
                SQL statement;
        """
        if self._cache_line is not None:
            line = self._cache_line
            self._cache_line = None
        else:
            line = self._get_next_line()
        if line is None:
            return None

        while line.endswith('started with:'):
            # first header line
            header = self._parse_header(line)
            line = self._get_next_line()
            if line is None:
                return None

        entry = MysqlSlowLogEntry()

        if line.startswith('# Time:'):
            self._parse_timestamp(line, entry)
            line = self._get_next_line()

        if line.startswith('# User@Host:'):
            self._parse_connect_info(line, entry)
            line = self._get_next_line()

        if line.startswith('# Schema:'):
            self._parse_schema_info(line, entry)
            line = self._get_next_line()

        if line.startswith('# Thread_id'):
            self._parse_thread_info(line, entry)
            line = self._get_next_line()

        if line.startswith('# Query_time:'):
            self._parse_performance(line, entry)
            line = self._get_next_line()

        self._parse_query(line, entry)

        return entry


class MysqlLogEntryBase(dict):
    """ log entry class, include slowquery log and general log(soon)
        dict element can be accessed using attributes like: entry['user'] = entry.user
    """

    def __init__(self):
        self['datetime'] = None
        self['database'] = None
        self['user'] = None
        self['host'] = None
        self['session_id'] = None

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("%s has no attribute '%s'" % (
                self.__class__.__name__, name))


class MysqlSlowLogEntry(MysqlLogEntryBase):
    """ slowquery log entry, attribute can be used just like dictionary """

    def __init__(self):
        super(MysqlSlowLogEntry, self).__init__()
        self['query'] = None
        self['query_time'] = None
        self['lock_time'] = None
        self['rows_examined'] = None
        self['rows_sent'] = None
        self['rows_affected'] = None
        self['rows_read'] = None

    def __str__(self):
        """ String representation """
        param = self.copy()
        param['classsname'] = self.__class__.__name__
        try:
            param['datetime'] = param['datetime'].strftime("%Y-%m-%d %H:%M:%S")
        except AttributeError:
            param['datetime'] = ''
        return ("<%(classsname)s %(datetime)s [%(user)s@%(host)s] "
                "%(query_time)s/%(lock_time)s/%(rows_examined)s/%(rows_sent)s>"
                ) % param
