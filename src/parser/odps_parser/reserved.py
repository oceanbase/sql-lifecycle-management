# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

sql92_reserved = (
    'SELECT',
    'FROM',
    'ADD',
    'AS',
    'ALL',
    'SOME',
    'ANY',
    'DISTINCT',
    'WHERE',
    'GROUP',
    'BY',
    'ORDER',
    'HAVING',
    'AT',
    'OR',
    'AND',
    'IN',
    'NOT',
    'NO',
    'EXISTS',
    'BETWEEN',
    'LIKE',
    'IS',
    'NULL',
    'TRUE',
    'FALSE',
    'FIRST',
    'LAST',
    'ESCAPE',
    'ASC',
    'DESC',
    'SUBSTRING',
    'POSITION',
    'FOR',
    'DATE',
    'TIME',
    'INTERVAL',
    'YEAR',
    'MONTH',
    'DAY',
    'HOUR',
    'MINUTE',
    'SECOND',
    'ZONE',
    'CURRENT_DATE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'EXTRACT',
    'CASE',
    'WHEN',
    'THEN',
    'ELSE',
    'END',
    'JOIN',
    'CROSS',
    'OUTER',
    'INNER',
    'LEFT',
    'RIGHT',
    'FULL',
    'NATURAL',
    'USING',
    'ON',
    'ROWS',
    'CURRENT',
    'WITH',
    'VALUES',
    'CREATE',
    'TABLE',
    'VIEW',
    'INSERT',
    'DELETE',
    'INTO',
    'CONSTRAINT',
    'DESCRIBE',
    'GRANT',
    'PRIVILEGES',
    'PUBLIC',
    'OPTION',
    'CAST',
    'COLUMN',
    'DROP',
    'UNION',
    'EXCEPT',
    'INTERSECT',
    'TO',
    'ALTER',
    'SET',
    'SESSION',
    'TRANSACTION',
    'COMMIT',
    'ROLLBACK',
    'WORK',
    'ISOLATION',
    'LEVEL',
    'READ',
    'WRITE',
    'ONLY',
)

sql92_conditions = (
    'CONDITION_NUMBER',
    'RETURNED_SQLSTATE',
    'CLASS_ORIGIN',
    'SUBCLASS_ORIGIN',
    'SERVER_NAME',
    'CONNECTION_NAME',
    'CONSTRATIN_CATALOG',
    'CONSTRAINT_SCHEMA',
    'CONSTRAINT_NAME',
    'CATALOG_NAME',
    'SCHEMA_NAME',
    'TABLE_NAME',
    'COLUMN_NAME',
    'CURSOR_NAME',
    'MESSAGE_TEXT',
    'MESSAGE_LENGTH',
    'MESSAGE_OCTET_LENGTH',
)

sql92_languages = ('ADA', 'C', 'COBOL', 'FORTRAN', 'MUMPS', 'PASCAL', 'PLI')

sql92_descriptors = (
    'TYPE',
    'LENGTH',
    'OCTET_LENGTH',
    'RETURNED_LENGTH',
    'RETURNED_OCTET_LENGTH',
    'PRECISION',
    'SCALE',
    'DATETIME_INTERVAL_CODE',
    'DATETIME_INTERVAL_PRECISION',
    'NULLABLE',
    'INDICATOR',
    'DATA',
    'NAME',
    'UNNAMED',
    'COLLATION_CATALOG',
    'COLLATION_SCHEMA',
    'COLLATION_NAME',
    'CHARACTER_SET_CATALOG',
    'CHARACTER_SET_SCHEMA',
    'CHARACTER_SET_NAME',
)

sql92_transaction = ('COMMITTED', 'REPEATABLE', 'SERIALIZABLE', 'UNCOMMITTED')

sql92_statement_info = (
    'COMMAND_FUNCTION',
    'DYNAMIC_FUNCTION',
    'MORE',
    'NUMBER',
    'ROW_COUNT',
)

sql92_nonreserved = (
    ('CONSTRAINT_CATALOG',)
    + sql92_conditions
    + sql92_transaction
    + sql92_languages
    + sql92_descriptors
    + sql92_statement_info
)

sql99_reserved = (
    'ABSOLUTE',
    'ACTION',
    'ADD',
    'AFTER',
    'ALL',
    'ALLOCATE',
    'ALTER',
    'AND',
    'ANY',
    'ARE',
    'ARRAY',
    'AS',
    'ASC',
    'ASSERTION',
    'AT',
    'AUTHORIZATION',
    'BEFORE',
    'BEGIN',
    'BETWEEN',
    'BINARY',
    'BIT',
    'BLOB',
    'BOOLEAN',
    'BOTH',
    'BREADTH',
    'BY',
    'CALL',
    'CASCADE',
    'CASCADED',
    'CASE',
    'CAST',
    'CATALOG',
    'CHAR',
    'CHARACTER',
    'CHECK',
    'CLOB',
    'CLOSE',
    'COLLATE',
    'COLLATION',
    'COLUMN',
    'COMMIT',
    'CONDITION',
    'CONNECT',
    'CONNECTION',
    'CONSTRAINT',
    'CONSTRAINTS',
    'CONSTRUCTOR',
    'CONTINUE',
    'CORRESPONDING',
    'CREATE',
    'CROSS',
    'CUBE',
    'CURRENT',
    'CURRENT_DATE',
    'CURRENT_DEFAULT_TRANSFORM_GROUP',
    'CURRENT_TRANSFORM_GROUP_FOR_TYPE',
    'CURRENT_PATH',
    'CURRENT_ROLE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'CURRENT_USER',
    'CURSOR',
    'CYCLE',
    'DATA',
    'DATE',
    'DAY',
    'DEALLOCATE',
    'DEC',
    'DECIMAL',
    'DECLARE',
    'DEFAULT',
    'DEFERRABLE',
    'DEFERRED',
    'DELETE',
    'DEPTH',
    'DEREF',
    'DESC',
    'DESCRIBE',
    'DESCRIPTOR',
    'DETERMINISTIC',
    'DIAGNOSTICS',
    'DISCONNECT',
    'DISTINCT',
    'DO',
    'DOMAIN',
    'DOUBLE',
    'DROP',
    'DYNAMIC',
    'EACH',
    'ELSE',
    'ELSEIF',
    'END',
    'END-EXEC',
    'EQUALS',
    'ESCAPE',
    'EXCEPT',
    'EXCEPTION',
    'EXEC',
    'EXECUTE',
    'EXISTS',
    'EXIT',
    'EXTERNAL',
    'FALSE',
    'FETCH',
    'FIRST',
    'FLOAT',
    'FOR',
    'FOREIGN',
    'FOUND',
    'FROM',
    'FREE',
    'FULL',
    'FUNCTION',
    'GENERAL',
    'GET',
    'GLOBAL',
    'GO',
    'GOTO',
    'GRANT',
    'GROUP',
    'GROUPING',
    'HANDLE',
    'HAVING',
    'HOLD',
    'HOUR',
    'IDENTITY',
    'IF',
    'IMMEDIATE',
    'IN',
    'INDICATOR',
    'INITIALLY',
    'INNER',
    'INOUT',
    'INPUT',
    'INSERT',
    'INT',
    'INTEGER',
    'INTERSECT',
    'INTERVAL',
    'INTO',
    'IS',
    'ISOLATION',
    'JOIN',
    'KEY',
    'LANGUAGE',
    'LARGE',
    'LAST',
    'LATERAL',
    'LEADING',
    'LEAVE',
    'LEFT',
    'LEVEL',
    'LIKE',
    'LOCAL',
    'LOCALTIME',
    'LOCALTIMESTAMP',
    'LOCATOR',
    'LOOP',
    'MAP',
    'MATCH',
    'METHOD',
    'MINUTE',
    'MODIFIES',
    'MODULE',
    'MONTH',
    'NAMES',
    'NATIONAL',
    'NATURAL',
    'NCHAR',
    'NCLOB',
    'NESTING',
    'NEW',
    'NEXT',
    'NO',
    'NONE',
    'NOT',
    'NULL',
    'NUMERIC',
    'OBJECT',
    'OF',
    'OLD',
    'ON',
    'ONLY',
    'OPEN',
    'OPTION',
    'OR',
    'ORDER',
    'ORDINALITY',
    'OUT',
    'OUTER',
    'OUTPUT',
    'OVERLAPS',
    'PAD',
    'PARAMETER',
    'PARTIAL',
    'PATH',
    'PRECISION',
    'PREPARE',
    'PRESERVE',
    'PRIMARY',
    'PRIOR',
    'PRIVILEGES',
    'PROCEDURE',
    'PUBLIC',
    'READ',
    'READS',
    'REAL',
    'RECURSIVE',
    'REDO',
    'REF',
    'REFERENCES',
    'REFERENCING',
    'RELATIVE',
    'RELEASE',
    'REPEAT',
    'RESIGNAL',
    'RESTRICT',
    'RESULT',
    'RETURN',
    'RETURNS',
    'REVOKE',
    'RIGHT',
    'ROLE',
    'ROLLBACK',
    'ROLLUP',
    'ROUTINE',
    'ROW',
    'ROWS',
    'SAVEPOINT',
    'SCHEMA',
    'SCROLL',
    'SEARCH',
    'SECOND',
    'SECTION',
    'SELECT',
    'SESSION',
    'SESSION_USER',
    'SET',
    'SETS',
    'SIGNAL',
    'SIMILAR',
    'SIZE',
    'SMALLINT',
    'SOME',
    'SPACE',
    'SPECIFIC',
    'SPECIFICTYPE',
    'SQL',
    'SQLEXCEPTION',
    'SQLSTATE',
    'SQLWARNING',
    'START',
    'STATE',
    'STATIC',
    'SYSTEM_USER',
    'TABLE',
    'TEMPORARY',
    'THEN',
    'TIME',
    'TIMEZONE_HOUR',
    'TIMEZONE_MINUTE',
    'TO',
    'TRAILING',
    'TRANSACTION',
    'TRANSLATION',
    'TREAT',
    'TRIGGER',
    'TRUE',
    'UNDER',
    'UNDO',
    'UNION',
    'UNIQUE',
    'UNKNOWN',
    'UNNEST',
    'UNTIL',
    'UPDATE',
    'USAGE',
    'USER',
    'USING',
    'VALUE',
    'VALUES',
    'VARCHAR',
    'VARYING',
    'VIEW',
    'WHEN',
    'WHENEVER',
    'WHERE',
    'WHILE',
    'WITH',
    'WITHOUT',
    'WORK',
    'WRITE',
    'YEAR',
)

sql03_reserved = (
    'ADD',
    'ALL',
    'ALLOCATE',
    'ALTER',
    'AND',
    'ANY',
    'ARE',
    'ARRAY',
    'AS',
    'ASENSITIVE',
    'ASYMMETRIC',
    'AT',
    'ATOMIC',
    'AUTHORIZATION',
    'BEGIN',
    'BETWEEN',
    'BIGINT',
    'BINARY',
    'BLOB',
    'BOOLEAN',
    'BOTH',
    'BY',
    'CALL',
    'CALLED',
    'CASCADED',
    'CASE',
    'CAST',
    'CHAR',
    'CHARACTER',
    'CHECK',
    'CLOB',
    'CLOSE',
    'COLLATE',
    'COLUMN',
    'COMMIT',
    'CONNECT',
    'CONSTRAINT',
    'CONTINUE',
    'CORRESPONDING',
    'CREATE',
    'CROSS',
    'CUBE',
    'CURRENT',
    'CURRENT_DATE',
    'CURRENT_DEFAULT_TRANSFORM_GROUP',
    'CURRENT_PATH',
    'CURRENT_ROLE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'CURRENT_TRANSFORM_GROUP_FOR_TYPE',
    'CURRENT_USER',
    'CURSOR',
    'CYCLE',
    'DATE',
    'DAY',
    'DEALLOCATE',
    'DEC',
    'DECIMAL',
    'DECLARE',
    'DEFAULT',
    'DELETE',
    'DEREF',
    'DESCRIBE',
    'DETERMINISTIC',
    'DISCONNECT',
    'DISTINCT',
    'DOUBLE',
    'DROP',
    'DYNAMIC',
    'EACH',
    'ELEMENT',
    'ELSE',
    'END',
    'END-EXEC',
    'ESCAPE',
    'EXCEPT',
    'EXEC',
    'EXECUTE',
    'EXISTS',
    'EXTERNAL',
    'FALSE',
    'FETCH',
    'FILTER',
    'FLOAT',
    'FOR',
    'FOREIGN',
    'FREE',
    'FROM',
    'FULL',
    'FUNCTION',
    'GET',
    'GLOBAL',
    'GRANT',
    'GROUP',
    'GROUPING',
    'HAVING',
    'HOLD',
    'HOUR',
    'IDENTITY',
    'IMMEDIATE',
    'IN',
    'INDICATOR',
    'INNER',
    'INOUT',
    'INPUT',
    'INSENSITIVE',
    'INSERT',
    'INT',
    'INTEGER',
    'INTERSECT',
    'INTERVAL',
    'INTO',
    'IS',
    'ISOLATION',
    'JOIN',
    'LANGUAGE',
    'LARGE',
    'LATERAL',
    'LEADING',
    'LEFT',
    'LIKE',
    'LOCAL',
    'LOCALTIME',
    'LOCALTIMESTAMP',
    'MATCH',
    'MEMBER',
    'MERGE',
    'METHOD',
    'MINUTE',
    'MODIFIES',
    'MODULE',
    'MONTH',
    'MULTISET',
    'NATIONAL',
    'NATURAL',
    'NCHAR',
    'NCLOB',
    'NEW',
    'NO',
    'NONE',
    'NOT',
    'NULL',
    'NUMERIC',
    'OF',
    'OLD',
    'ON',
    'ONLY',
    'OPEN',
    'OR',
    'ORDER',
    'OUT',
    'OUTER',
    'OUTPUT',
    'OVER',
    'OVERLAPS',
    'PARAMETER',
    'PARTITION',
    'PRECISION',
    'PREPARE',
    'PRIMARY',
    'PROCEDURE',
    'RANGE',
    'READS',
    'REAL',
    'RECURSIVE',
    'REF',
    'REFERENCES',
    'REFERENCING',
    'REGR_AVGX',
    'REGR_AVGY',
    'REGR_COUNT',
    'REGR_INTERCEPT',
    'REGR_R2',
    'REGR_SLOPE',
    'REGR_SXX',
    'REGR_SXY',
    'REGR_SYY',
    'RELEASE',
    'RESULT',
    'RETURN',
    'RETURNS',
    'REVOKE',
    'RIGHT',
    'ROLLBACK',
    'ROLLUP',
    'ROW',
    'ROWS',
    'SAVEPOINT',
    'SCROLL',
    'SEARCH',
    'SECOND',
    'SELECT',
    'SENSITIVE',
    'SESSION_USER',
    'SET',
    'SIMILAR',
    'SMALLINT',
    'SOME',
    'SPECIFIC',
    'SPECIFICTYPE',
    'SQL',
    'SQLEXCEPTION',
    'SQLSTATE',
    'SQLWARNING',
    'START',
    'STATIC',
    'SUBMULTISET',
    'SYMMETRIC',
    'SYSTEM',
    'SYSTEM_USER',
    'TABLE',
    'THEN',
    'TIME',
    'TIMEZONE_HOUR',
    'TIMEZONE_MINUTE',
    'TO',
    'TRAILING',
    'TRANSLATION',
    'TREAT',
    'TRIGGER',
    'TRUE',
    'UESCAPE',
    'UNION',
    'UNIQUE',
    'UNKNOWN',
    'UNNEST',
    'UPDATE',
    'UPPER',
    'USE',
    'USER',
    'USING',
    'VALUE',
    'VALUES',
    'VAR_POP',
    'VAR_SAMP',
    'VARCHAR',
    'VARYING',
    'WHEN',
    'WHENEVER',
    'WHERE',
    'WIDTH_BUCKET',
    'WINDOW',
    'WITH',
    'WITHIN',
    'WITHOUT',
    'YEAR',
)

reserved = (
    'ADD',
    'ADDDATE',
    'ADDTIME',
    'ALL',
    'ALTER',
    'ANALYZE',
    'AND',
    'ARRAY',
    'AS',
    'ASC',
    'ATAN2',
    'BETWEEN',
    'BIGINT',
    'BINARY',
    'BLOB',
    'BOTH',
    'BRIEF',
    'BY',
    'CASCADE',
    'CASE',
    'CHANGE',
    'CHAR',
    'CHARACTER',
    'CHECK',
    'COLLATE',
    'CONCAT',
    'CONSTRAINT',
    'CONTINUE',
    'CONVERT',
    'CREATE',
    'CROSS',
    'CUME_DIST',
    'CURDATE',
    'CURRENT_DATE',
    'CURRENT_ROLE',
    'CURRENT_TIME',
    'CURRENT_TIMESTAMP',
    'CURRENT_USER',
    'CURSOR',
    'CURTIME',
    'DATABASE',
    'DATABASES',
    'DATEDIFF',
    'DATE_ADD',
    'DATE_SUB',
    'DAY_HOUR',
    'DAY_MICROSECOND',
    'DAY_MINUTE',
    'DAY_SECOND',
    'DEC',
    'DECIMAL',
    'DEFAULT',
    'DELAYED',
    'DELETE',
    'DENSE_RANK',
    'DESC',
    'DESCRIBE',
    'DISTINCT',
    'DISTINCTROW',
    'DIV',
    'DOUBLE',
    'DROP',
    'DUAL',
    'ELSE',
    'ELSEIF',
    'ENCLOSED',
    'ESCAPED',
    'EXCEPT',
    'EXISTS',
    'EXIT',
    'EXPLAIN',
    'EXTRACT',
    'FALSE',
    'FETCH',
    'FIRST_VALUE',
    'FLOAT',
    'FOR',
    'FORCE',
    'FOREIGN',
    'FROM',
    'FULL',
    'FULLTEXT',
    'GENERATED',
    'GET_FORMAT',
    'GRANT',
    'GROUP',
    'GROUP_CONCAT',
    'HAVING',
    'HIGH_PRIORITY',
    'HOUR_MICROSECOND',
    'HOUR_MINUTE',
    'HOUR_SECOND',
    'IF',
    'IGNORE',
    'ILIKE',
    'IN',
    'INDEX',
    'INFILE',
    'INNER',
    'INOUT',
    'INSERT',
    'INT',
    'INT1',
    'INT2',
    'INT3',
    'INT4',
    'INT8',
    'INTEGER',
    'INTERSECT',
    'INTERVAL',
    'JOIN',
    'KEY',
    'KEYS',
    'KILL',
    'LAG',
    'LEAD',
    'LEADING',
    'LEAVE',
    'LEFT',
    'LIKE',
    'LIMIT',
    'LINEAR',
    'LINES',
    'LOAD',
    'LOCALTIME',
    'LOCALTIMESTAMP',
    'LOCK',
    'LONG',
    'LONGBLOB',
    'LONGTEXT',
    'LOW_PRIORITY',
    'MATCH',
    'MAXVALUE',
    'MEDIUMBLOB',
    'MEDIUMINT',
    'MEDIUMTEXT',
    'MIN',
    'MINUTE_MICROSECOND',
    'MINUTE_SECOND',
    'MOD',
    'NATURAL',
    'NOT',
    'NO_WRITE_TO_BINLOG',
    'NTH_VALUE',
    'NTILE',
    'NULL',
    'NUMERIC',
    'ON',
    'OPTIMIZE',
    'OPTION',
    'OPTIONALLY',
    'ORDER',
    'OUT',
    'OUTER',
    'OUTFILE',
    'PARTITION',
    'PERCENT_RANK',
    'PRECISION',
    'PRIMARY',
    'PROCEDURE',
    'RADIANS',
    'RAND',
    'RANGE',
    'READ',
    'REAL',
    'RECURSIVE',
    'REFERENCES',
    'REGEXP',
    'REGEXP_INSTR',
    'REGEXP_LIKE',
    'REGEXP_REPLACE',
    'RELEASE',
    'RENAME',
    'REPEAT',
    'REQUIRE',
    'RESIGNAL',
    'RESTRICT',
    'RETURNED_SQLSTATE',
    'REVOKE',
    'RIGHT',
    'RLIKE',
    'ROW_NUMBER',
    'SECOND_MICROSECOND',
    'SELECT',
    'SET',
    'SHOW',
    'SIGNAL',
    'SIN',
    'SMALLINT',
    'SQL',
    'SQLEXCEPTION',
    'SQLSTATE',
    'SQLWARNING',
    'SQL_BIG_RESULT',
    'SQRT',
    'SSL',
    'STARTING',
    'STATS_EXTENDED',
    'STD',
    'STDDEV',
    'STDDEV_POP',
    'STDDEV_SAMP',
    'STRING',
    'SUBDATE',
    'SUBSTRING',
    'SUBTIME',
    'TABLE',
    'TABLESAMPLE',
    'TAN',
    'TERMINATED',
    'THEN',
    'TIMESTAMPADD',
    'TIMESTAMPDIFF',
    'TINYBLOB',
    'TINYINT',
    'TINYTEXT',
    'TO',
    'TOP',
    'TRAILING',
    'TRIGGER',
    'TRIM',
    'TRUE',
    'UNION',
    'UNSIGNED',
    'UNTIL',
    'UPDATE',
    'USAGE',
    'USING',
    'UTC_DATE',
    'UTC_TIME',
    'UTC_TIMESTAMP',
    'VARBINARY',
    'VARCHACTER',
    'VARCHAR',
    'VARIANCE',
    'VARYING',
    'VAR_POP',
    'VAR_SAMP',
    'VIRTUAL',
    'WHEN',
    'WHERE',
    'WHILE',
    'WINDOW',
    'YEAR_MONTH',
    '_BINARY',
)

nonreserved = (
    'ABS',
    'ACCESSIBLE',
    'ACCOUNT',
    'ACOS',
    'ACTION',
    'ACTIVATE',
    'ACTIVE',
    'AES_DECRYPT',
    'AES_ENCRYPT',
    'AFTER',
    'AGAINST',
    'AGGREGATE',
    'ALGORITHM',
    'ALWAYS',
    'ANALYSE',
    'ANY',
    'ANY_VALUE',
    'APPROX_COUNT_DISTINCT',
    'APPROX_COUNT_DISTINCT_SYNOPSIS',
    'APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE',
    'ARCHIVELOG',
    'ASCII',
    'ASENSITIVE',
    'ASIN',
    'ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_MANAGED',
    'ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_SOURCE',
    'ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_MANAGED',
    'ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_SOURCE',
    'ASYNCHRONOUS_CONNECTION_FAILOVER_RESET',
    'AT',
    'ATAN',
    'AUDIT',
    'AUTHORS',
    'AUTO',
    'AUTOEXTEND_SIZE',
    'AUTO_INCREMENT',
    'AVG',
    'AVG_ROW_LENGTH',
    'BACKUP',
    'BACKUPSET',
    'BALANCE',
    'BASE',
    'BASELINE',
    'BASELINE_ID',
    'BASIC',
    'BEFORE',
    'BEGI',
    'BENCHMARK',
    'BIN',
    'BINDING',
    'BINLOG',
    'BIN_TO_UUID',
    'BIT',
    'BIT_COUNT',
    'BIT_LENGTH',
    'BLOCK',
    'BLOCK_INDEX',
    'BLOCK_SIZE',
    'BLOOM_FILTER',
    'BOOL',
    'BOOLEAN',
    'BOOTSTRAP',
    'BREADTH',
    'BTREE',
    'BUCKETS',
    'BULK',
    'BYTE',
    'CACHE',
    'CALL',
    'CANCEL',
    'CASCADED',
    'CAST',
    'CATALOG_NAME',
    'CEIL',
    'CEILING',
    'CHAIN',
    'CHANGED',
    'CHARACTER_LENGT',
    'CHARACTER_LENGTH',
    'CHARSET',
    'CHAR_LENGTH',
    'CHECKPOINT',
    'CHECKSUM',
    'CHUNK',
    'CIPHER',
    'CLASS_ORIGIN',
    'CLEAN',
    'CLEAR',
    'CLIENT',
    'CLOG',
    'CLOSE',
    'CLUSTER',
    'CLUSTER_ID',
    'CLUSTER_NAME',
    'COALESCE',
    'CODE',
    'COERCIBILITY',
    'COPY',
    'COLLATION',
    'COLUMNS',
    'COLUMN_FORMAT',
    'COLUMN_NAME',
    'COLUMN_STAT',
    'COMMENT',
    'COMMIT',
    'COMMITTED',
    'COMPACT',
    'COMPLETION',
    'COMPRESS',
    'COMPRESSED',
    'COMPRESSION',
    'CONCAT_WS',
    'CONCURRENT',
    'CONNECTION',
    'CONNECTION_ID',
    'CONSISTENT',
    'CONSISTENT_MODE',
    'CONSTRAINT_CATALOG',
    'CONSTRAINT_NAME',
    'CONSTRAINT_SCHEMA',
    'CONTAINS',
    'CONTEXT',
    'CONTRIBUTORS',
    'CONY',
    'COS',
    'COT',
    'COUNT',
    'CPU',
    'CRC32',
    'CREATE_TIMESTAMP',
    'CTXCAT',
    'CTX_ID',
    'CUBE',
    'CURRENT',
    'CURSOR_NAME',
    'CYCLE',
    'DATA',
    'DATABASE_ID',
    'DATAFILE',
    'DATA_TABLE_ID',
    'DATE',
    'DATETIME',
    'DATE_FORMAT',
    'DAY',
    'DAYNAME',
    'DAYOFMONTH',
    'DAYOFWEEK',
    'DAYOFYEAR',
    'DEALLOCATE',
    'DECLARE',
    'DEFAULT_AUTH',
    'DEFAULT_TABLEGROUP',
    'DEFINER',
    'DEGREES',
    'DELAY',
    'DELAY_KEY_WRITE',
    'DEPTH',
    'DESTINATION',
    'DES_KEY_FILE',
    'DETERMINISTIC',
    'DIAGNOSTICS',
    'DIRECTORY',
    'DISABLE',
    'DISCARD',
    'DISK',
    'DISKGROUP',
    'DO',
    'DUMP',
    'DUMPFILE',
    'DUPLICATE',
    'DUPLICATE_SCOPE',
    'DYNAMIC',
    'EACH',
    'EFFECTIVE',
    'EGEXP_INSTR',
    'ELT',
    'ENABLE',
    'ENCRYPTION',
    'END',
    'ENDS',
    'ENGINE',
    'ENGINES',
    'ENGINE_',
    'ENTITY',
    'ENUM',
    'ERRORS',
    'ERROR_CODE',
    'ERROR_P',
    'ERSION',
    'ESCAPE',
    'EVENT',
    'EVENTS',
    'EVERY',
    'EXCHANGE',
    'EXECUTE',
    'EXP',
    'EXPANSION',
    'EXPIRE',
    'EXPIRED',
    'EXPIRE_INFO',
    'EXPORT',
    'EXPORT_SET',
    'EXTENDED',
    'EXTENDED_NOADDR',
    'EXTENT_SIZE',
    'EXTRACTVALUE',
    'FAST',
    'FAULTS',
    'FIELD',
    'FIELDS',
    'FILEX',
    'FILE_ID',
    'FINAL_COUNT',
    'FIND_IN_SET',
    'FIRST',
    'FIXED',
    'FLASHBACK',
    'FLOAT4',
    'FLOAT8',
    'FLOOR',
    'FLUSH',
    'FOLLOWER',
    'FOLLOWING',
    'FORMAT',
    'FOUND',
    'FOUND_ROWS',
    'FREEZE',
    'FREQUENCY',
    'FROM_BASE64',
    'FROM_DAYS',
    'FROM_UNIXTIME',
    'FUNCTION',
    'GENERAL',
    'GEOMETRY',
    'GEOMETRYCOLLECTION',
    'GET',
    'GET_LOCK',
    'GLOBAL',
    'GLOBAL_ALIAS',
    'GLOBAL_NAME',
    'GRANTS',
    'GREATEST',
    'GROUPS',
    'GROUPING',
    'GROUP_REPLICATION_DISABLE_MEMBER_ACTION',
    'GROUP_REPLICATION_ENABLE_MEMBER_ACTION',
    'GROUP_REPLICATION_GET_COMMUNICATION_PROTOCOL',
    'GROUP_REPLICATION_GET_WRITE_CONCURRENCY',
    'GROUP_REPLICATION_RESET_MEMBER_ACTIONS',
    'GROUP_REPLICATION_SET_AS_PRIMARY',
    'GROUP_REPLICATION_SET_COMMUNICATION_PROTOCOL',
    'GROUP_REPLICATION_SET_WRITE_CONCURRENCY',
    'GROUP_REPLICATION_SWITCH_TO_MULTI_PRIMARY_MODE',
    'GROUP_REPLICATION_SWITCH_TO_SINGLE_PRIMARY_MODE',
    'GTID_SUBSET',
    'GTID_SUBTRACT',
    'GTS',
    'HANDLER',
    'HASH',
    'HELP',
    'HEX',
    'HISTOGRAM',
    'HOST',
    'HOSTS',
    'HOST_IP',
    'HOUR',
    'ICU_VERSION',
    'ID',
    'IDC',
    'IDENTIFIED',
    'IFIGNORE',
    'IFNULL',
    'IGNORE_SERVER_IDS',
    'ILOG',
    'ILOGCACHE',
    'IMPORT',
    'INCR',
    'INCREMENTAL',
    'INDEXES',
    'INDEX_TABLE_ID',
    'INET6_ATON',
    'INET6_NTOA',
    'INET_ATON',
    'INET_NTOA',
    'INFO',
    'INITIAL_SIZE',
    'INTO',
    'INNER_PARSE',
    'INNODB',
    'INSENSITIVE',
    'INSERT_METHOD',
    'INSTALL',
    'INSTANCE',
    'INSTR',
    'INVISIBLE',
    'INVOKER',
    'IO',
    'IO_AFTER_GTIDS',
    'IO_BEFORE_GTIDS',
    'IO_THREAD',
    'IPC',
    'IS',
    'ISNULL',
    'ISOLATION',
    'ISSUER',
    'IS_FREE_LOCK',
    'IS_IPV4',
    'IS_IPV4_COMPAT',
    'IS_IPV4_MAPPED',
    'IS_IPV6',
    'IS_TENANT_SYS_POOL',
    'IS_USED_LOCK',
    'IS_UUID',
    'ITERATE',
    'JOB',
    'JSON',
    'JSON_ARRAY',
    'JSON_ARRAYAGG',
    'JSON_ARRAY_APPEND',
    'JSON_ARRAY_INSERT',
    'JSON_CONTAINS',
    'JSON_CONTAINS_PATH',
    'JSON_DEPTH',
    'JSON_EXTRACT',
    'JSON_INSERT',
    'JSON_KEYS',
    'JSON_LENGTH',
    'JSON_MERGE',
    'JSON_MERGE_PATCH',
    'JSON_MERGE_PRESERVE',
    'JSON_OBJECT',
    'JSON_OVERLAPS',
    'JSON_PERTTY',
    'JSON_QUOTE',
    'JSON_REMOVE',
    'JSON_REPLACE',
    'JSON_SCHEMA_VALID',
    'JSON_SCHEMA_VALIDATION_REPORT',
    'JSON_SEARCH',
    'JSON_SET',
    'JSON_STORAGE_FREE',
    'JSON_STORAGE_SIZE',
    'JSON_TABLE',
    'JSON_TYPE',
    'JSON_UNQUOTE',
    'JSON_VAILD',
    'JSON_VALUE',
    'KEY_BLOCK_SIZE',
    'KEY_VERSION',
    'KVCACHE',
    'LANGUAGE',
    'LAST',
    'LAST_DAY',
    'LAST_INSERT_ID',
    'LAST_VALUE',
    'LCASE',
    'LEADER',
    'LEAK',
    'LEAK_MOD',
    'LEAST',
    'LEAVES',
    'LENGTH',
    'LESS',
    'LEVEL',
    'LINESTRING',
    'LISTAGG',
    'LIST_',
    'LN',
    'LOAD_FILE',
    'LOB',
    'LOCAL',
    'LOCALITY',
    'LOCATE',
    'LOCATION',
    'LOCKED',
    'LOCKS',
    'LOCK_',
    'LOG',
    'LOG10',
    'LOG2',
    'LOGFILE',
    'LOGONLY_REPLICA_NUM',
    'LOGS',
    'LONGB',
    'LOOP',
    'LOWER',
    'LPAD',
    'LTRIM',
    'MAJOR',
    'MAKEDATE',
    'MAKE_SE',
    'MAKE_SET',
    'MANUAL',
    'MASTER',
    'MASTER_AUTO_POSITION',
    'MASTER_BIND',
    'MASTER_CONNECT_RETRY',
    'MASTER_DELAY',
    'MASTER_HEARTBEAT_PERIOD',
    'MASTER_HOST',
    'MASTER_LOG_FILE',
    'MASTER_LOG_POS',
    'MASTER_PASSWORD',
    'MASTER_PORT',
    'MASTER_POS_WAIT',
    'MASTER_RETRY_COUNT',
    'MASTER_SERVER_ID',
    'MASTER_SSL',
    'MASTER_SSL_CA',
    'MASTER_SSL_CAPATH',
    'MASTER_SSL_CERT',
    'MASTER_SSL_CIPHER',
    'MASTER_SSL_CRL',
    'MASTER_SSL_CRLPATH',
    'MASTER_SSL_KEY',
    'MASTER_SSL_VERIFY_SERVER_CERT',
    'MASTER_USER',
    'MATCHED',
    'MATERIALIZED',
    'MAX',
    'MAX_CONNECTIONS_PER_HOUR',
    'MAX_CPU',
    'MAX_DISK_SIZE',
    'MAX_IOPS',
    'MAX_MEMORY',
    'MAX_PT',
    'MAX_QUERIES_PER_HOUR',
    'MAX_ROWS',
    'MAX_SESSION_NUM',
    'MAX_SIZE',
    'MAX_UPDATES_PER_HOUR',
    'MAX_USED_PART_ID',
    'MAX_USER_CONNECTIONS',
    'MD5',
    'MEDIUM',
    'MEMBER',
    'MEMORY',
    'MEMTABLE',
    'MERGE',
    'MESSAGE_TEXT',
    'META',
    'MICROSECOND',
    'MID',
    'MIDDLEINT',
    'MIGRATE',
    'MIGRATION',
    'MINOR',
    'MINUTE',
    'MIN_CPU',
    'MIN_IOPS',
    'MIN_MEMORY',
    'MIN_ROWS',
    'MKEDATE',
    'MODE',
    'MODIFIES',
    'MODIFY',
    'MONTH',
    'MONTHNAME',
    'MOVE',
    'MULTILINESTRING',
    'MULTIPOINT',
    'MULTIPOLYGON',
    'MUTEX',
    'MYSQL_ERRNO',
    'NAME',
    'NAMES',
    'NAME_CONST',
    'NATIONAL',
    'NCHAR',
    'NDB',
    'NDBCLUSTER',
    'NEW',
    'NEXT',
    'NO',
    'NOARCHIVELOG',
    'NODEGROUP',
    'NONE',
    'NORMAL',
    'NOW',
    'NOWAIT',
    'NO_WAIT',
    'NULLS',
    'NULLIF',
    'NVARCHAR',
    'NVL',
    'OAD_FILE',
    'OCCUR',
    'OCT',
    'OCTET_LENGTH',
    'OERCIBILITY',
    'OF',
    'OFF',
    'OFFSET',
    'OLD_KEY',
    'OLD_PASSWORD',
    'ONE',
    'ONE_SHOT',
    'ONLY',
    'ONTHNAME',
    'OPEN',
    'OPTIONS',
    'OR',
    'ORA_DECODE',
    'ORD',
    'ORIG_DEFAULT',
    'OUTLINE',
    'OVER',
    'OWER',
    'OWNER',
    'PACE',
    'PACK_KEYS',
    'PAGE',
    'PARAMETERS',
    'PARSER',
    'PARTIAL',
    'PARTITIONING',
    'PARTITIONS',
    'PARTITION_ID',
    'PASSWORD',
    'PAUSE',
    'PCTFREE',
    'PERIOD_ADD',
    'PERIOD_DIFF',
    'PHASE',
    'PHYSICAL',
    'PI',
    'PL',
    'PLAN',
    'PLANREGRESS',
    'PLUGIN',
    'PLUGINS',
    'PLUGIN_DIR',
    'POINT',
    'POLYGON',
    'POOL',
    'PORT',
    'POSITION',
    'POW',
    'POWER',
    'PRECEDING',
    'PREPARE',
    'PRESERVE',
    'PREV',
    'PREVIEW',
    'PRIMARY_ZONE',
    'PRIVILEGES',
    'PROCESS',
    'PROCESSLIST',
    'PROFILE',
    'PROFILES',
    'PROGRESSIVE_MERGE_NUM',
    'PROXY',
    'PURGE',
    'P_CHUNK',
    'P_ENTITY',
    'QUARTER',
    'QUERY',
    'QUICK',
    'QUOTE',
    'R32',
    'RANDOM',
    'RANDOM_BYTES',
    'RANK',
    'READS',
    'READ_ONLY',
    'READ_WRITE',
    'REBUILD',
    'RECOVER',
    'RECYCLE',
    'RECYCLEBIN',
    'REDOFILE',
    'REDO_BUFFER_SIZE',
    'REDUNDANT',
    'REFRESH',
    'REGION',
    'RELAY',
    'RELAYLOG',
    'RELAY_LOG_FILE',
    'RELAY_LOG_POS',
    'RELAY_THREAD',
    'RELEASE_ALL_LOCKS',
    'RELEASE_LOCK',
    'RELOAD',
    'REMOTE_OSS',
    'REMOVE',
    'REORGANIZE',
    'REPAIR',
    'REPEATABLE',
    'REPLACE',
    'REPLICA',
    'REPLICATION',
    'REPLICA_NUM',
    'REPLICA_TYPE',
    'REPORT',
    'RESET',
    'RESOURCE',
    'RESOURCE_POOL_LIST',
    'RESPECT',
    'RESTART',
    'RESTORE',
    'RESUME',
    'RETURN',
    'RETURNING',
    'RETURNS',
    'REVERSE',
    'REWRITE_MERGE_VERSION',
    'ROLES_GRAPHML',
    'ROLLBACK',
    'ROLLING',
    'ROLLUP',
    'ROM_BASE64',
    'ROM_UNIXTIME',
    'ROOT',
    'ROOTSERVICE',
    'ROOTTABLE',
    'ROTATE',
    'ROUTINE',
    'ROUND',
    'ROW',
    'ROWS',
    'ROW_COUNT',
    'ROW_FORMAT',
    'RPAD',
    'RTREE',
    'RTRIM',
    'RUDUNDANT',
    'RUN',
    'SAMPLE',
    'SAVEPOINT',
    'SCHEDULE',
    'SCHEMA',
    'SCHEMAS',
    'SCHEMA_NAME',
    'SCOPE',
    'SEARCH',
    'SECOND',
    'SECURITY',
    'SEC_TO_TIME',
    'SEED',
    'SENSITIVE',
    'SEPARATOR',
    'SERIAL',
    'SERIALIZABLE',
    'SERVER',
    'SERVER_IP',
    'SERVER_PORT',
    'SERVER_TYPE',
    'SESSION',
    'SESSION_ALIAS',
    'SESSION_USER',
    'SET_MASTER_CLUSTER',
    'SET_SLAVE_CLUSTER',
    'SET_TP',
    'SHA',
    'SHA1',
    'SHA2',
    'SHARE',
    'SHUTDOWN',
    'SKIP',
    'SIGN',
    'SIGNED',
    'SIMPLE',
    'SINGLE_AT_IDENTIFIER',
    'SLAVE',
    'SLEEP',
    'SLOT_IDX',
    'SLOW',
    'SNAPSHOT',
    'SOCKET',
    'SOME',
    'SONAME',
    'SOUNDEX',
    'SOUNDS',
    'SOURCE',
    'SOURCE_POS_WAIT',
    'SPACE',
    'SPATIAL',
    'SPECIFIC',
    'SPFILE',
    'SPLIT',
    'SQL_AFTER_GTIDS',
    'SQL_AFTER_MTS_GAPS',
    'SQL_BEFORE_GTIDS',
    'SQL_BUFFER_RESULT',
    'SQL_CACHE',
    'SQL_CALC_FOUND_ROWS',
    'SQL_ID',
    'SQL_NO_CACHE',
    'SQL_SMALL_RESULT',
    'SQL_THREAD',
    'SQL_TSI_DAY',
    'SQL_TSI_HOUR',
    'SQL_TSI_MINUTE',
    'SQL_TSI_MONTH',
    'SQL_TSI_QUARTER',
    'SQL_TSI_SECOND',
    'SQL_TSI_WEEK',
    'SQL_TSI_YEAR',
    'STANDBY',
    'START',
    'STARTS',
    'STAT',
    'STATEMENT_DIGEST',
    'STATEMENT_DIGEST_TEXT',
    'STATS_AUTO_RECALC',
    'STATS_PERSISTENT',
    'STATS_SAMPLE_PAGES',
    'STATUS',
    'STOP',
    'STORAGE',
    'STORAGE_FORMAT_VERSION',
    'STORAGE_FORMAT_WORK_VERSION',
    'STORED',
    'STORING',
    'STRAIGHT_JOIN',
    'STRCMP',
    'STR_TO_DATE',
    'SUBCLASS_ORIGIN',
    'SUBJECT',
    'SUBPARTITION',
    'SUBPARTITIONS',
    'SUBSTR',
    'SUBSTRING_INDEX',
    'SUPER',
    'SUM',
    'SUSPEND',
    'SWAPS',
    'SWITCH',
    'SWITCHES',
    'SWITCHOVER',
    'SYNCHRONIZATION',
    'SYSDATE',
    'SYSTEM',
    'SYSTEM_USER',
    'TABLEGROUP',
    'TABLEGROUPS',
    'TABLEGROUP_ID',
    'TABLES',
    'TABLESPACE',
    'TABLET',
    'TABLET_MAX_SIZE',
    'TABLET_SIZE',
    'TABLE_CHECKSUM',
    'TABLE_ID',
    'TABLE_MODE',
    'TABLE_NAME',
    'TASK',
    'TATEMENT_DIGEST',
    'TEMPLATE',
    'TEMPORARY',
    'TEMPTABLE',
    'TENANT',
    'TENANT_ID',
    'TEXT',
    'THAN',
    'TIME',
    'TIMEDIFF',
    'TIMESTAMP',
    'TIME_FORMAT',
    'TIME_TO_SEC',
    'TIME_TO_USEC',
    'TIME_ZONE_INFO',
    'TO_BASE64',
    'TO_DAYS',
    'TO_SECONDS',
    'TP_NAME',
    'TP_NO',
    'TRACE',
    'TRADITIONAL',
    'TRANSACTION',
    'TRIGGERS',
    'TRUNCATE',
    'TYPE',
    'TYPES',
    'UBTIME',
    'UCASE',
    'UNBOUNDED',
    'UNCOMMITTED',
    'UNCOMPRESS',
    'UNCOMPRESSED_LENGTH',
    'UNDEFINED',
    'UNDO',
    'UNDOFILE',
    'UNDO_BUFFER_SIZE',
    'UNHEX',
    'UNICODE',
    'UNIQUE',
    'UNINSTALL',
    'UNIT',
    'UNIT_NUM',
    'UNIX_TIMESTAMP',
    'UNKNOWN',
    'UNLOCK',
    'UNLOCKED',
    'UNUSUAL',
    'UOTE',
    'UPDATEXML',
    'UPGRADE',
    'UPPER',
    'USEC_TO_TIME',
    'USER',
    'USE',
    'USER_RESOURCES',
    'USE_BLOOM_FILTER',
    'USE_FRM',
    'UUID',
    'UUID_SHORT',
    'UUID_TO_BIN',
    'VALID',
    'VALIDATE',
    'VALIDATE_PASSWORD_STRENGTH',
    'VALUE',
    'VALUES',
    'VARCHARACTER',
    'VARIABLES',
    'VAR_VARIANCE',
    'VERBOSE',
    'VERSION',
    'VIEW',
    'VIRTUAL_COLUMN_ID',
    'VISIBLE',
    'WAIT',
    'WAIT_FOR_EXECUTED_GTID_SET',
    'WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS',
    'WARNINGS',
    'WEEK',
    'WEEKDAY',
    'WEEKOFYEAR',
    'WEIGHT_STRING',
    'WITH',
    'WITH_ROWID',
    'WORK',
    'WRAPPER',
    'WRITE',
    'X509',
    'XA',
    'XML',
    'XOR',
    'XTRACTVALUE',
    'YEAR',
    'YEARWEEK',
    'ZEROFILL',
    'ZONE',
    'ZONE_LIST',
    'ZONE_TYPE',
)
