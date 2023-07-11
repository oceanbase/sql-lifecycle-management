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
    'SELECT', 'FROM', 'ADD', 'AS', 'ALL', 'SOME', 'ANY', 'DISTINCT', 'WHERE', 'GROUP', 'BY',
    'ORDER', 'HAVING', 'AT', 'OR', 'AND', 'IN', 'NOT', 'NO', 'EXISTS', 'BETWEEN', 'LIKE', 'IS',
    'NULL', 'TRUE', 'FALSE', 'FIRST', 'LAST', 'ESCAPE', 'ASC', 'DESC', 'SUBSTRING', 'POSITION',
    'FOR', 'DATE', 'TIME', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE',
    'SECOND', 'ZONE', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'EXTRACT', 'CASE',
    'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'CROSS', 'OUTER', 'INNER', 'LEFT', 'RIGHT', 'FULL',
    'NATURAL', 'USING', 'ON', 'ROWS', 'CURRENT', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'VIEW',
    'INSERT', 'DELETE', 'INTO', 'CONSTRAINT', 'DESCRIBE', 'GRANT', 'PRIVILEGES', 'PUBLIC',
    'OPTION', 'CAST', 'COLUMN', 'DROP', 'UNION', 'EXCEPT', 'INTERSECT', 'TO', 'ALTER', 'SET',
    'SESSION', 'TRANSACTION', 'COMMIT', 'ROLLBACK', 'WORK', 'ISOLATION', 'LEVEL', 'READ', 'WRITE',
    'ONLY'
)

sql92_conditions = (
    'CONDITION_NUMBER', 'RETURNED_SQLSTATE', 'CLASS_ORIGIN', 'SUBCLASS_ORIGIN', 'SERVER_NAME',
    'CONNECTION_NAME', 'CONSTRATIN_CATALOG', 'CONSTRAINT_SCHEMA', 'CONSTRAINT_NAME',
    'CATALOG_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'CURSOR_NAME', 'MESSAGE_TEXT',
    'MESSAGE_LENGTH', 'MESSAGE_OCTET_LENGTH'
)

sql92_languages = (
    'ADA', 'C', 'COBOL', 'FORTRAN', 'MUMPS', 'PASCAL', 'PLI'
)

sql92_descriptors = (
    'TYPE', 'LENGTH', 'OCTET_LENGTH', 'RETURNED_LENGTH', 'RETURNED_OCTET_LENGTH', 'PRECISION',
    'SCALE', 'DATETIME_INTERVAL_CODE', 'DATETIME_INTERVAL_PRECISION', 'NULLABLE', 'INDICATOR',
    'DATA', 'NAME', 'UNNAMED', 'COLLATION_CATALOG', 'COLLATION_SCHEMA', 'COLLATION_NAME',
    'CHARACTER_SET_CATALOG', 'CHARACTER_SET_SCHEMA', 'CHARACTER_SET_NAME'
)

sql92_transaction = (
    'COMMITTED', 'REPEATABLE', 'SERIALIZABLE', 'UNCOMMITTED'
)

sql92_statement_info = (
    'COMMAND_FUNCTION', 'DYNAMIC_FUNCTION', 'MORE', 'NUMBER', 'ROW_COUNT'
)

sql92_nonreserved = ('CONSTRAINT_CATALOG',) + sql92_conditions + sql92_transaction + \
                    sql92_languages + sql92_descriptors + sql92_statement_info

sql99_reserved = (
    'ABSOLUTE', 'ACTION', 'ADD', 'AFTER', 'ALL', 'ALLOCATE', 'ALTER', 'AND', 'ANY', 'ARE', 'ARRAY',
    'AS', 'ASC', 'ASSERTION', 'AT', 'AUTHORIZATION', 'BEFORE', 'BEGIN', 'BETWEEN', 'BINARY', 'BIT',
    'BLOB', 'BOOLEAN', 'BOTH', 'BREADTH', 'BY', 'CALL', 'CASCADE', 'CASCADED', 'CASE', 'CAST',
    'CATALOG', 'CHAR', 'CHARACTER', 'CHECK', 'CLOB', 'CLOSE', 'COLLATE', 'COLLATION', 'COLUMN',
    'COMMIT', 'CONDITION', 'CONNECT', 'CONNECTION', 'CONSTRAINT', 'CONSTRAINTS', 'CONSTRUCTOR',
    'CONTINUE', 'CORRESPONDING', 'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'CURRENT_DATE',
    'CURRENT_DEFAULT_TRANSFORM_GROUP', 'CURRENT_TRANSFORM_GROUP_FOR_TYPE', 'CURRENT_PATH',
    'CURRENT_ROLE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER', 'CURSOR', 'CYCLE', 'DATA',
    'DATE', 'DAY', 'DEALLOCATE', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DEFERRABLE', 'DEFERRED',
    'DELETE', 'DEPTH', 'DEREF', 'DESC', 'DESCRIBE', 'DESCRIPTOR', 'DETERMINISTIC', 'DIAGNOSTICS',
    'DISCONNECT', 'DISTINCT', 'DO', 'DOMAIN', 'DOUBLE', 'DROP', 'DYNAMIC', 'EACH', 'ELSE',
    'ELSEIF', 'END', 'END-EXEC', 'EQUALS', 'ESCAPE', 'EXCEPT', 'EXCEPTION', 'EXEC', 'EXECUTE',
    'EXISTS', 'EXIT', 'EXTERNAL', 'FALSE', 'FETCH', 'FIRST', 'FLOAT', 'FOR', 'FOREIGN', 'FOUND',
    'FROM', 'FREE', 'FULL', 'FUNCTION', 'GENERAL', 'GET', 'GLOBAL', 'GO', 'GOTO', 'GRANT', 'GROUP',
    'GROUPING', 'HANDLE', 'HAVING', 'HOLD', 'HOUR', 'IDENTITY', 'IF', 'IMMEDIATE', 'IN',
    'INDICATOR', 'INITIALLY', 'INNER', 'INOUT', 'INPUT', 'INSERT', 'INT', 'INTEGER', 'INTERSECT',
    'INTERVAL', 'INTO', 'IS', 'ISOLATION', 'JOIN', 'KEY', 'LANGUAGE', 'LARGE', 'LAST', 'LATERAL',
    'LEADING', 'LEAVE', 'LEFT', 'LEVEL', 'LIKE', 'LOCAL', 'LOCALTIME', 'LOCALTIMESTAMP', 'LOCATOR',
    'LOOP', 'MAP', 'MATCH', 'METHOD', 'MINUTE', 'MODIFIES', 'MODULE', 'MONTH', 'NAMES', 'NATIONAL',
    'NATURAL', 'NCHAR', 'NCLOB', 'NESTING', 'NEW', 'NEXT', 'NO', 'NONE', 'NOT', 'NULL', 'NUMERIC',
    'OBJECT', 'OF', 'OLD', 'ON', 'ONLY', 'OPEN', 'OPTION', 'OR', 'ORDER', 'ORDINALITY', 'OUT',
    'OUTER', 'OUTPUT', 'OVERLAPS', 'PAD', 'PARAMETER', 'PARTIAL', 'PATH', 'PRECISION', 'PREPARE',
    'PRESERVE', 'PRIMARY', 'PRIOR', 'PRIVILEGES', 'PROCEDURE', 'PUBLIC', 'READ', 'READS', 'REAL',
    'RECURSIVE', 'REDO', 'REF', 'REFERENCES', 'REFERENCING', 'RELATIVE', 'RELEASE', 'REPEAT',
    'RESIGNAL', 'RESTRICT', 'RESULT', 'RETURN', 'RETURNS', 'REVOKE', 'RIGHT', 'ROLE', 'ROLLBACK',
    'ROLLUP', 'ROUTINE', 'ROW', 'ROWS', 'SAVEPOINT', 'SCHEMA', 'SCROLL', 'SEARCH', 'SECOND',
    'SECTION', 'SELECT', 'SESSION', 'SESSION_USER', 'SET', 'SETS', 'SIGNAL', 'SIMILAR', 'SIZE',
    'SMALLINT', 'SOME', 'SPACE', 'SPECIFIC', 'SPECIFICTYPE', 'SQL', 'SQLEXCEPTION', 'SQLSTATE',
    'SQLWARNING', 'START', 'STATE', 'STATIC', 'SYSTEM_USER', 'TABLE', 'TEMPORARY', 'THEN', 'TIME',
    'TIMEZONE_HOUR', 'TIMEZONE_MINUTE', 'TO', 'TRAILING', 'TRANSACTION',
    'TRANSLATION', 'TREAT', 'TRIGGER', 'TRUE', 'UNDER', 'UNDO', 'UNION', 'UNIQUE', 'UNKNOWN',
    'UNNEST', 'UNTIL', 'UPDATE', 'USAGE', 'USER', 'USING', 'VALUE', 'VALUES', 'VARCHAR', 'VARYING',
    'VIEW', 'WHEN', 'WHENEVER', 'WHERE', 'WHILE', 'WITH', 'WITHOUT', 'WORK', 'WRITE', 'YEAR'
)

sql03_reserved = (
    'ADD', 'ALL', 'ALLOCATE', 'ALTER', 'AND', 'ANY', 'ARE', 'ARRAY', 'AS', 'ASENSITIVE',
    'ASYMMETRIC', 'AT', 'ATOMIC', 'AUTHORIZATION', 'BEGIN', 'BETWEEN', 'BIGINT', 'BINARY', 'BLOB',
    'BOOLEAN', 'BOTH', 'BY', 'CALL', 'CALLED', 'CASCADED', 'CASE', 'CAST', 'CHAR', 'CHARACTER',
    'CHECK', 'CLOB', 'CLOSE', 'COLLATE', 'COLUMN', 'COMMIT', 'CONNECT', 'CONSTRAINT', 'CONTINUE',
    'CORRESPONDING', 'CREATE', 'CROSS', 'CUBE', 'CURRENT', 'CURRENT_DATE',
    'CURRENT_DEFAULT_TRANSFORM_GROUP', 'CURRENT_PATH', 'CURRENT_ROLE', 'CURRENT_TIME',
    'CURRENT_TIMESTAMP', 'CURRENT_TRANSFORM_GROUP_FOR_TYPE', 'CURRENT_USER', 'CURSOR', 'CYCLE',
    'DATE', 'DAY', 'DEALLOCATE', 'DEC', 'DECIMAL', 'DECLARE', 'DEFAULT', 'DELETE', 'DEREF',
    'DESCRIBE', 'DETERMINISTIC', 'DISCONNECT', 'DISTINCT', 'DOUBLE', 'DROP', 'DYNAMIC', 'EACH',
    'ELEMENT', 'ELSE', 'END', 'END-EXEC', 'ESCAPE', 'EXCEPT', 'EXEC', 'EXECUTE', 'EXISTS',
    'EXTERNAL', 'FALSE', 'FETCH', 'FILTER', 'FLOAT', 'FOR', 'FOREIGN', 'FREE', 'FROM', 'FULL',
    'FUNCTION', 'GET', 'GLOBAL', 'GRANT', 'GROUP', 'GROUPING', 'HAVING', 'HOLD', 'HOUR',
    'IDENTITY', 'IMMEDIATE', 'IN', 'INDICATOR', 'INNER', 'INOUT', 'INPUT', 'INSENSITIVE', 'INSERT',
    'INT', 'INTEGER', 'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'ISOLATION', 'JOIN', 'LANGUAGE',
    'LARGE', 'LATERAL', 'LEADING', 'LEFT', 'LIKE', 'LOCAL', 'LOCALTIME', 'LOCALTIMESTAMP', 'MATCH',
    'MEMBER', 'MERGE', 'METHOD', 'MINUTE', 'MODIFIES', 'MODULE', 'MONTH', 'MULTISET', 'NATIONAL',
    'NATURAL', 'NCHAR', 'NCLOB', 'NEW', 'NO', 'NONE', 'NOT', 'NULL', 'NUMERIC', 'OF', 'OLD', 'ON',
    'ONLY', 'OPEN', 'OR', 'ORDER', 'OUT', 'OUTER', 'OUTPUT', 'OVER', 'OVERLAPS', 'PARAMETER',
    'PARTITION', 'PRECISION', 'PREPARE', 'PRIMARY', 'PROCEDURE', 'RANGE', 'READS', 'REAL',
    'RECURSIVE', 'REF', 'REFERENCES', 'REFERENCING', 'REGR_AVGX', 'REGR_AVGY', 'REGR_COUNT',
    'REGR_INTERCEPT', 'REGR_R2', 'REGR_SLOPE', 'REGR_SXX', 'REGR_SXY', 'REGR_SYY', 'RELEASE',
    'RESULT', 'RETURN', 'RETURNS', 'REVOKE', 'RIGHT', 'ROLLBACK', 'ROLLUP', 'ROW', 'ROWS',
    'SAVEPOINT', 'SCROLL', 'SEARCH', 'SECOND', 'SELECT', 'SENSITIVE', 'SESSION_USER', 'SET',
    'SIMILAR', 'SMALLINT', 'SOME', 'SPECIFIC', 'SPECIFICTYPE', 'SQL', 'SQLEXCEPTION', 'SQLSTATE',
    'SQLWARNING', 'START', 'STATIC', 'SUBMULTISET', 'SYMMETRIC', 'SYSTEM', 'SYSTEM_USER', 'TABLE',
    'THEN', 'TIME', 'TIMEZONE_HOUR', 'TIMEZONE_MINUTE', 'TO', 'TRAILING',
    'TRANSLATION', 'TREAT', 'TRIGGER', 'TRUE', 'UESCAPE', 'UNION', 'UNIQUE', 'UNKNOWN', 'UNNEST',
    'UPDATE', 'UPPER', 'USER', 'USING', 'VALUE', 'VALUES', 'VAR_POP', 'VAR_SAMP', 'VARCHAR',
    'VARYING', 'WHEN', 'WHENEVER', 'WHERE', 'WIDTH_BUCKET', 'WINDOW', 'WITH', 'WITHIN', 'WITHOUT',
    'YEAR'
)

presto_tokens = (
    'SELECT', 'FROM', 'ADD', 'AS', 'ALL', 'SOME', 'ANY', 'DISTINCT', 'WHERE', 'GROUP', 'BY',
    'GROUPING', 'SETS', 'CUBE', 'ROLLUP', 'ORDER', 'HAVING', 'LIMIT', 'APPROXIMATE', 'AT',
    'CONFIDENCE', 'OR', 'AND', 'IN', 'NOT', 'NO', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL',
    'TRUE', 'FALSE', 'NULLS', 'FIRST', 'LAST', 'ESCAPE', 'ASC', 'DESC', 'SUBSTRING', 'POSITION',
    'FOR', 'DATE', 'TIME', 'INTERVAL', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE',
    'SECOND', 'ZONE', 'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'LOCALTIME',
    'LOCALTIMESTAMP', 'EXTRACT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'CROSS', 'OUTER',
    'INNER', 'LEFT', 'RIGHT', 'FULL', 'NATURAL', 'USING', 'ON', 'OVER', 'PARTITION', 'RANGE',
    'ROWS', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'ROW', 'WITH', 'RECURSIVE', 'VALUES',
    'CREATE', 'TABLE', 'VIEW', 'REPLACE', 'INSERT', 'DELETE', 'INTO', 'CONSTRAINT', 'DESCRIBE',
    'GRANT', 'PRIVILEGES', 'PUBLIC', 'OPTION', 'EXPLAIN', 'ANALYZE', 'FORMAT', 'TYPE', 'TEXT',
    'GRAPHVIZ', 'LOGICAL', 'DISTRIBUTED', 'TRY', 'CAST', 'TRY_CAST', 'SHOW', 'TABLES', 'SCHEMAS',
    'CATALOGS', 'COLUMNS', 'COLUMN', 'USE', 'PARTITIONS', 'FUNCTIONS', 'DROP', 'UNION', 'EXCEPT',
    'INTERSECT', 'TO', 'SYSTEM', 'BERNOULLI', 'POISSONIZED', 'TABLESAMPLE', 'RESCALED', 'STRATIFY',
    'ALTER', 'RENAME', 'UNNEST', 'ORDINALITY', 'ARRAY', 'MAP', 'SET', 'RESET', 'SESSION', 'DATA',
    'START', 'TRANSACTION', 'COMMIT', 'ROLLBACK', 'WORK', 'ISOLATION', 'LEVEL', 'SERIALIZABLE',
    'REPEATABLE', 'COMMITTED', 'UNCOMMITTED', 'READ', 'WRITE', 'ONLY', 'CALL', 'NORMALIZE', 'NFD',
    'NFC', 'NFKD', 'NFKC', 'IF', 'NULLIF', 'COALESCE', 'OFFSET', 'UNIQUE',
    'INT', 'FLOAT', 'VARCHAR', 'ICONST', 'CHAR', 'PRIMARY', 'KEY', 'UNSIGNED', 'AUTO_INCREMENT', 'COMMENT', 'DEFAULT',
    'BIGINT', 'BLOCK_SIZE', 'DATETIME', 'TINYINT', 'UPDATE', 'TIMESTAMP', 'ENGINE', 'CHARSET', 'COLLATE', 'COMPRESSION',
    'REPLICA_NUM', 'USE_BLOOM_FILTER', 'TABLET_SIZE', 'PCTFREE', 'NOWAIT', 'WAIT', 'IGNORE', 'REGEXP', 'LOCK', 'SHARE',
    'MODE', 'FORCE', 'INDEX', 'DECIMAL', 'CHARACTER'

)

presto_nonreserved = (
    'SHOW', 'TABLES', 'COLUMNS', 'COLUMN', 'PARTITIONS', 'FUNCTIONS', 'SCHEMAS', 'CATALOGS',
    'SESSION', 'ADD', 'OVER', 'PARTITION', 'RANGE', 'ROWS', 'PRECEDING', 'FOLLOWING', 'CURRENT',
    'ROW', 'MAP', 'ARRAY', 'DATE', 'TIME', 'INTERVAL', 'ZONE', 'YEAR', 'MONTH', 'DAY',
    'HOUR', 'MINUTE', 'SECOND', 'EXPLAIN', 'ANALYZE', 'FORMAT', 'TYPE', 'TEXT', 'GRAPHVIZ',
    'LOGICAL', 'DISTRIBUTED', 'TABLESAMPLE', 'SYSTEM', 'BERNOULLI', 'POISSONIZED', 'USE', 'TO',
    'RESCALED', 'APPROXIMATE', 'AT', 'CONFIDENCE', 'RESET', 'VIEW', 'REPLACE', 'IF',
    'NULLIF', 'COALESCE', 'TRY', 'POSITION', 'NO', 'DATA', 'START', 'TRANSACTION',
    'ROLLBACK', 'WORK', 'ISOLATION', 'LEVEL', 'SERIALIZABLE', 'REPEATABLE', 'COMMITTED',
    'UNCOMMITTED', 'READ', 'WRITE', 'ONLY', 'CALL', 'GRANT', 'PRIVILEGES', 'PUBLIC', 'OPTION',
    'SUBSTRING'
)

presto_reserved = tuple([i for i in presto_tokens if i not in presto_nonreserved])
