import unittest

from optimizer.formatter import format_sql
from parser.mysql_parser import parser


class MyTestCase(unittest.TestCase):

    def test_union_all(self):
        statement = parser.parse("""
                SELECT * FROM T1 WHERE C1 < 20000 UNION ALL
                SELECT * FROM T1 WHERE C2 < 30 AND LNNVL (C1 < 20000)
                """)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT *
FROM
  T1
WHERE C1 < 20000
UNION ALL SELECT *
FROM
  T1
WHERE C2 < 30 AND LNNVL(C1 < 20000)
"""

    def test_union(self):
        statement = parser.parse("""
                    SELECT * FROM T1 WHERE C1 < 20000 UNION
                    SELECT * FROM T1 WHERE C2 < 30
                    """)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT *
FROM
  T1
WHERE C1 < 20000
UNION SELECT *
FROM
  T1
WHERE C2 < 30
"""

    def test_as(self):
        statement = parser.parse("""SELECT a.* FROM d1 as a""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT a.*
FROM
  d1 AS a
"""

    def test_update(self):
        statement = parser.parse("""update t set a = 1,b = 2 where c= 3""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """UPDATE t SET a = 1 , b = 2 WHERE c = 3
"""
        statement = parser.parse("""update t set a = 1,b = 2 where c= 3 order by c limit 1""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """UPDATE t SET a = 1 , b = 2 WHERE c = 3
ORDER BY c ASC
LIMIT 1
"""

    def test_delete(self):
        statement = parser.parse("""delete from t where c= 3 and a = 1""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """DELETE FROM t WHERE c = 3 AND a = 1
"""

        statement = parser.parse("""delete from t where c= 3 and a = 1 order by c limit 1""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """DELETE FROM t WHERE c = 3 AND a = 1
ORDER BY c ASC
LIMIT 1
"""

    def test_sql_1(self):
        statement = parser.parse("""select tnt_inst_id as tnt_inst_id,gmt_create as gmt_create,gmt_modified as gmt_modified,principal_id as principal_id,version as version from cu_version_control where (principal_id = 'TOKENREL|100100000003358587777|IPAY_HK'  )""")
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  tnt_inst_id AS tnt_inst_id
, gmt_create AS gmt_create
, gmt_modified AS gmt_modified
, principal_id AS principal_id
, version AS version
FROM
  cu_version_control
WHERE principal_id = \'TOKENREL|100100000003358587777|IPAY_HK\'
"""

    def test_subquery_limit(self):
        statement = parser.parse("""
                SELECT COUNT(*) FROM ( SELECT * FROM customs_script_match_history LIMIT ? ) a
        """)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT COUNT(*)
FROM
  (SELECT *
FROM
  customs_script_match_history
LIMIT ?
) a
"""


if __name__ == '__main__':
    unittest.main()
