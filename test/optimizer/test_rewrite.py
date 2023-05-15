import json
import unittest

from metadata.metadata_utils import MetaDataUtils
from optimizer.formatter import format_sql
from optimizer.rewrite_rule import *
from parser.mysql_parser import parser


class MyTestCase(unittest.TestCase):

    def test_or(self):
        statement = parser.parse("SELECT * FROM T1 WHERE C1 < 20000 OR C2 < 30")
        RewriteMySQLORRule().match_action(statement)
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

    def test_supplement_column_rewrite(self):
        statement = parser.parse("SELECT * FROM sqless_base")
        catalog_json = """
        {"columns": [{"schema":"sqless_test","table":"sqless_base",
  "name":"a","type":"int(2)","nullable":false},{"schema":"sqless_test","table":"sqless_base",
  "name":"b","type":"int(2)","nullable":false}], "indexes": [{"schema":"sqless_test","table":"sqless_base",
  "name":"PRIMARY","column":"c",
  "cardinality":1,"unique":true}], "tables":[{"schema":"sqless_test","table":"sqless_base","rows":1,"engine":"InnoDB"}],
  "version": "5.7.36"}
        """
        catalog_object = MetaDataUtils.json_to_catalog(json.loads(catalog_json))
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  a
, b
FROM
  sqless_base
"""

    def test_supplement_column_rewrite_rule_match(self):
        statement = parser.parse("SELECT * FROM sqless_base")
        catalog_json = """
        {"columns": [{"schema":"sqless_test","table":"sqless_base",
  "name":"a","type":"int(2)","nullable":false},{"schema":"sqless_test","table":"sqless_base",
  "name":"b","type":"int(2)","nullable":false}], "indexes": [{"schema":"sqless_test","table":"sqless_base",
  "name":"PRIMARY","column":"c",
  "cardinality":1,"unique":true}], "tables":[{"schema":"sqless_test","table":"sqless_base","rows":1,"engine":"InnoDB"}],
  "version": "5.7.36"}
        """
        catalog_object = MetaDataUtils.json_to_catalog(json.loads(catalog_json))
        match = RewriteSupplementColumnRule().match(statement, catalog_object)
        assert match is True
        statement = parser.parse("SELECT a FROM sqless_base")
        match = RewriteSupplementColumnRule().match(statement, catalog_object)
        assert match is False

    def test_completion_column(self):
        catalog_json = """
        {
            "columns": 
                [
                    {"schema":"sqless_test","table":"d1","name":"a","type":"int(2)","nullable":false},
                    {"schema":"sqless_test","table":"d1","name":"c","type":"int(2)","nullable":false},
                    {"schema":"sqless_test","table":"d2","name":"b","type":"int(2)","nullable":false}
                ], 
            "indexes": 
                [
                    {"schema":"sqless_test","table":"d1","name":"PRIMARY","column":"a","cardinality":1,"unique":true}
                ], 
            "tables":
                [
                    {"schema":"sqless_test","table":"d1","rows":1,"engine":"InnoDB"},
                    {"schema":"sqless_test","table":"d2","rows":1,"engine":"InnoDB"}
                ],
            "version": "5.7.36"
        }
        """
        catalog_object = MetaDataUtils.json_to_catalog(json.loads(catalog_json))
        statement = parser.parse("SELECT * FROM d1")
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  a
, c
FROM
  d1
"""
        statement = parser.parse("SELECT a.* FROM d1 a")
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  a.a
, a.c
FROM
  d1 a
"""
        statement = parser.parse("SELECT a.* FROM d1 as a")
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  a.a
, a.c
FROM
  d1 AS a
"""
        statement = parser.parse("SELECT c.* , d2.b FROM a.d1 c,d2")
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  c.a
, c.c
, d2.b
FROM
  a.d1 c
, d2
"""

    def test_or_same_column(self):
        after_sql_rewrite_format = """SELECT *
FROM
  T1
WHERE C1 IN (20000, 30)
"""

        statement = parser.parse("SELECT * FROM T1 WHERE C1 = 20000 OR C1 = 30")
        RewriteMySQLORRule().match_action(statement)
        result = format_sql(statement, 0)
        assert result == after_sql_rewrite_format

        statement = parser.parse("SELECT * FROM T1 WHERE C1 in (20000) OR C1 = 30")
        RewriteMySQLORRule().match_action(statement)
        result = format_sql(statement, 0)
        assert result == after_sql_rewrite_format

        statement = parser.parse("SELECT * FROM T1 WHERE C1 in (20000) OR C1 in (30)")
        RewriteMySQLORRule().match_action(statement)
        result = format_sql(statement, 0)
        assert result == after_sql_rewrite_format

        statement = parser.parse("SELECT * FROM T1 WHERE C1 in (20000) OR C2 in (30)")
        RewriteMySQLORRule().match_action(statement)
        result = format_sql(statement, 0)
        assert result == """SELECT *
FROM
  T1
WHERE C1 IN (20000)
UNION SELECT *
FROM
  T1
WHERE C2 IN (30)
"""

    def test_like(self):
        statement = parser.parse("select * from sqless_base where d like 'a%'")
        catalog_json = """
                {   
                    "columns": 
                        [
                            {"schema":"sqless_test","table":"sqless_base","name":"c","type":"int(2)","nullable":false},
                            {"schema":"sqless_test","table":"sqless_base","name":"d","type":"int(2)","nullable":false}
                        ], 
                    "indexes": 
                        [
                            {"schema":"sqless_test","table":"sqless_base","name":"PRIMARY","column":"c","cardinality":1,"unique":true},
                            {"schema":"sqless_test","table":"sqless_base","name":"test","column":"d","cardinality":1,"unique":true}
                        ], 
                    "tables":
                        [
                            {"schema":"sqless_test","table":"sqless_base","rows":1,"engine":"InnoDB"}
                        ],
                    "version": "5.7.36"}
                """
        RewriteSupplementColumnRule().match_action(statement, MetaDataUtils.json_to_catalog(json.loads(catalog_json)))
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  c
, d
FROM
  sqless_base
WHERE d LIKE \'a%\'
"""

    def test_qm(self):
        statement = parser.parse("""select * FROM cm_relation    WHERE status = ?               
        AND primary_id = ?                     AND rel_type = ?                AND rel_biz_type = ?""")
        catalog_json = """
        {"columns": [{"schema":"luli1","table":"cm_relation",
"name":"db_id","type":"bigint(20)","nullable":false,"collation":""},{"schema":"luli1","table":"cm_relation",
"name":"tnt_inst_id","type":"varchar(8)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"primary_id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"slave_id","type":"varchar(32)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"rel_type","type":"varchar(6)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"rel_biz_type","type":"varchar(6)","nullable":false,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"ext_info","type":"varchar(512)","nullable":true,"collation":"utf8_general_ci"},{"schema":"luli1","table":"cm_relation",
"name":"gmt_create","type":"datetime","nullable":false,"collation":""},{"schema":"luli1","table":"cm_relation",
"name":"gmt_modified","type":"datetime(3)","nullable":false,"collation":""},{"schema":"luli1","table":"cm_relation",
"name":"status","type":"varchar(1)","nullable":false,"collation":"utf8_general_ci"}], "indexes": [{"schema":"luli1","table":"cm_relation",
"name":"PRIMARY","column":"db_id","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"uk_id","column":"id","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"uk_psrr","column":"primary_id","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"uk_psrr","column":"slave_id","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"uk_psrr","column":"rel_type","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"uk_psrr","column":"rel_biz_type","index_type":"btree",
"cardinality":0,"unique":true},{"schema":"luli1","table":"cm_relation",
"name":"idx_primary","column":"primary_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_slave","column":"slave_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_status","column":"status","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_status","column":"primary_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_status","column":"slave_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_prr","column":"primary_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_prr","column":"rel_type","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_prr","column":"rel_biz_type","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_srsi","column":"slave_id","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_srsi","column":"rel_type","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_srsi","column":"status","index_type":"btree",
"cardinality":0,"unique":false},{"schema":"luli1","table":"cm_relation",
"name":"idx_srsi","column":"id","index_type":"btree",
"cardinality":0,"unique":false}], "tables":[{"schema":"luli1","table":"cm_relation","rows":10000, "type":"BASE TABLE","engine":"InnoDB","collation":"utf8_general_ci"}],
"server_name": "dbadmin.eu95", "version": "5.7.36"}
        """
        catalog_object = MetaDataUtils.json_to_catalog(json.loads(catalog_json))
        RewriteSupplementColumnRule().match_action(statement, catalog_object)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """SELECT
  db_id
, tnt_inst_id
, id
, primary_id
, slave_id
, rel_type
, rel_biz_type
, ext_info
, gmt_create
, gmt_modified
, status
FROM
  cm_relation
WHERE status = ? AND primary_id = ? AND rel_type = ? AND rel_biz_type = ?
"""

    def test_delete_update_order(self):
        statement = parser.parse("""delete from tbl where col1 = ? order by col""")
        match = RemoveOrderByInDeleteUpdateRule().match(statement, None)
        assert match
        RemoveOrderByInDeleteUpdateRule().match_action(statement, None)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """DELETE FROM tbl WHERE col1 = ?
"""
        statement = parser.parse("""delete from tbl where col1 = ? order by col limit 1""")
        match = RemoveOrderByInDeleteUpdateRule().match(statement, None)
        assert not match

        statement = parser.parse("""update tbl set col1 = ? where col2 = ? order by col""")
        match = RemoveOrderByInDeleteUpdateRule().match(statement, None)
        assert match
        RemoveOrderByInDeleteUpdateRule().match_action(statement, None)
        after_sql_rewrite_format = format_sql(statement, 0)
        assert after_sql_rewrite_format == """UPDATE tbl SET col1 = ? WHERE col2 = ?
"""
        statement = parser.parse("""update tbl set col1 = ? where col2 = ? order by col limit 1""")
        match = RemoveOrderByInDeleteUpdateRule().match(statement, None)
        assert not match

    def test_subquery_or(self):
        statement = parser.parse("SELECT t1.* FROM t1 WHERE  t1.c1 IN (?) AND t1.c2 = ? AND t1.c3 > ? and c4 not in (select t2.c5 from t2 where t2.c5 = ? or t2.c5 = ?)")
        is_match = RewriteMySQLORRule().match(statement)
        assert not is_match


if __name__ == '__main__':
    unittest.main()
