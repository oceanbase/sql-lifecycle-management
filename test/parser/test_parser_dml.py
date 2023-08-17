# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from src.parser.tree.with_stmt import WithHasQuery

from src.common.utils import Utils
from src.parser.mysql_parser.parser import parser as mysql_parser
from src.parser.mysql_parser.lexer import lexer as mysql_lexer
from src.parser.oceanbase_parser.parser import parser as oceanbase_parser
from src.parser.oceanbase_parser.lexer import lexer as oceanbase_lexer
from src.parser.tree.expression import ComparisonExpression
from src.parser.tree.statement import Statement
import warnings


class MyTestCase(unittest.TestCase):
    def test_and_in_update(self):
        sql = """
        update foo set t1 = '1' and t2 = '2' where t3 = '3'
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_update_1(self):
        result = oceanbase_parser.parse(
            """
        update jss_alarm_def     
        set                
        scope_id = 5,         
        alarm_name = 'sparkmeta-jss同步刷新任务告警',         
        create_operator = '0005292026',
        is_delete = 0,
        gmt_modify = '2019-08-13 17:11:56.979'
        where alarm_id = 2000003
        """
        )
        assert isinstance(result, Statement)

    def test_update_set(self):
        result = oceanbase_parser.parse(
            """
        update t set a = 1, b = 2 where c = 3
        """
        )
        assert isinstance(result.set_list, list)
        assert isinstance(result.table, list)
        assert isinstance(result.where, ComparisonExpression)

    def test_insert(self):
        sql = """
              insert into t1 values(?,?,?)
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

        sql = """
              insert into t1(c1,c2,c3) values(?,?,?)
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

        sql = """
              insert into t1 select * from t2
              """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_lock_in_share_mode(self):
        sql = """
        INSERT IGNORE INTO ilimitcenter05.tp_48246_ogt_fc_lc_day (`id`, `tnt_inst_id`, `principal_id`, `principal_type`, `cumulate_code` , `stat_time`, `amount`, `day_count`, `reverse_amount`, `reverse_count` , `max_value`, `min_value`, `cumulate_properties`, `p1`, `p2` , `p3`, `p4`, `p5`, `p6`, `p7` , `p8`, `p9`, `p10`, `p11`, `p12` , `p13`, `p14`, `p15`, `properties_md5`, `gmt_create` , `gmt_modified`, `currency`, `version`) SELECT `id`, `tnt_inst_id`, `principal_id`, `principal_type`, `cumulate_code` , `stat_time`, `amount`, `day_count`, `reverse_amount`, `reverse_count` , `max_value`, `min_value`, `cumulate_properties`, `p1`, `p2` , `p3`, `p4`, `p5`, `p6`, `p7` , `p8`, `p9`, `p10`, `p11`, `p12` , `p13`, `p14`, `p15`, `properties_md5`, `gmt_create` , `gmt_modified`, `currency`, `version` FROM ilimitcenter05.fc_lc_day FORCE INDEX (`PRIMARY`) WHERE `id` > ? AND (`id` < ? OR `id` = ?) LOCK IN SHARE MODE
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
        assert isinstance(result, Statement)

    def test_insert_now(self):
        sql = """
INSERT IGNORE INTO bumonitor_risk_process_context (gmt_create, gmt_modified, rowkey, context) VALUES (now(), now(), ?, ?)
        """
        sql = Utils.remove_sql_text_affects_parser(sql)
        result = oceanbase_parser.parse(sql)
        assert isinstance(result, Statement)

    def test_delete_1(self):
        result = oceanbase_parser.parse(
            """
delete from execution_log          where                (                             record_id = 2000006         and           sub_job_id = -3                                             )
        """
        )
        assert isinstance(result, Statement)

    def test_mysql_logical_opt(self):
        test_sqls = [
            """SELECT engine FROM move_title WHERE a XOR '29'""",
            """SELECT id FROM move_title WHERE id > '29' && name = 'test'""",
            """SELECT id FROM move_title WHERE id > '29' AND name = 'test'""",
            """SELECT id FROM move_title WHERE id > '29' OR name = 'test'""",
            """SELECT id FROM move_title WHERE id > ('1' | '2')""",
            """SELECT id FROM move_title WHERE id > ('1' & '2')""",
            """SELECT id FROM move_title WHERE id > ('1' ^ '2')""",
            """SELECT id FROM move_title WHERE id > ('1' << '2')""",
            """SELECT id FROM move_title WHERE id > ('1' >> '2')""",
        ]
        for sql in test_sqls:
            sql = Utils.remove_sql_text_affects_parser(sql)
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)

    def test_mysql_regexp_opt(self):
        test_sqls = [
            """SELECT * FROM t WHERE a RLIKE 'hello|world'""",
            """SELECT * FROM t WHERE a REGEXP 'hello|world'""",
        ]
        for sql in test_sqls:
            sql = Utils.remove_sql_text_affects_parser(sql)
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)

    def test_resvered_word_can_used_as_token_in_mysql_ob(self):
        test_sqls = [
            """SELECT cast FROM t""",
            """SELECT end FROM t""",
            """SELECT escape FROM t""",
            """SELECT id FROM t""",
            """SELECT into FROM t""",
            """SELECT is FROM t""",
            """SELECT or FROM t""",
            """SELECT with FROM t""",
            """SELECT engine FROM t""",
            """SELECT position FROM t""",
            """SELECT max FROM t""",
            """SELECT now FROM t""",
            """SELECT top FROM t""",
            """SELECT round FROM t""",
            """SELECT copy FROM t""",
            """SELECT sign FROM t""",
            """SELECT ceiliing FROM t""",
            """SELECT rows FROM t""",
        ]
        for sql in test_sqls:
            sql = Utils.remove_sql_text_affects_parser(sql)
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_resvered_word_can_used_as_token_only_in_ob(self):
        test_sqls = [
            """SELECT zone_type FROM t""",
            """SELECT groups FROM t""",
        ]
        for sql in test_sqls:
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_vector_expression(self):
        test_sqls = [
            "select * from t where a in ('1','2','3')",
            "select (1,2) > (2,3)",
            "select * from t where ((a) > ('29'))",
            "select * from t where (a,b,c)>('1','2','3')",
            "select * from t where (a,b,c) in ((1,2,3),(1,2,3),(1,2,3))",
        ]
        for sql in test_sqls:
            sql = Utils.remove_sql_text_affects_parser(sql)
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_concat_function(self):
        test_sqls = [
            "SELECT * FROM t WHERE a=CONCAT('a','-','b')",
            "SELECT CONCAT('a','-','b')",
            "SELECT * FROM t WHERE a=CONCAT('a',CONCAT('b','-','c'))",
        ]
        for sql in test_sqls:
            sql = Utils.remove_sql_text_affects_parser(sql)
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_cast_function(self):
        test_sqls = [
            "SELECT CAST(CAST(1+2 AS TIME) AS JSON)",
            "SELECT CAST((8+1) AS SIGNED)",
            "SELECT CAST(9 AS TIME)",
            "SELECT CAST(1 BETWEEN 1 AND 2 AS SIGNED)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_inner_join(self):
        test_sqls = [
            " SELECT * FROM ((SELECT * FROM b INNER JOIN a ON a.task_id = b.task_id) x INNER JOIN y ON x.id = y.id)"
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_set_operation(self):
        test_sqls = [
            " SELECT * FROM a UNION SELECT * FROM b UNION SELECT * FROM c",
            " (SELECT * FROM a UNION SELECT * FROM b) UNION SELECT * FROM c",
            " ((SELECT * FROM a ) UNION SELECT * FROM b) UNION SELECT * FROM c",
            " ((SELECT * FROM a ) UNION (SELECT * FROM b)) UNION SELECT * FROM c",
            " ((SELECT * FROM a ) UNION (SELECT * FROM b)) UNION (SELECT * FROM c)",
            " ((SELECT * FROM a ) UNION (SELECT * FROM b)) UNION (SELECT * FROM c)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_with_operation(self):
        test_sqls = [
            "WITH test1 AS ( SELECT * FROM test2 where a> 10 and b in (1,2,3)) SELECT * FROM test1",
            "WITH test1(a,b,c) AS ( SELECT * FROM test2 where a> 10 and b in (1,2,3)) SELECT * FROM test1",
            "WITH test1(a,b,c) AS ( SELECT * FROM test2),test3(a,b,c) AS ( SELECT * FROM test4) SELECT * FROM test1",
            "WITH test1(a,b,c) AS ( SELECT * FROM test2),test3(a,b,c) AS ( SELECT * FROM test4) SELECT * FROM test1",
            "WITH test1(a,b,c) AS ( SELECT * FROM test2),test3(a,b,c) AS ( SELECT * FROM test4) SELECT * FROM test1 UNION SELECT * FROM test2",
        ]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            for sql in test_sqls:
                result = mysql_parser.parse(sql, lexer=mysql_lexer)
                assert isinstance(result, WithHasQuery)
                result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
                assert isinstance(result, WithHasQuery)

    def test_windows_func(self):
        test_sqls = [
            """
        SELECT 
            first_value(value) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_val,
            lag(value) OVER (PARTITION BY id ORDER BY date) AS lag_val,
            lead(value) OVER (PARTITION BY id ORDER BY date) AS lead_val,
            cume_dist() OVER (PARTITION BY id ORDER BY value) AS cume_dist_val,
            dense_rank() OVER (PARTITION BY id ORDER BY value) AS dense_rank_val,
            last_value(value) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_val,
            nth_value(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_val,
            ntile(4) OVER (PARTITION BY id ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ntile_val,
            percent_rank() OVER (PARTITION BY id ORDER BY value) AS percent_rank_val,
            rank() OVER (PARTITION BY id ORDER BY value) AS rank_val,
            row_number() OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS row_num
        FROM 
            my_table
        """,
            """
        SELECT 
            first_value(value) OVER (PARTITION BY id ORDER BY date RANGE UNBOUNDED PRECEDING) AS first_val,
            lag(value, 2) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lag_val,
            lead(value, 2) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS lead_val,
            cume_dist() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cume_dist_val,
            dense_rank() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dense_rank_val,
            last_value(value) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_val,
            nth_value(value, 2) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_val,
            ntile(4) OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ntile_val,
            percent_rank() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS percent_rank_val,
            rank() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_val,
            row_number() OVER (PARTITION BY id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS row_num
        FROM 
            my_table
        """,
            """
        SELECT 
            first_value(value) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_val,
            lag(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS lag_val,
            lead(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS lead_val,
            cume_dist() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS cume_dist_val,
            dense_rank() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS dense_rank_val,
            last_value(value) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS last_val,
            nth_value(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS nth_val,
            ntile(4) OVER (PARTITION BY id ORDER BY value ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS ntile_val,
            percent_rank() OVER (PARTITION BY id ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS percent_rank_val,
            rank() OVER (PARTITION BY id ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rank_val,
            row_number() OVER (PARTITION BY id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_num
        FROM 
            my_table
        """,
            """
        SELECT 
            first_value(value) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_val,
            lag(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS lag_val,
            lead(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS lead_val,
            cume_dist() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS cume_dist_val,
            dense_rank() OVER (PARTITION BY id ORDER BY value RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS dense_rank_val,
            last_value(value) OVER (PARTITION BY id ORDER BY date RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS last_val,
            nth_value(value, 2) OVER (PARTITION BY id ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS nth_val,
            ntile(4) OVER (PARTITION BY id ORDER BY value ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS ntile_val,
            percent_rank() OVER (PARTITION BY id ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS percent_rank_val,
            rank() OVER (PARTITION BY id ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rank_val,
            row_number() OVER (PARTITION BY id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_num
        FROM 
            my_table
        """,
            """
        SELECT 
            first_value(value) OVER my_window AS first_val,
            lag(value) OVER my_window AS lag_val,
            lead(value) OVER my_window AS lead_val,
            cume_dist() OVER my_window AS cume_dist_val,
            dense_rank() OVER my_window AS dense_rank_val,
            last_value(value) OVER my_window AS last_val,
            nth_value(value, 2) OVER my_window AS nth_val,
            ntile(4) OVER my_window AS ntile_val,
            percent_rank() OVER my_window AS percent_rank_val,
            rank() OVER my_window AS rank_val,
            row_number() OVER my_window AS row_num
        FROM 
            my_table
        WHERE 
            id IN (1, 2)
        WINDOW my_window AS (
            PARTITION BY id 
            ORDER BY date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
        """,
            """
        SELECT 
            first_value(value) OVER my_window_1 AS first_val,
            lag(value, 2) OVER my_window_1 AS lag_val,
            lead(value, 2) OVER my_window_1 AS lead_val,
            cume_dist() OVER my_window_2 AS cume_dist_val,
            dense_rank() OVER my_window_2 AS dense_rank_val,
            last_value(value) OVER my_window_3 AS last_val,
            nth_value(value, 2) OVER my_window_4 AS nth_val,
            ntile(4) OVER my_window_5 AS ntile_val,
            percent_rank() OVER my_window_6 AS percent_rank_val,
            rank() OVER my_window_7 AS rank_val,
            row_number() OVER my_window_8 AS row_num
        FROM 
            my_table
        WHERE 
            id IN (1, 2)
        WINDOW my_window_1 AS (
             PARTITION BY id 
             ORDER BY date 
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND UNBOUNDED FOLLOWING
        ), my_window_2 AS (
             PARTITION BY id 
             ORDER BY value 
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW
        ), my_window_3 AS (
             PARTITION BY id 
             ORDER BY date 
             RANGE BETWEEN INTERVAL 1 DAY FOLLOWING AND UNBOUNDED FOLLOWING
        ), my_window_4 AS (
             PARTITION BY id 
             ORDER BY date 
             RANGE BETWEEN INTERVAL 1 YEAR PRECEDING AND UNBOUNDED FOLLOWING
        ), my_window_5 AS (
             PARTITION BY id 
             ORDER BY value 
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING
        ), my_window_6 AS (
             PARTITION BY id 
             ORDER BY value 
             RANGE BETWEEN INTERVAL 1 WEEK PRECEDING AND INTERVAL 1 WEEK FOLLOWING
        ), my_window_7 AS (
             PARTITION BY id 
             ORDER BY value 
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING
        ), my_window_8 AS (
             PARTITION BY id 
             ORDER BY date 
             RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW
        )
        """,
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_same_name_token(self):
        test_sqls = [
            'SELECT identifier FROM my_table',
            'SELECT digit_identifier FROM my_table',
            'SELECT quoted_identifier FROM my_table',
            'SELECT backquoted_identifier FROM my_table',
            'SELECT period FROM my_table',
            'SELECT comma FROM my_table',
            'SELECT plus FROM my_table',
            'SELECT minus FROM my_table',
            'SELECT lparen FROM my_table',
            'SELECT rparen FROM my_table',
            'SELECT andand FROM my_table',
            'SELECT assignmenteq FROM my_table',
            'SELECT gt FROM my_table',
            'SELECT ge FROM my_table',
            'SELECT lt FROM my_table',
            'SELECT le FROM my_table',
            'SELECT eq FROM my_table',
            'SELECT ne FROM my_table',
            'SELECT bit_or FROM my_table',
            'SELECT bit_and FROM my_table',
            'SELECT bit_xor FROM my_table',
            'SELECT bit_opposite FROM my_table',
            'SELECT excla_mark FROM my_table',
            'SELECT bit_move_left FROM my_table',
            'SELECT bit_move_right FROM my_table',
            'SELECT pipes FROM my_table',
            'SELECT slash FROM my_table',
            'SELECT asterisk FROM my_table',
            'SELECT percent FROM my_table',
            'SELECT non_reserved FROM my_table',
            'SELECT number FROM my_table',
            'SELECT fraction FROM my_table',
            'SELECT qm FROM my_table',
            'SELECT sconst FROM my_table',
            'SELECT row_number FROM my_table',
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_like_escape(self):
        test_sqls = [
            "select * from t where id like '0049663881' escape '`'",
            'select * from t where id like "0049663881" escape "`"',
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer)
            assert isinstance(result, Statement)

    def test_interval(self):
        test_sqls = [
            "select * from t where u > date_add(now(),interval -300 second)",
            "select * from t where u > date_sub(now(),interval 300 day)",
            "select * from t where u > adddate(now(),interval 300 hour)",
            "select * from t where u > subdate(now(),interval 300 minute)",
            "select * from t where u > 50 - interval 300 day_second",
            "select * from t where u > 50 + interval 300 day_hour",
            "select * from t where interval 300 day_minute + 50",
            "select * from t where a>date_sub(now(),?)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_when_case(self):
        test_sqls = [
            "SELECT CASE WHEN grade >= 90 THEN '优秀' WHEN grade >= 80 THEN '良好' WHEN grade >= 60 THEN '及格' ELSE '不及格' END AS result FROM student",
            "SELECT CASE WHEN gender = '男' THEN '先生' WHEN gender = '女' THEN '女士' ELSE '未知' END AS title, name FROM user",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_group_concat(p):
        test_sqls = [
            "SELECT group_concat(name) FROM product",
            "SELECT group_concat(name SEPARATOR ' | ') FROM product WHERE category = '手机'",
            "SELECT category, group_concat(DISTINCT brand ORDER BY brand ASC SEPARATOR ', ') AS brands FROM product GROUP BY category",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_join(p):
        test_sqls = [
            "SELECT o.order_id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.customer_id",
            "SELECT o.order_id, c.name FROM orders o LEFT OUTER JOIN customers c ON o.customer_id = c.customer_id",
            "SELECT o.order_id, c.name FROM orders o RIGHT OUTER JOIN customers c ON o.customer_id = c.customer_id",
            "SELECT o.order_id, c.name FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.customer_id;",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_subquery_compare(p):
        test_sqls = [
            "SELECT o.order_id, o.amount FROM orders o WHERE o.amount > ( SELECT AVG(amount) FROM orders)",
            "SELECT o.order_id, o.amount FROM orders o WHERE o.amount > ANY ( SELECT AVG(amount) FROM orders)",
            "SELECT o.order_id, o.amount FROM orders o WHERE o.amount > ALL ( SELECT AVG(amount) FROM orders)",
            "SELECT o.order_id, o.amount FROM orders o WHERE o.amount > SOME ( SELECT AVG(amount) FROM orders)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_if(p):
        test_sqls = [
            "SELECT name,IF(age>=18, '成年人', '未成年人') AS status FROM users",
            "SELECT name, IF(gender='male', '先生', '女士') AS title FROM users",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_sconst(p):
        test_sqls = [
            r"SELECT * FROM users where a='aaa\a\a\'\''",
            r"SELECT * FROM users where a=''''",
            r"SELECT * FROM users where a='''aaa\a\a'",
            r"SELECT * FROM users where a='''aaa\a\a\\' and b='b'",
            r"SELECT * FROM users where a='''aaa\a\a\\' and b='\'b'",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_alias_name(p):
        test_sqls = [
            'SELECT category AS product_category,COUNT(*) AS "product_count" FROM products',
            "SELECT category AS product_category,COUNT(*) AS 'product_count' FROM products",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_match_against(p):
        test_sqls = [
            "SELECT * FROM products WHERE MATCH(product_name) AGAINST('apple')",
            "SELECT * FROM products WHERE MATCH(product_name) AGAINST('phone' IN BOOLEAN MODE)",
            "SELECT * FROM products WHERE MATCH(product_name, product_description) AGAINST('camera' WITH QUERY EXPANSION)",
            "SELECT * FROM products WHERE MATCH(product_name, product_description) AGAINST('camera' IN NATURAL LANGUAGE MODE)",
            "SELECT * FROM products WHERE MATCH(product_name, product_description) AGAINST('camera' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_assignment(p):
        test_sqls = [
            "SELECT category_name FROM categories WHERE category_id = @category_id",
            "SELECT category_name FROM categories WHERE category_id = (select @rownum := 0)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_binary(p):
        test_sqls = [
            "SELECT * FROM users WHERE BINARY username = 'admin'",
            "SELECT * FROM users WHERE _BINARY 'admin' = 'test'",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_for_update(p):
        test_sqls = [
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE",
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE NOWAIT",
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE WAIT 1",
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE WAIT 1.1",
            "SELECT * FROM orders WHERE order_status = 'new' LOCK IN SHARE MODE",
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE SKIP LOCKED",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_for_update_ob(p):
        test_sqls = [
            "SELECT * FROM orders WHERE order_status = 'new' FOR UPDATE NO_WAIT",
        ]
        for sql in test_sqls:
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_convert(p):
        test_sqls = [
            "SELECT CONVERT(product_price, UNSIGNED) AS price_string FROM products",
            "SELECT CONVERT(product_price, CHAR) AS price_string FROM products",
            "SELECT CONVERT(product_price, BINARY) AS price_string FROM products",
            "SELECT CONVERT(product_price, DECIMAL(10,2)) AS price_string FROM products",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_aggreate_func_with_window(p):
        test_sqls = [
            "SELECT COUNT(order_id) OVER() AS total_orders, AVG(order_amount) OVER() AS average_amount FROM orders",
            "SELECT SUM(order_amount) OVER() AS total_amount FROM orders",
            "SELECT MIN(order_date) OVER(PARTITION BY user_id) AS earliest_order_date FROM orders",
            "SELECT MAX(order_date) OVER(PARTITION BY user_id) AS latest_order_date FROM orders",
            "SELECT GROUP_CONCAT(DISTINCT product_name ORDER BY product_name ASC SEPARATOR ', ') OVER(PARTITION BY user_id) AS product_list FROM orders",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_group_by_with_order(p):
        test_sqls = [
            "SELECT * FROM orders GROUP BY order_year, order_month",
            "SELECT * FROM orders GROUP BY order_year, order_month ASC",
            "SELECT * FROM orders GROUP BY order_year, order_month DESC",
            "SELECT * FROM orders GROUP BY order_year ASC, order_month DESC",
            "SELECT * FROM orders GROUP BY order_year DESC, order_month ASC",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_length(p):
        test_sqls = [
            "SELECT length(title) FROM t",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_date_lit(p):
        test_sqls = [
            "SELECT DATE '2021-05-05'",
            "SELECT TIME '2021-05-05'",
            "SELECT TIMESTAMP '2021-05-05'",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_noeqnull(p):
        test_sqls = [
            "SELECT 1 <=> 2",
            "SELECT 1 <=> 1+1",
            "SELECT 1|1 <=> 1+1*1>2",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_cross_nutural_join(p):
        test_sqls = [
            "SELECT * FROM orders CROSS JOIN customers on orders.name=customers.name",
            "SELECT * FROM products CROSS JOIN orders CROSS JOIN customers on orders.name=customers.name",
            "SELECT * FROM orders NATURAL JOIN customers on orders.name=customers.name",
            "SELECT * FROM products NATURAL JOIN orders CROSS JOIN customers on orders.name=customers.name",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_extract(p):
        test_sqls = [
            "SELECT EXTRACT(YEAR FROM order_date) AS order_year FROM orders",
            "SELECT EXTRACT(QUARTER FROM launch_date) AS launch_quarter FROM products",
            "SELECT EXTRACT(MONTH FROM date_of_birth) AS birth_month FROM customers",
            "SELECT EXTRACT(DAY FROM order_date) AS order_day, EXTRACT(HOUR FROM order_date) AS order_hour FROM orders",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_dual(p):
        test_sqls = [
            "SELECT * FROM DUAL",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_partition(p):
        test_sqls = [
            "SELECT * FROM t PARTITION(p85,p1)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_collate(p):
        test_sqls = [
            "SELECT * FROM t order by a collate utf-8",
            "SELECT a collate utf-8  as c FROM t",
            "SELECT a collate utf-8 FROM t",
            'SELECT k FROM t1 GROUP BY k COLLATE latin1_german2_ci',
            'SELECT MAX(k COLLATE latin1_german2_ci) FROM t1',
            'SELECT DISTINCT k COLLATE latin1_german2_ci FROM t1',
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_number(p):
        test_sqls = [
            "SELECT 0x1F1",
            "SELECT 0X1Ffab1",
            "SELECT 0b111",
            "SELECT 0B101",
            "SELECT x'1F1'",
            "SELECT X'1Ffab1'",
            "SELECT b'111'",
            "SELECT B'101'",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_index_hint(p):
        test_sqls = [
            "SELECT * FROM t1 IGNORE INDEX (i2) USE INDEX (i1) USE INDEX (i2)",
            "SELECT * FROM t1 USE INDEX (i1,i2) IGNORE INDEX (i2)",
            "SELECT * FROM t1 USE INDEX (i1,i2) FORCE INDEX (i2)",
            "SELECT * FROM t USE INDEX (index1) IGNORE INDEX FOR ORDER BY (index1) IGNORE INDEX FOR GROUP BY (index1)",
            "SELECT * FROM t1 USE INDEX FOR JOIN (i1) FORCE INDEX FOR JOIN (i2)",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_string_after_string(p):
        test_sqls = [
            "SELECT * FROM t1 WHERE a='1' '22333' '3333'",
            "SELECT * FROM t1 ORDER BY 'a' DESC",
            "SELECT * FROM t1 ORDER BY 'a' 'desc'",
            "SELECT * FROM t1 GROUP BY 'a' DESC",
            "SELECT * FROM t1 GROUP BY 'a' DESC",
            "SELECT * FROM t1 GROUP BY 'a' 'desc'",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_trim_funcion(p):
        test_sqls = [
            "SELECT TRIM('  bar   ')",
            "SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx')",
            "SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx')",
            "SELECT TRIM(TRAILING 'x' FROM 'xxxbarxxx')",
            "SELECT TRIM('x' FROM 'xxxbarxxx')",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_coalesce_function(p):
        test_sqls = [
            "SELECT COALESCE(NULL)",
            "SELECT COALESCE(NULL,'a')",
            "SELECT COALESCE(NULL,'a','b')",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_logical_operator(p):
        test_sqls = [
            "SELECT * FROM products WHERE price BETWEEN 100 AND 200 AND quantity > 10",
            "SELECT * FROM products WHERE category IN ('Electronics', 'Appliances') OR brand IN ('Apple', 'Samsung')",
            "SELECT * FROM products WHERE NOT price BETWEEN 100 AND 200",
            "SELECT * FROM products WHERE NOT price BETWEEN 100 AND 200 || brand IN ('Apple', 'Samsung') XOR name IN ('Phone')",
            "SELECT * FROM products WHERE NOT price BETWEEN 100 AND 200 OR brand IN ('Apple', 'Samsung')",
            "SELECT * FROM products WHERE NOT price BETWEEN 100 AND 200 && brand IN ('Apple', 'Samsung')",
        ]
        for sql in test_sqls:
            result = mysql_parser.parse(sql, lexer=mysql_lexer, debug=True)
            assert isinstance(result, Statement)
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)

    def test_ob_function(p):
        test_sqls = [
            "SELECT ALL(a) FROM t",
            "SELECT ALL a FROM t",
            "SELECT UNIQUE(a) FROM t",
            "SELECT UNIQUE a FROM t",
            "SELECT HOST_IP()",
        ]
        for sql in test_sqls:
            result = oceanbase_parser.parse(sql, lexer=oceanbase_lexer, debug=True)
            assert isinstance(result, Statement)


if __name__ == "__main__":
    unittest.main()
