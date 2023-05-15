import unittest

from optimizer.pmd_rule import *
from parser.mysql_parser import parser


class MyTestCase(unittest.TestCase):

    def test_pmd_select_all_rule_match(self):
        statement = parser.parse("SELECT * FROM T1 WHERE C1 < 20000 OR C2 < 30")
        pmd_result = PMDSelectAllRule().match(statement)
        assert pmd_result is True
        statement = parser.parse("SELECT a FROM T1 WHERE C1 < 20000 OR C2 < 30")
        pmd_result = PMDSelectAllRule().match(statement)
        assert pmd_result is False
        statement = parser.parse("SELECT a.* FROM T1 a WHERE C1 < 20000 OR C2 < 30")
        pmd_result = PMDSelectAllRule().match(statement)
        assert pmd_result is True
        statement = parser.parse("SELECT a.* , a.b FROM T1 a WHERE C1 < 20000 OR C2 < 30")
        pmd_result = PMDSelectAllRule().match(statement)
        assert pmd_result is True

    def test_pmd_select_all_rule(self):
        statement = parser.parse("SELECT * FROM T1 WHERE C1 < 20000 OR C2 < 30")
        pmd_result = PMDSelectAllRule().match_action(statement)
        assert pmd_result is not None

    def test_pmd_full_scan_rule(self):
        statement = parser.parse("select 1 from a")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b != 1")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b <> 1")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b not like '1%' ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b not in (1) ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where not exists (select 1 from a) ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where not exists (select 1 from a where c = 2) ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b like '%a' ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("select 1 from a where b like '%a%' ")
        match = PMDFullScanRule().match(statement)
        assert match

        statement = parser.parse("""SELECT * FROM product LEFT JOIN product_details
ON (product.id = product_details.id)
AND   product.amount=200
""")
        match = PMDFullScanRule().match(statement)
        assert not match

        statement = parser.parse("""select 1 from a where b like '%a%' and c BETWEEN 1 AND 20""")
        match = PMDFullScanRule().match(statement)
        assert not match

    def test_update_delete(self):
        statement = parser.parse("""update
  sqless_base set nick=1231
where
  a = 1""")
        match = PMDFullScanRule().match(statement)
        assert not match
        statement = parser.parse("""delete from 
          sqless_base
        where
          a = 1""")
        match = PMDFullScanRule().match(statement)
        assert not match

    def test_is_null(self):
        statement = parser.parse("select * from sqless_base where a is null")
        match = PMDIsNullRule().match(statement)
        assert not match
        statement = parser.parse("select * from sqless_base where a = null")
        match = PMDIsNullRule().match(statement)
        assert match

    def test_count(self):
        statement = parser.parse("select count(a) from sqless_base")
        match = PMDCountRule().match(statement)
        assert match
        statement = parser.parse("select count(1) from sqless_base")
        match = PMDCountRule().match(statement)
        assert match
        statement = parser.parse("select count(DISTINCT a) from sqless_base")
        match = PMDCountRule().match(statement)
        assert match
        statement = parser.parse("select count(*) from sqless_base")
        match = PMDCountRule().match(statement)
        assert not match

    def test_arithmetic_binary(self):
        statement = parser.parse("select count(a) from sqless_base where a * 2 > 1")
        match = PMDArithmeticRule().match(statement)
        assert match
        statement = parser.parse("select count(1) from sqless_base where a  > 1 * 2")
        match = PMDArithmeticRule().match(statement)
        assert not match

    def test_update_delete_multi_table(self):
        statement = parser.parse("""DELETE 
  FROM Product P
  LEFT JOIN OrderItem I ON P.Id = I.ProductId
 WHERE I.Id IS NULL""")
        match = PMDUpdateDeleteMultiTableRule().match(statement)
        assert match

        statement = parser.parse("""UPDATE orders o
INNER JOIN order_details od
  ON o.order_id = od.order_id
SET o.total_orders = 7
    ,item= 'pendrive'
WHERE o.order_id = 1
  AND order_detail_id = 1""")
        match = PMDUpdateDeleteMultiTableRule().match(statement)
        assert match

    def test_nowait_or_wait(self):
        statement = parser.parse("""
        SELECT * FROM match_record_id  FOR UPDATE
        """)
        match = PMDNowaitWaitRule().match(statement)
        assert match
        statement = parser.parse("""
        SELECT * FROM match_record_id  FOR UPDATE NOWAIT
        """)
        match = PMDNowaitWaitRule().match(statement)
        assert not match
        statement = parser.parse("""
        SELECT * FROM match_record_id  FOR UPDATE WAIT 1
        """)
        match = PMDNowaitWaitRule().match(statement)
        assert not match

    def test_multi_table(self):
        statement = parser.parse("""
        SELECT p.product_name, c.category_name, s.supplier_name, o.order_date
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN suppliers s ON p.supplier_id = s.supplier_id
JOIN orders o ON p.product_id = o.product_id
WHERE o.order_date BETWEEN '2022-01-01' AND '2022-12-31'
ORDER BY o.order_date DESC""")
        match = PMDMultiTableRule().match(statement)
        assert match

        statement = parser.parse("""SELECT o.order_date, c.customer_name, p.product_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_date BETWEEN '2022-01-01' AND '2022-12-31'
ORDER BY o.order_date DESC
""")
        match = PMDMultiTableRule().match(statement)
        assert not match


if __name__ == '__main__':
    unittest.main()
