# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from __future__ import print_function

import types

from ply import yacc
from src.optimizer.optimizer_enum import IndexType
from src.parser.oceanbase_parser.lexer import tokens
from src.parser.tree.expression import (
    ArithmeticBinaryExpression,
    ArithmeticUnaryExpression,
    AssignmentExpression,
    BetweenPredicate,
    Cast,
    ComparisonExpression,
    CurrentTime,
    ExistsPredicate,
    FunctionCall,
    InListExpression,
    InPredicate,
    IsNullPredicate,
    LikePredicate,
    ListExpression,
    LogicalBinaryExpression,
    NotExpression,
    QualifiedNameReference,
    RegexpPredicate,
    SimpleCaseExpression,
    SubqueryExpression,
    WhenClause,
)
from src.parser.tree.grouping import SimpleGroupBy
from src.parser.tree.join_criteria import JoinOn, JoinUsing, NaturalJoin
from src.parser.tree.literal import (
    BooleanLiteral,
    DoubleLiteral,
    LongLiteral,
    NullLiteral,
    StringLiteral,
)
from src.parser.tree.node import Node
from src.parser.tree.qualified_name import QualifiedName
from src.parser.tree.query_specification import QuerySpecification
from src.parser.tree.relation import AliasedRelation, Join
from src.parser.tree.select import Select
from src.parser.tree.select_item import SingleColumn
from src.parser.tree.set_operation import Except, Intersect, Union
from src.parser.tree.sort_item import SortItem
from src.parser.tree.statement import Delete, Insert, Query, Update
from src.parser.tree.table import Table
from src.parser.tree.values import Values
from src.parser.tree.field_type import UNSPECIFIEDLENGTH, FieldType, MySQLType

tokens = tokens

precedence = (
    ('right', 'ASSIGNMENTEQ'),
    ('left', 'PIPES', 'OR'),
    ('left', 'XOR'),
    ('left', 'AND', 'ANDAND'),
    ('right', 'NOT'),
    ('left', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE'),
    ('left', 'EQ', 'NE', 'LT', 'LE', 'GT', 'GE', 'IS', 'LIKE', 'RLIKE', 'REGEXP', 'IN'),
    ('left', 'BIT_OR'),
    ('left', 'BIT_AND'),
    ('left', 'BIT_MOVE_LEFT', 'BIT_MOVE_RIGHT'),
    ('left', 'PLUS', 'MINUS'),
    ('left', 'ASTERISK', 'SLASH', 'PERCENT', 'DIV', 'MOD'),
    ('left', 'BIT_XOR'),
    ('left', 'BIT_OPPOSITE'),
    ('right', 'NEG'),
    ('left', 'EXCLA_MARK'),
    ('left', 'LPAREN'),
    ('right', 'RPAREN'),
)


def p_command(p):
    r"""command : ddl
    | dml"""
    p[0] = p[1]


def p_ddl(p):
    r"""ddl : create_table"""
    p[0] = p[1]


def p_dml(p):
    r"""dml : statement"""
    p[0] = p[1]


def p_create_table(p):
    r"""create_table : CREATE TABLE identifier LPAREN column_list RPAREN create_table_end
    | CREATE TABLE identifier LPAREN column_list COMMA primary_clause RPAREN create_table_end
    """
    dict = {}
    dict['type'] = 'create_table'
    dict['table_name'] = p[3]
    dict['element_list'] = p[5]
    if len(p) == 10:
        dict['index_list'] = p[7]
    p[0] = dict


def p_create_table_end(p):
    r"""create_table_end : ENGINE EQ identifier create_table_end
    | DEFAULT CHARSET EQ identifier create_table_end
    | COLLATE EQ identifier create_table_end
    | AUTO_INCREMENT EQ number create_table_end
    | COMMENT EQ SCONST create_table_end
    | COMPRESSION EQ SCONST create_table_end
    | REPLICA_NUM EQ number create_table_end
    | BLOCK_SIZE EQ number create_table_end
    | USE_BLOOM_FILTER EQ FALSE create_table_end
    | TABLET_SIZE EQ number create_table_end
    | PCTFREE EQ number create_table_end
    | empty
    """
    pass


def p_column_list(p):
    r"""
    column_list : column
                | column_list COMMA column
    """
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[3])


def p_column(p):
    r"""
    column :  identifier column_type
            | identifier column_type UNIQUE
    """
    if len(p) == 4:
        p[0] = (p[1], p[2], True)
    else:
        p[0] = (p[1], p[2], False)


def p_column_type(p):
    r"""
    column_type : INT column_end
                | INT LPAREN number RPAREN column_end
                | FLOAT column_end
                | BIGINT column_end
                | BIGINT LPAREN number RPAREN column_end
                | TINYINT LPAREN number RPAREN column_end
                | DATETIME column_end
                | DATETIME LPAREN number RPAREN column_end
                | VARCHAR LPAREN number RPAREN column_end
                | CHAR LPAREN number RPAREN column_end
                | TIMESTAMP column_end
                | DECIMAL LPAREN number COMMA number RPAREN column_end
    """
    p[0] = p[1].lower()


def p_column_end(p):
    r"""
    column_end : collate NOT NULL comment_end
             | collate NOT NULL DEFAULT SCONST comment_end
             | collate DEFAULT NULL comment_end
             | collate NULL DEFAULT NULL comment_end
             | collate UNSIGNED AUTO_INCREMENT comment_end
             | collate NOT NULL AUTO_INCREMENT comment_end
             | collate NOT NULL DEFAULT CURRENT_TIMESTAMP comment_end
             | collate NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment_end
             | CHARACTER SET IDENTIFIER column_end
             | empty
    """


def p_collate(p):
    r"""
    collate : COLLATE identifier
             | empty
    """


def p_comment_end(p):
    r"""
    comment_end : COMMENT SCONST
             | empty
    """


def p_primary_clause(p):
    r"""
    primary_clause : PRIMARY KEY LPAREN index_column_list RPAREN
                   | PRIMARY KEY LPAREN index_column_list RPAREN COMMA index_list
    """
    p[0] = []
    p[0].append((IndexType.PRIMARY, 'PRIMARY', p[4]))

    if len(p) == 8:
        p[0].extend(p[7])


def p_index_list(p):
    r"""
    index_list : index_key identifier LPAREN index_column_list RPAREN index_end
               | index_list COMMA index_key identifier LPAREN index_column_list RPAREN index_end
    """
    p[0] = []
    if len(p) == 7:
        p[0].append((p[1], p[2], p[4]))
    else:
        p[0].extend(p[1])
        p[0].append((p[3], p[4], p[6]))


def p_index_key(p):
    r"""
    index_key : KEY
              | UNIQUE KEY
    """
    if len(p) == 2:
        p[0] = IndexType.NORMAL
    else:
        p[0] = IndexType.UNIQUE


def p_index_column_list(p):
    r"""
    index_column_list : identifier
                      | index_column_list COMMA identifier
    """
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0].extend(p[1])
        p[0].append(p[3])


def p_index_end(p):
    r"""
    index_end : BLOCK_SIZE number
              | empty
    """


def p_statement(p):
    r"""statement : cursor_specification
    | delete
    | update
    | insert"""
    p[0] = p[1]


def p_insert(p):
    r"""insert : INSERT ignore INTO table_reference VALUES LPAREN insert_value RPAREN
    | INSERT ignore INTO table_reference LPAREN index_column_list RPAREN VALUES LPAREN insert_value RPAREN
    | INSERT ignore INTO table_reference LPAREN index_column_list RPAREN query_spec
    | INSERT ignore INTO table_reference query_spec"""
    p[0] = Insert(target=p[4])


def p_insert_value(p):
    r"""insert_value : value
    | insert_value COMMA value"""
    pass


def p_ignore(p):
    r"""
    ignore : IGNORE
           | empty
    """


def p_delete(p):
    r"""delete : DELETE FROM relations where_opt order_by_opt limit_opt"""
    p_limit = p[6]
    offset = 0
    limit = 0
    if p_limit:
        offset = int(p_limit[0])
        limit = int(p_limit[1])
    p[0] = Delete(table=p[3], where=p[4], order_by=p[5], limit=limit, offset=offset)


def p_update(p):
    r"""update : UPDATE relations SET assignment_list where_opt order_by_opt limit_opt"""
    p_limit = p[7]
    offset = 0
    limit = 0
    if p_limit:
        offset = int(p_limit[0])
        limit = int(p_limit[1])
    p[0] = Update(
        table=p[2], set_list=p[4], where=p[5], order_by=p[6], limit=limit, offset=offset
    )


def p_assignment_list(p):
    r"""assignment_list : assignment
    | assignment_list COMMA assignment"""
    _item_list(p)


def p_assignment(p):
    r"""assignment : qualified_name eq_or_assignment_eq expr_or_default"""
    name = QualifiedNameReference(p.lineno(1), p.lexpos(1), name=p[1])
    p[0] = AssignmentExpression(p.lineno(1), p.lexpos(1), p[2], name, p[3])


def p_eq_or_assignment_eq(p):
    r"""eq_or_assignment_eq : EQ
    | ASSIGNMENTEQ"""
    p[0] = p[1]


def p_expr_or_default(p):
    r"""expr_or_default : expression
    | DEFAULT"""
    p[0] = p[1]


# TODO: union limit,offset,order_by
def p_cursor_specification(p):
    r"""cursor_specification : query_expression
    | query_spec"""
    if p.slice[1].type == "query_spec":
        order_by = p[1].order_by
        limit, offset = p[1].limit, p[1].offset
        p[1].order_by = []
        p[0] = Query(
            p.lineno(1),
            p.lexpos(1),
            with_=None,
            query_body=p[1],
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
    else:
        p[0] = p[1]


def p_for_update_opt(p):
    r"""for_update_opt : FOR UPDATE
    | FOR UPDATE NOWAIT
    | FOR UPDATE WAIT number
    | empty"""
    if len(p) == 3:
        p[0] = (True, False)
    elif len(p) < 3:
        p[0] = (False, False)
    else:
        p[0] = (True, True)


def p_query_expression(p):
    r"""query_expression : set_operation_stmt"""
    p[0] = p[1]


def p_set_operation_stmt(p):
    r"""set_operation_stmt : set_operation_stmt_wout_order_limit
    | set_operation_stmt_w_order
    | set_operation_stmt_w_limit
    | set_operation_stmt_w_order_limit
    """
    p[0] = p[1]


def p_set_operation_stmt_w_order_by_limit(p):
    r"""set_operation_stmt_wout_order_limit : set_operation_stmt_subquery
    |  set_operation_stmt_simple_table
    """
    order_by, limit, offset = [], 0, 0
    if p.slice[1].type == "set_operation_stmt_simple_table":
        if type(p[1]) == Union:
            simple_table = p[1].relations[1]
        elif type(p[1]) == Except:
            simple_table = p[1].right
        elif type(p[1]) == Intersect:
            simple_table = p[1].relations[1]
        order_by, limit, offset = (
            simple_table.order_by,
            simple_table.limit,
            simple_table.offset,
        )
        simple_table.order_by = []
    p[0] = Query(
        p.lineno(1),
        p.lexpos(1),
        with_=None,
        query_body=p[1],
        order_by=order_by,
        limit=limit,
        offset=offset,
    )


def p_set_operation_stmt_w_order(p):
    r"""set_operation_stmt_w_order :  set_operation_stmt_subquery order_by
    | subquery order_by
    """
    p[0] = Query(
        p.lineno(1),
        p.lexpos(1),
        with_=None,
        query_body=p[1],
        order_by=p[2],
        limit=0,
        offset=0,
    )


def p_set_operation_stmt_w_limit(p):
    r"""set_operation_stmt_w_limit :  set_operation_stmt_subquery limit_stmt
    | subquery limit_stmt
    """
    offset, limit = 0, 0
    if p[2]:
        offset, limit = p[2][0], p[2][1]
    p[0] = Query(
        p.lineno(1),
        p.lexpos(1),
        with_=None,
        query_body=p[1],
        order_by=[],
        offset=offset,
        limit=limit,
    )


def p_set_operation_stmt_w_order_limit(p):
    r"""set_operation_stmt_w_order_limit :  set_operation_stmt_subquery order_by limit_stmt
    | subquery order_by limit_stmt
    """
    offset, limit = 0, 0
    if p[3]:
        offset, limit = p[3][0], p[3][1]
    p[0] = Query(
        p.lineno(1),
        p.lexpos(1),
        with_=None,
        query_body=p[1],
        order_by=p[2],
        offset=offset,
        limit=limit,
    )


def p_set_operation_stmt_subquery(p):
    r"""set_operation_stmt_subquery : set_operation_expressions set_operation set_quantifier_opt subquery"""
    p[0] = _set_operation(
        p.lineno(1), p.lexpos(1), left=p[1], right=p[4], oper=p[2], distinctOrAll=p[3]
    )


def p_set_operation_stmt_simple_table(p):
    r"""set_operation_stmt_simple_table : set_operation_expressions set_operation set_quantifier_opt simple_table"""
    p[0] = _set_operation(
        p.lineno(1), p.lexpos(1), left=p[1], right=p[4], oper=p[2], distinctOrAll=p[3]
    )


# ORDER BY
def p_order_by_opt(p):
    r"""order_by_opt : order_by
    | empty"""
    p[0] = p[1] if p[1] else []


def p_order_by(p):
    r"""order_by : ORDER BY sort_items"""
    p[0] = p[3]


def p_sort_items(p):
    r"""sort_items : sort_item
    | sort_items COMMA sort_item"""
    _item_list(p)


def p_sort_item(p):
    r"""sort_item : value_expression order_opt null_ordering_opt
    | LPAREN value_expression RPAREN order_opt null_ordering_opt"""
    if len(p) == 4:
        p[0] = SortItem(
            p.lineno(1),
            p.lexpos(1),
            sort_key=p[1],
            ordering=p[2] or 'asc',
            null_ordering=p[3],
        )
    else:
        p[0] = SortItem(
            p.lineno(1),
            p.lexpos(1),
            sort_key=p[2],
            ordering=p[4] or 'asc',
            null_ordering=p[5],
        )


def p_order_opt(p):
    r"""order_opt : ASC
    | DESC
    | empty"""
    p[0] = p[1]


def p_null_ordering_opt(p):
    r"""null_ordering_opt : NULLS FIRST
    | NULLS LAST
    | empty"""
    p[0] = p[2] if p[1] else None


# LIMIT
def p_limit_opt(p):
    r"""limit_opt : limit_stmt
    | empty"""
    p[0] = p[1] if p[1] else None


def p_limit_stmt(p):
    r"""limit_stmt : LIMIT parameterization
    | LIMIT parameterization COMMA parameterization
    | LIMIT parameterization OFFSET parameterization
    | LIMIT ALL"""
    if len(p) < 5:
        p[0] = (0, p[2])
    else:
        if p[3] == ',':
            p[0] = (p[2], p[4])
        else:
            p[0] = (p[4], p[2])


def p_parameterization(p):
    r"""parameterization : number
    | QM
    """
    p[0] = p[1]


def p_number(p):
    r"""number : NUMBER"""
    p[0] = p[1]


def p_set_operation_expressions(p):
    r"""set_operation_expressions : set_operation_expression
    | set_operation_expressions set_operation set_quantifier_opt set_operation_expression
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = _set_operation(
            p.lineno(1),
            p.lexpos(1),
            left=p[1],
            right=p[4],
            oper=p[2],
            distinctOrAll=p[3],
        )


def _set_operation(line, pos, left, right, oper, distinctOrAll):
    distinct = distinctOrAll is not None and distinctOrAll.upper() == "DISTINCT"
    all = distinctOrAll is not None and distinctOrAll.upper() == "ALL"
    oper = oper.upper()
    if oper == "UNION":
        set_operation = Union(
            line, pos, relations=[left, right], distinct=distinct, all=all
        )
    elif oper == "EXCEPT":
        set_operation = Except(
            line, pos, left=left, right=right, distinct=distinct, all=all
        )
    elif oper == "INTERSECT":
        set_operation = Intersect(
            line, pos, relations=[left, right], distinct=distinct, all=all
        )
    return set_operation


def p_set_operation(p):
    r"""set_operation : UNION
    | EXCEPT
    | INTERSECT"""
    p[0] = p[1]


# QUERY TERM
def p_set_operation_expression(p):
    r"""set_operation_expression : simple_table
    | subquery"""
    p[0] = p[1]


def p_subquery(p):
    r"""subquery : LPAREN simple_table RPAREN
    | LPAREN set_operation_stmt RPAREN
    | LPAREN subquery RPAREN"""
    p[0] = SubqueryExpression(p.lineno(1), p.lexpos(1), query=p[2])


def p_simple_table(p):
    r"""simple_table : query_spec
    | explicit_table
    | table_value_constructor"""
    p[0] = p[1]


# TODO:Add order_by_opt and limit_opt to Table and Values
def p_explicit_table(p):
    r"""explicit_table : TABLE qualified_name order_by_opt limit_opt for_update_opt"""
    p[0] = Table(p.lineno(1), p.lexpos(1), name=p[2])


def p_table_value_constructor(p):
    r"""table_value_constructor : VALUES values_list order_by_opt limit_opt for_update_opt"""
    p[0] = Values(p.lineno(1), p.lexpos(1), rows=p[2])


def p_values_list(p):
    r"""values_list : values_list COMMA expression
    | expression"""
    _item_list(p)


def _item_list(p):
    if len(p) == 2:
        p[0] = [p[1]]
    elif isinstance(p[1], list):
        p[1].append(p[3])
        p[0] = p[1]
    else:
        p[0] = None


def p_query_spec(p):
    r"""query_spec : SELECT select_items table_expression_opt order_by_opt limit_opt for_update_opt"""
    select_items = p[2]
    table_expression_opt = p[3]
    from_relations = table_expression_opt.from_ if table_expression_opt else None
    where = table_expression_opt.where if table_expression_opt else None
    group_by = table_expression_opt.group_by if table_expression_opt else None
    having = table_expression_opt.having if table_expression_opt else None
    p_for_update = p[6]
    for_update = None
    nowait_or_wait = None

    p_limit = p[5]
    offset = 0
    limit = 0
    if p_limit:
        offset = p_limit[0]
        limit = p_limit[1]

    if p_for_update:
        for_update = p_for_update[0]
        nowait_or_wait = p_for_update[1]

    # Reduce the implicit join relations
    from_ = None
    if from_relations:
        from_ = from_relations[0]
        for rel in from_relations[1:]:  # Skip first one
            from_ = Join(
                p.lineno(3), p.lexpos(3), join_type="IMPLICIT", left=from_, right=rel
            )

    p[0] = QuerySpecification(
        p.lineno(1),
        p.lexpos(1),
        select=Select(p.lineno(1), p.lexpos(1), select_items=select_items),
        from_=from_,
        where=where,
        group_by=group_by,
        having=having,
        for_update=for_update,
        nowait_or_wait=nowait_or_wait,
        order_by=p[4],
        limit=limit,
        offset=offset,
    )


def p_where_opt(p):
    r"""where_opt : WHERE search_condition
    | empty"""
    if p.slice[1].type == "WHERE":
        p[0] = p[2]
    else:
        p[0] = None


def p_group_by_opt(p):
    r"""group_by_opt : GROUP BY grouping_expressions
    | empty"""
    p[0] = SimpleGroupBy(p.lineno(1), p.lexpos(1), columns=p[3]) if p[1] else None


def p_grouping_expressions(p):
    r"""grouping_expressions : value_expression
    | grouping_expressions COMMA value_expression"""
    _item_list(p)


def p_having_opt(p):
    r"""having_opt : HAVING search_condition
    | empty"""
    p[0] = p[2] if p[1] else None


def p_set_quantifier_opt(p):
    r"""set_quantifier_opt : ALL
    | DISTINCT
    | empty"""
    p[0] = p[1]


def p_select_items(p):
    r"""select_items : select_item
    | select_items COMMA select_item"""
    _item_list(p)


def p_select_item(p):
    r"""select_item : derived_column
    | DISTINCT LPAREN derived_column RPAREN
    | DISTINCT derived_column"""
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 3:
        p[0] = p[2]
    else:
        p[0] = p[3]


def p_derived_column(p):
    r"""derived_column : boolean_factor alias_opt"""
    p[0] = SingleColumn(p.lineno(1), p.lexpos(1), alias=p[2], expression=p[1])


def p_table_expression_opt(p):
    r"""table_expression_opt : FROM relations force_index where_opt group_by_opt having_opt
    | empty"""
    if p[1]:
        p[0] = Node(
            p.lineno(1),
            p.lexpos(1),
            from_=p[2],
            where=p[4],
            group_by=p[6],
            having=p[6],
        )
    else:
        p[0] = p[1]


def p_force_index(p):
    r"""force_index : FORCE INDEX LPAREN identifier RPAREN
    | empty"""
    pass


def p_relations(p):
    r"""relations : relations COMMA table_reference
    | table_reference"""
    _item_list(p)


# query expression
def p_table_reference(p):
    r"""table_reference : table_primary
    | joined_table"""
    p[0] = p[1]


# table reference
def p_table_primary(p):
    r"""table_primary : aliased_relation
    | derived_table
    | LPAREN relations RPAREN"""
    if len(p) == 3:
        p[0] = p[2]
    else:
        p[0] = p[1]


# joined table
def p_joined_table(p):
    r"""joined_table : cross_join
    | qualified_join
    | natural_join"""
    p[0] = p[1]


def p_cross_join(p):
    r"""cross_join : table_reference CROSS JOIN table_primary"""
    p[0] = Join(
        p.lineno(1),
        p.lexpos(1),
        join_type="CROSS",
        left=p[1],
        right=p[4],
        criteria=None,
    )


def p_qualified_join(p):
    r"""qualified_join : table_reference join_type JOIN table_reference join_criteria"""
    right = p[4]
    criteria = p[5]
    join_type = p[2] if p[2] in ("LEFT", "RIGHT", "FULL") else "INNER"
    p[0] = Join(
        p.lineno(1),
        p.lexpos(1),
        join_type=join_type,
        left=p[1],
        right=right,
        criteria=criteria,
    )


def p_natural_join(p):
    r"""natural_join : table_reference NATURAL join_type JOIN table_primary"""
    right = p[5]
    criteria = NaturalJoin()
    join_type = "INNER"
    p[0] = Join(
        p.lineno(1),
        p.lexpos(1),
        join_type=join_type,
        left=p[1],
        right=right,
        criteria=criteria,
    )


def p_join_type(p):
    r"""join_type : INNER
    | LEFT outer_opt
    | RIGHT outer_opt
    | FULL outer_opt
    | empty"""
    p[0] = p[1]


def p_outer_opt(p):
    r"""outer_opt : OUTER
    | empty"""
    # Ignore


def p_join_criteria(p):
    r"""join_criteria : ON search_condition
    | USING LPAREN join_columns RPAREN
    | empty"""
    if p.slice[1].type == "ON":
        p[0] = JoinOn(expression=p[2])
    elif p.slice[1].type == "USING":
        p[0] = JoinUsing(columns=p[3])
    else:
        p[0] = None


def p_identifiers(p):
    r"""join_columns : identifier
    | join_columns COMMA identifier"""
    _item_list(p)


# Potentially Aliased table_reference
def p_aliased_relation(p):
    r"""aliased_relation : qualified_name alias_opt"""
    rel = Table(p.lineno(1), p.lexpos(1), name=p[1])
    if p[2]:
        p[0] = AliasedRelation(p.lineno(1), p.lexpos(1), relation=rel, alias=p[2])
    else:
        p[0] = rel


def p_derived_table(p):
    r"""derived_table : subquery alias_opt"""
    if p[2]:
        p[0] = AliasedRelation(p.lineno(1), p.lexpos(1), relation=p[1], alias=p[2])
    else:
        p[0] = p[1]


def p_alias_opt(p):
    r"""alias_opt : AS identifier
    | identifier
    | empty"""
    if p.slice[1].type == "AS":
        p[0] = (p[1], p[2])
    elif p.slice[1].type == "identifier":
        p[0] = p[1]
    else:
        p[0] = p[1]


def p_expression(p):
    r"""expression : search_condition"""
    p[0] = p[1]


def p_search_condition(p):
    r"""search_condition : boolean_factor
    | search_condition OR search_condition
    | search_condition logical_and search_condition
    | search_condition XOR search_condition
    | NOT search_condition"""
    if len(p) == 2:
        p[0] = p[1]
    elif p.slice[2].type == "OR":
        p[0] = LogicalBinaryExpression(
            p.lineno(1), p.lexpos(1), type="OR", left=p[1], right=p[3]
        )
    elif p.slice[2].type == "logical_and":
        p[0] = LogicalBinaryExpression(
            p.lineno(1), p.lexpos(1), type="AND", left=p[1], right=p[3]
        )
    elif p.slice[2] == "XOR":
        p[0] = LogicalBinaryExpression(
            p.lineno(1), p.lexpos(1), type="XOR", left=p[1], right=p[3]
        )
    elif p.slice[1] == "NOT":
        p[0] = NotExpression(p.lineno(1), p.lexpos(1), value=p[2])


def p_logical_and(p):
    r"""logical_and : AND
    | ANDAND"""
    p[0] = p[1]


def p_boolean_factor(p):
    r"""boolean_factor : boolean_factor comparison_operator predicate
    | predicate"""
    if len(p) == 4:
        p[0] = ComparisonExpression(
            p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
        )
    else:
        p[0] = p[1]


def p_predicate(p):
    r"""predicate : between_predicate
    | in_predicate
    | like_predicate
    | regexp_predicate
    | null_predicate
    | exists_predicate
    | value_expression"""
    p[0] = p[1]


def p_between_predicate(p):
    r"between_predicate : value_expression between_opt predicate AND predicate"
    p[0] = BetweenPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], min=p[3], max=p[5]
    )


def p_in_predicate(p):
    r"""in_predicate : value_expression in_opt in_value"""
    p[0] = InPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], value_list=p[3]
    )


def p_like_predicate(p):
    r"""like_predicate : value_expression like_opt value_expression"""
    p[0] = LikePredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], pattern=p[3]
    )


def p_regexp_predicate(p):
    r"""regexp_predicate : value_expression reg_sym_opt value_expression"""
    p[0] = RegexpPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], pattern=p[3]
    )


def p_null_predicate(p):
    r"""null_predicate : value_expression is_opt NULL"""
    p[0] = IsNullPredicate(p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1])


def p_exists_predicate(p):
    r"""exists_predicate : exists_opt subquery"""
    p[0] = ExistsPredicate(p.lineno(1), p.lexpos(1), is_not=p[1], subquery=p[2])


def p_between_opt(p):
    r"""between_opt : NOT BETWEEN
    | BETWEEN"""
    p[0] = p.slice[1].type == "NOT"


def p_in_opt(p):
    r"""in_opt : NOT IN
    | IN"""
    p[0] = p.slice[1].type == "NOT"


def p_in_value(p):
    r"""in_value : LPAREN call_list RPAREN
    | subquery"""
    if p.slice[1].type == "subquery":
        p[0] = p[1]
    else:
        p[0] = InListExpression(p.lineno(1), p.lexpos(1), values=p[2])


def p_like_opt(p):
    r"""like_opt : NOT LIKE
    | LIKE"""
    p[0] = p.slice[1].type == "NOT"


def p_exists_opt(p):
    r"""exists_opt : NOT EXISTS
    | EXISTS"""
    p[0] = p.slice[1].type == "NOT"


def p_is_opt(p):
    r"""is_opt : IS NOT
    | IS"""
    p[0] = len(p) == 3


def p_reg_sym_opt(p):
    r"""reg_sym_opt : NOT regexp_sym
    | regexp_sym"""
    p[0] = p.slice[1].type == "NOT"


def p_regexp_sym(p):
    r"""regexp_sym : REGEXP
    | RLIKE"""
    pass


def p_value_expression(p):
    r"""value_expression : numeric_value_expression"""
    p[0] = p[1]


def p_numeric_value_expression(p):
    r"""numeric_value_expression : numeric_value_expression arithmetic_opt numeric_value_expression
    | numeric_value_expression bit_opt numeric_value_expression
    | factor"""
    if len(p) == 4:
        if p.slice[2].type == "arithmetic_opt":
            p[0] = ArithmeticBinaryExpression(
                p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
            )
        elif p.slice[2].type == "bit_opt":
            p[0] = LogicalBinaryExpression(
                p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
            )
    else:
        p[0] = p[1]


def p_arithmetic_opt(p):
    r"""arithmetic_opt : PLUS
    | MINUS
    | ASTERISK
    | SLASH
    | DIV
    | MOD
    | PERCENT
    | PIPES"""
    p[0] = p[1]


def p_bit_opt(p):
    r"""bit_opt : BIT_AND
    | BIT_OR
    | BIT_XOR
    | BIT_MOVE_LEFT
    | BIT_MOVE_RIGHT"""
    p[0] = p[1]


# TODO ADD OPPOSITE or
def p_factor(p):
    r"""factor : BIT_OPPOSITE factor
    | MINUS factor %prec NEG
    | PLUS factor %prec NEG
    | EXCLA_MARK factor
    | base_primary_expression"""
    if len(p) == 3:
        p[0] = ArithmeticUnaryExpression(
            p.lineno(1), p.lexpos(1), value=p[2], sign=p[1]
        )
    else:
        p[0] = p[1]


def p_base_primary_expression(p):
    r"""base_primary_expression : value
    | qualified_name
    | subquery %prec NEG
    | function_call
    | date_time
    | LPAREN call_list RPAREN
    | case_specification
    | cast_specification"""
    if p.slice[1].type == "qualified_name":
        p[0] = QualifiedNameReference(p.lineno(1), p.lexpos(1), name=p[1])
    elif len(p) == 4:
        p[0] = ListExpression(p.lineno(1), p.lexpos(1), values=p[2])
    else:
        p[0] = p[1]


def p_value(p):
    r"""value : NULL
    | SCONST
    | figure
    | boolean_value
    | QUOTED_IDENTIFIER
    | QM"""
    if p.slice[1].type == "NULL":
        p[0] = NullLiteral(p.lineno(1), p.lexpos(1))
    elif p.slice[1].type == "SCONST" or p.slice[1].type == "QUOTED_IDENTIFIER":
        p[0] = StringLiteral(p.lineno(1), p.lexpos(1), p[1][1:-1])
    else:
        p[0] = p[1]


def p_function_call(p):
    r"""function_call : qualified_name LPAREN call_args RPAREN
    | qualified_name LPAREN DISTINCT call_args RPAREN
    | CURRENT_DATE LPAREN RPAREN"""
    if len(p) == 5:
        distinct = p[3] is None or (
            isinstance(p[3], str) and p[3].upper() == "DISTINCT"
        )
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=distinct, arguments=p[3]
        )
    elif len(p) == 4:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=[]
        )
    else:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=True, arguments=p[4]
        )


def p_call_args(p):
    r"""call_args : call_list
    | empty_call_args"""
    p[0] = p[1]


def p_empty_call_args(p):
    r"""empty_call_args : ASTERISK
    | empty"""
    p[0] = []


def p_case_specification(p):
    r"""case_specification : simple_case"""
    p[0] = p[1]


def p_simple_case(p):
    r"""simple_case : CASE expression_opt when_clauses else_opt END"""
    p[0] = SimpleCaseExpression(
        p.lineno(1), p.lexpos(1), operand=p[2], when_clauses=p[3], default_value=p[4]
    )


def p_cast_specification(p):
    r"""cast_specification : CAST LPAREN expression AS cast_field RPAREN"""
    p[0] = Cast(p.lineno(1), p.lexpos(1), expression=p[3], data_type=p[5], safe=False)


def p_expression_opt(p):
    r"""expression_opt : expression
    | empty"""
    p[0] = p[1]


def p_when_clauses(p):
    r"""when_clauses : when_clauses when_clause
    | when_clause"""
    if len(p) == 2:
        p[0] = [p[1]]
    elif isinstance(p[1], list):
        p[1].append(p[2])
        p[0] = p[1]
    else:
        p[0] = None


def p_when_clause(p):
    r"""when_clause : WHEN search_condition THEN value_expression"""
    p[0] = WhenClause(p.lineno(1), p.lexpos(1), operand=p[2], result=p[4])


def p_else_opt(p):
    r"""else_opt : ELSE value_expression
    | empty"""
    p[0] = p[2] if p[1] else None


def p_call_list(p):
    r"""call_list : call_list COMMA expression
    | expression"""
    _item_list(p)


def p_cast_field(p):
    r"""cast_field : BINARY field_len_opt
    | char_type field_len_opt field_param_list_opt
    | DATE
    | YEAR
    | DATETIME field_len_opt
    | DECIMAL float_opt
    | TIME field_len_opt
    | SIGNED integer_opt
    | UNSIGNED integer_opt
    | JSON
    | DOUBLE
    | FLOAT float_opt
    | REAL"""
    field = FieldType(p.lineno(1), p.lexpos(1))
    if p.slice[1].type == "BINARY":
        field.set_tp(MySQLType.BINARY, "BINARY")
        field.set_length(p[2])
    elif p.slice[1].type == "char_type":
        field.set_tp(MySQLType.CHAR, p[1])
        field.set_length(p[2])
        if p[3] != None:
            field.set_charset_and_collation(f"({','.join(p[3])})")
    elif p.slice[1].type == "DATE":
        field.set_tp(MySQLType.DATE, "DATE")
    elif p.slice[1].type == "YEAR":
        field.set_tp(MySQLType.YEAR, "YEAR")
    elif p.slice[1].type == 'DATETIME':
        field.set_tp(MySQLType.DATETIME, "DATETIME")
        field.set_length(p[2])
    elif p.slice[1].type == 'DECIMAL':
        field.set_tp(MySQLType.DECIMAL, "DEMCIMAL")
        field.set_length(p[2].length)
        field.set_decimal(p[2].decimal)
    elif p.slice[1].type == "TIME":
        field.set_tp(MySQLType.TIME, "TIME")
        field.set_length(p[2])
    elif p.slice[1].type == "SIGNED":
        field.set_tp(MySQLType.INTEGER, p[2])
    elif p.slice[1].type == "UNSIGNED":
        field.set_tp(MySQLType.INTEGER, p[2])
    elif p.slice[1].type == "JSON":
        field.set_tp(MySQLType.JSON, "JSON")
    elif p.slice[1].type == "DOUBLE":
        field.set_tp(MySQLType.DOUBLE, "DOUBLE")
    elif p.slice[1].type == "FLOAT":
        field.set_tp(MySQLType.FLOAT, "FLOAT")
        field.set_length(p[2].length)
        field.set_decimal(p[2].decimal)
    elif p.slice[1].type == "REAL":
        field.set_tp(MySQLType.REAL, "REAL")


def p_field_len_opt(p):
    r"""field_len_opt : LPAREN NUMBER RPAREN
    | empty"""
    if len(p) == 4:
        p[0] = p[2].value & 0xFFFFFFFF  # convert to unsigned int
    p[0] = UNSPECIFIEDLENGTH


def p_field_param_list_opt(p):
    r"""field_param_list_opt : LPAREN field_param_list RPAREN
    | empty"""
    p[0] = p[2] if p[1] else p[1]


def p_field_param_list(p):
    r"""field_param_list : field_param_list COMMA field_parameter
    | field_parameter"""
    _item_list(p)


def p_field_parameter(p):
    r"""field_parameter : number
    | base_data_type"""
    p[0] = p[1]


def p_float_opt(p):
    r"""float_opt : LPAREN NUMBER RPAREN
    | LPAREN NUMBER COMMA NUMBER RPAREN
    | empty"""
    # First is length,Second is decimal
    if len(p) == 2:
        p[0] = {'length': UNSPECIFIEDLENGTH, 'decimal': UNSPECIFIEDLENGTH}
    elif len(p) == 4:
        p[0] = {'length': p[2], 'decimal': UNSPECIFIEDLENGTH}
    elif len(p) == 6:
        p[0] = {'length': p[2], 'decimal': p[4]}


def p_char_type(p):
    r"""char_type : CHARACTER
    | CHAR"""
    p[0] = p[1]


def p_integer_opt(p):
    r"""integer_opt : INTEGER
    | INT
    | empty"""
    p[0] = p[1]


def p_base_data_type(p):
    r"""base_data_type : identifier"""
    p[0] = p[1]


def p_date_time(p):
    r"""date_time : CURRENT_DATE
    | CURRENT_TIME      number_param_opt
    | CURRENT_TIMESTAMP number_param_opt
    | LOCALTIME         number_param_opt
    | LOCALTIMESTAMP    number_param_opt"""
    precision = p[2] if len(p) == 3 else None
    p[0] = CurrentTime(p.lineno(1), p.lexpos(1), type=p[1], precision=precision)


def p_comparison_operator(p):
    r"""comparison_operator : EQ
    | NE
    | LT
    | LE
    | GT
    | GE"""
    p[0] = p[1]


def p_boolean_value(p):
    r"""boolean_value : TRUE
    | FALSE"""
    p[0] = BooleanLiteral(p.lineno(1), p.lexpos(1), value=p[1])


def p_number_param_opt(p):
    """number_param_opt : LPAREN number RPAREN
    | LPAREN RPAREN
    | empty"""
    p[0] = int(p[2]) if len(p) == 4 else None


def p_qualified_name(p):
    r"""qualified_name : identifier
    | identifier PERIOD identifier
    | identifier PERIOD identifier PERIOD identifier"""
    parts = [p[1]]
    if len(p) == 4:
        parts.append(p[3])
    if len(p) == 6:
        parts.append(p[3])
        parts.append(p[5])
    p[0] = QualifiedName(parts=parts)


def p_identifier(p):
    r"""identifier : IDENTIFIER
    | quoted_identifier
    | non_reserved
    | DIGIT_IDENTIFIER
    | ASTERISK"""
    p[0] = p[1]


def p_non_reserved(p):
    r"""non_reserved : ACCESSIBLE
    | ACCOUNT
    | ACTION
    | ACTIVE
    | ADDDATE
    | AFTER
    | AGAINST
    | AGGREGATE
    | ALGORITHM
    | ALWAYS
    | ANALYSE
    | ANY
    | APPROX_COUNT_DISTINCT
    | APPROX_COUNT_DISTINCT_SYNOPSIS
    | APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE
    | ASCII
    | ASENSITIVE
    | AT
    | AUTHORS
    | AUTO
    | AUTOEXTEND_SIZE
    | AUTO_INCREMENT
    | AVG
    | AVG_ROW_LENGTH
    | BACKUP
    | BALANCE
    | BASE
    | BASELINE
    | BASELINE_ID
    | BASIC
    | BEGI
    | BEFORE
    | BINARY
    | BINDING
    | BINLOG
    | BIT
    | BLOCK
    | BLOCK_INDEX
    | BLOCK_SIZE
    | BLOOM_FILTER
    | BOOL
    | BOOLEAN
    | BOOTSTRAP
    | BOTH
    | BTREE
    | BYTE
    | BREADTH
    | BUCKETS
    | BULK
    | CACHE
    | KVCACHE
    | FILE_ID
    | ILOGCACHE
    | CANCEL
    | CASCADED
    | CAST
    | CATALOG_NAME
    | CHAIN
    | CHANGED
    | CHARSET
    | CHECKSUM
    | CHECKPOINT
    | CHUNK
    | CIPHER
    | CLASS_ORIGIN
    | CLEAN
    | CLEAR
    | CLIENT
    | CLOG
    | CLOSE
    | CLUSTER
    | CLUSTER_ID
    | CLUSTER_NAME
    | COALESCE
    | COLUMN_STAT
    | CODE
    | COLLATION
    | COLUMN_FORMAT
    | COLUMN_NAME
    | COLUMNS
    | COMMENT
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPLETION
    | COMPRESSED
    | COMPRESSION
    | CONCURRENT
    | CONNECTION
    | CONSISTENT
    | CONSISTENT_MODE
    | CONSTRAINT_CATALOG
    | CONSTRAINT_NAME
    | CONSTRAINT_SCHEMA
    | CONTAINS
    | CONTEXT
    | CONTRIBUTORS
    | COPY
    | COUNT
    | CPU
    | CREATE_TIMESTAMP
    | CTXCAT
    | CTX_ID
    | CUBE
    | CURDATE
    | CURRENT
    | CURTIME
    | CURSOR_NAME
    | CUME_DIST
    | CYCLE
    | CALL
    | CASCADE
    | CHANGE
    | CHARACTER
    | CONSTRAINT
    | CONTINUE
    | CONVERT
    | COLLATE
    | CROSS
    | CURSOR
    | DATA
    | DATAFILE
    | DATA_TABLE_ID
    | DATE
    | DATE_ADD
    | DATE_SUB
    | DATETIME
    | DAY
    | DEALLOCATE
    | DEFAULT_AUTH
    | DEFINER
    | DELAY
    | DELAY_KEY_WRITE
    | DEPTH
    | DES_KEY_FILE
    | DENSE_RANK
    | DESTINATION
    | DIAGNOSTICS
    | DIRECTORY
    | DISABLE
    | DISCARD
    | DISK
    | DISKGROUP
    | DO
    | DUMP
    | DUMPFILE
    | DUPLICATE
    | DUPLICATE_SCOPE
    | DYNAMIC
    | DATABASE_ID
    | DEFAULT_TABLEGROUP
    | DAY_HOUR
    | DAY_MICROSECOND
    | DAY_MINUTE
    | DAY_SECOND
    | DATABASES
    | DEC
    | DECLARE
    | DELAYED
    | DETERMINISTIC
    | DISTINCTROW
    | DIV
    | DUAL
    | EFFECTIVE
    | ENABLE
    | ENCRYPTION
    | END
    | ENDS
    | ENGINE_
    | ENGINES
    | ENUM
    | ENTITY
    | ERROR_CODE
    | ERROR_P
    | ERRORS
    | ESCAPE
    | EVENT
    | EVENTS
    | EVERY
    | EXCHANGE
    | EXECUTE
    | EXPANSION
    | EXPIRE
    | EXPIRE_INFO
    | EXPORT
    | OUTLINE
    | EXTENDED
    | EXTENDED_NOADDR
    | EXTENT_SIZE
    | EXTRACT
    | EACH
    | ENCLOSED
    | ELSEIF
    | ESCAPED
    | EXIT
    | EXPLAIN
    | FAST
    | FAULTS
    | FIELDS
    | FILEX
    | FINAL_COUNT
    | FIRST
    | FIRST_VALUE
    | FIXED
    | FLUSH
    | FOLLOWER
    | FORMAT
    | FOUND
    | FREEZE
    | FREQUENCY
    | FUNCTION
    | FOLLOWING
    | FETCH
    | FLOAT4
    | FLOAT8
    | FORCE
    | FLASHBACK
    | GENERAL
    | GEOMETRY
    | GEOMETRYCOLLECTION
    | GET_FORMAT
    | GLOBAL
    | GRANTS
    | GROUP_CONCAT
    | GROUPING
    | GTS
    | GET
    | GENERATED
    | GLOBAL_NAME
    | HANDLER
    | HASH
    | HELP
    | HISTOGRAM
    | HOST
    | HOSTS
    | HOUR
    | ID
    | IDC
    | HIGH_PRIORITY
    | HOUR_MICROSECOND
    | HOUR_MINUTE
    | HOUR_SECOND
    | IDENTIFIED
    | IGNORE_SERVER_IDS
    | ILOG
    | IMPORT
    | INCR
    | INDEXES
    | INDEX_TABLE_ID
    | INFO
    | INITIAL_SIZE
    | INNODB
    | INSERT_METHOD
    | INSTALL
    | INSTANCE
    | INVOKER
    | IO
    | IO_THREAD
    | IPC
    | ISOLATION
    | ISSUER
    | IS_TENANT_SYS_POOL
    | INVISIBLE
    | IF
    | IFIGNORE
    | IGNORE
    | INNER
    | INFILE
    | INOUT
    | INSENSITIVE
    | INT
    | INT1
    | INT2
    | INT3
    | INT4
    | INT8
    | MERGE
    | IO_AFTER_GTIDS
    | IO_BEFORE_GTIDS
    | ISNULL
    | ITERATE
    | JOB
    | JSON
    | KEY_BLOCK_SIZE
    | KEY_VERSION
    | KEYS
    | KILL
    | LAG
    | LANGUAGE
    | LAST
    | LAST_VALUE
    | LEAD
    | LEADER
    | LEAVES
    | LESS
    | LEAK
    | LEAK_MOD
    | LINESTRING
    | LIST_
    | LISTAGG
    | LOCAL
    | LOCALITY
    | LOCATION
    | LOCKED
    | LOCKS
    | LOGFILE
    | LOGONLY_REPLICA_NUM
    | LOGS
    | LEADING
    | LEAVE
    | LEFT
    | LINEAR
    | LINES
    | LOAD
    | LOCK_
    | LONGB
    | LOB
    | LONGTEXT
    | LOOP
    | LOW_PRIORITY
    | MAJOR
    | MANUAL
    | MASTER
    | MASTER_AUTO_POSITION
    | MASTER_CONNECT_RETRY
    | MASTER_DELAY
    | MASTER_HEARTBEAT_PERIOD
    | MASTER_HOST
    | MASTER_LOG_FILE
    | MASTER_LOG_POS
    | MASTER_PASSWORD
    | MASTER_PORT
    | MASTER_RETRY_COUNT
    | MASTER_SERVER_ID
    | MASTER_SSL
    | MASTER_SSL_CA
    | MASTER_SSL_CAPATH
    | MASTER_SSL_CERT
    | MASTER_SSL_CIPHER
    | MASTER_SSL_CRL
    | MASTER_SSL_CRLPATH
    | MASTER_SSL_KEY
    | MASTER_USER
    | MAX
    | MAX_CONNECTIONS_PER_HOUR
    | MAX_CPU
    | MAX_DISK_SIZE
    | MAX_IOPS
    | MAX_MEMORY
    | MAX_QUERIES_PER_HOUR
    | MAX_ROWS
    | MAX_SESSION_NUM
    | MAX_SIZE
    | MAX_UPDATES_PER_HOUR
    | MAX_USER_CONNECTIONS
    | MEDIUM
    | MEMORY
    | MEMTABLE
    | MESSAGE_TEXT
    | META
    | MICROSECOND
    | MIGRATE
    | MIN
    | MIN_CPU
    | MIN_IOPS
    | MIN_MEMORY
    | MINOR
    | MIN_ROWS
    | MINUTE
    | MODE
    | MODIFY
    | MONTH
    | MOVE
    | MULTILINESTRING
    | MULTIPOINT
    | MULTIPOLYGON
    | MUTEX
    | MYSQL_ERRNO
    | MIGRATION
    | MAX_USED_PART_ID
    | MASTER_BIND
    | MASTER_SSL_VERIFY_SERVER_CERT
    | MATCH
    | MAXVALUE
    | MEDIUMBLOB
    | MEDIUMINT
    | MEDIUMTEXT
    | MIDDLEINT
    | MINUTE_MICROSECOND
    | MINUTE_SECOND
    | MOD
    | MODIFIES
    | NATURAL
    | NO_WRITE_TO_BINLOG
    | NAME
    | NAMES
    | NATIONAL
    | NCHAR
    | NDB
    | NDBCLUSTER
    | NEW
    | NEXT
    | NO
    | NODEGROUP
    | NONE
    | NORMAL
    | NOW
    | NOWAIT
    | NO_WAIT
    | NULLS
    | NVARCHAR
    | NTILE
    | NTH_VALUE
    | NUMERIC
    | OCCUR
    | OFF
    | OFFSET
    | OLD_PASSWORD
    | ONE
    | ONE_SHOT
    | ONLY
    | OPEN
    | OPTIONS
    | ORIG_DEFAULT
    | OWNER
    | OLD_KEY
    | OVER
    | OPTIMIZE
    | OPTIONALLY
    | OUT
    | OUTER
    | OUTFILE
    | PACK_KEYS
    | PAGE
    | PARAMETERS
    | PARSER
    | PARTIAL
    | PARTITION_ID
    | PARTITIONING
    | PARTITIONS
    | PASSWORD
    | PAUSE
    | PERCENT_RANK
    | PHASE
    | PLAN
    | PHYSICAL
    | PLANREGRESS
    | PLUGIN
    | PLUGIN_DIR
    | PLUGINS
    | POINT
    | POLYGON
    | PROCEDURE
    | PURGE
    | PARTITION
    | POOL
    | PORT
    | POSITION
    | PREPARE
    | PRESERVE
    | PREV
    | PRIMARY_ZONE
    | PRIVILEGES
    | PROCESS
    | PROCESSLIST
    | PROFILE
    | PROFILES
    | PROXY
    | PRECEDING
    | PCTFREE
    | P_ENTITY
    | P_CHUNK
    | QUARTER
    | QUERY
    | QUICK
    | RANK
    | READ_ONLY
    | REBUILD
    | RECOVER
    | RECYCLE
    | REDO_BUFFER_SIZE
    | REDOFILE
    | REDUNDANT
    | REFRESH
    | REGION
    | RELAY
    | RELAYLOG
    | RELAY_LOG_FILE
    | RELAY_LOG_POS
    | RELAY_THREAD
    | RELOAD
    | REMOVE
    | REORGANIZE
    | REPAIR
    | REPEATABLE
    | REPLICA
    | REPLICA_NUM
    | REPLICA_TYPE
    | REPLICATION
    | REPORT
    | RESET
    | RESOURCE
    | RESOURCE_POOL_LIST
    | RESPECT
    | RESTART
    | RESTORE
    | RESUME
    | RETURNED_SQLSTATE
    | RETURNS
    | REVERSE
    | REWRITE_MERGE_VERSION
    | ROLLBACK
    | ROLLUP
    | ROOT
    | ROOTTABLE
    | ROOTSERVICE
    | ROUTINE
    | ROW
    | RESIGNAL
    | RESTRICT
    | RETURN
    | RIGHT
    | RLIKE
    | RETURNING
    | ROLLING
    | ROW_COUNT
    | ROW_FORMAT
    | ROWS
    | RTREE
    | RUN
    | RECYCLEBIN
    | ROTATE
    | ROW_NUMBER
    | RUDUNDANT
    | RANGE
    | RECURSIVE
    | READ
    | READ_WRITE
    | READS
    | REAL
    | RELEASE
    | REFERENCES
    | REPLACE
    | REPEAT
    | REQUIRE
    | RANDOM
    | SAMPLE
    | SAVEPOINT
    | SCHEDULE
    | SCHEMA_NAME
    | SCOPE
    | SECOND
    | SECURITY
    | SEED
    | SERIAL
    | SERIALIZABLE
    | SERVER
    | SERVER_IP
    | SERVER_PORT
    | SERVER_TYPE
    | SESSION
    | SESSION_USER
    | SET_MASTER_CLUSTER
    | SET_SLAVE_CLUSTER
    | SET_TP
    | SHARE
    | SHUTDOWN
    | SIGNED
    | SIMPLE
    | SLAVE
    | SLOW
    | SLOT_IDX
    | SNAPSHOT
    | SOCKET
    | SOME
    | SONAME
    | SOUNDS
    | SOURCE
    | SPFILE
    | SPLIT
    | SQL_AFTER_GTIDS
    | SQL_AFTER_MTS_GAPS
    | SQL_BEFORE_GTIDS
    | SQL_BUFFER_RESULT
    | SQL_CACHE
    | SQL_NO_CACHE
    | SQL_ID
    | SQL_THREAD
    | SQL_TSI_DAY
    | SQL_TSI_HOUR
    | SQL_TSI_MINUTE
    | SQL_TSI_MONTH
    | SQL_TSI_QUARTER
    | SQL_TSI_SECOND
    | SQL_TSI_WEEK
    | SQL_TSI_YEAR
    | STANDBY
    | STAT
    | START
    | STARTS
    | STATS_AUTO_RECALC
    | STATS_PERSISTENT
    | STATS_SAMPLE_PAGES
    | STATUS
    | STDDEV
    | STDDEV_POP
    | STDDEV_SAMP
    | PROGRESSIVE_MERGE_NUM
    | STOP
    | STORAGE
    | STORAGE_FORMAT_VERSION
    | STORAGE_FORMAT_WORK_VERSION
    | STORING
    | STRING
    | SUBCLASS_ORIGIN
    | SUBDATE
    | SUBJECT
    | SUBPARTITION
    | SUBPARTITIONS
    | SUBSTR
    | SUBSTRING
    | SUM
    | SUPER
    | SUSPEND
    | SWAPS
    | SWITCH
    | SWITCHES
    | SWITCHOVER
    | SYSTEM
    | SYSTEM_USER
    | SYSDATE
    | SEARCH
    | SECOND_MICROSECOND
    | SCHEMA
    | SCHEMAS
    | SEPARATOR
    | SENSITIVE
    | SIGNAL
    | SPATIAL
    | SPECIFIC
    | SQL
    | SQLEXCEPTION
    | SQLSTATE
    | SQLWARNING
    | SQL_BIG_RESULT
    | SQL_CALC_FOUND_ROWS
    | SQL_SMALL_RESULT
    | SSL
    | STARTING
    | STRAIGHT_JOIN
    | STORED
    | TABLE_CHECKSUM
    | TABLE_MODE
    | TABLE_ID
    | TABLE_NAME
    | TABLEGROUPS
    | TABLES
    | TABLESPACE
    | TABLET
    | TABLET_MAX_SIZE
    | TEMPLATE
    | TEMPORARY
    | TEMPTABLE
    | TENANT
    | TEXT
    | THAN
    | TIME
    | TIMESTAMP
    | TIMESTAMPADD
    | TIMESTAMPDIFF
    | TP_NO
    | TP_NAME
    | TRACE
    | TRADITIONAL
    | TRANSACTION
    | TRIGGERS
    | TRIM
    | TRUNCATE
    | TYPE
    | TYPES
    | TASK
    | TABLET_SIZE
    | TABLEGROUP_ID
    | TENANT_ID
    | TERMINATED
    | TINYBLOB
    | TINYTEXT
    | TABLEGROUP
    | TRAILING
    | UNCOMMITTED
    | UNDEFINED
    | UNDO_BUFFER_SIZE
    | UNDOFILE
    | UNICODE
    | UNINSTALL
    | UNIT
    | UNIT_NUM
    | UNLOCKED
    | UNTIL
    | UNUSUAL
    | UPGRADE
    | USE_BLOOM_FILTER
    | UNKNOWN
    | USE_FRM
    | USER
    | USER_RESOURCES
    | UNBOUNDED
    | UNDO
    | UNLOCK
    | USING
    | UTC_DATE
    | UTC_TIME
    | UTC_TIMESTAMP
    | VALID
    | VALUE
    | VARIANCE
    | VARIABLES
    | VERBOSE
    | MATERIALIZED
    | VIEW
    | VISIBLE
    | VIRTUAL_COLUMN_ID
    | VARCHARACTER
    | VARYING
    | VIRTUAL
    | WAIT
    | WARNINGS
    | WEEK
    | WEIGHT_STRING
    | WITH_ROWID
    | WORK
    | WRAPPER
    | WHILE
    | WRITE
    | INNER_PARSE
    | X509
    | XA
    | XML
    | XOR
    | YEAR
    | LEVEL
    | YEAR_MONTH
    | ACTIVATE
    | SYNCHRONIZATION
    | ZONE
    | ZONE_LIST
    | TIME_ZONE_INFO
    | ZONE_TYPE
    | MATCHED
    | AUDIT
    | PL
    | ZEROFILL
    | ARCHIVELOG
    | NOARCHIVELOG
    | INCREMENTAL
    | EXPIRED
    | PREVIEW
    | VALIDATE
    | BACKUPSET
    | REMOTE_OSS
    | GLOBAL_ALIAS
    | SESSION_ALIAS"""
    p[0] = p[1]


def p_quoted_identifier(p):
    r"""quoted_identifier : BACKQUOTED_IDENTIFIER"""
    p[0] = p[1][1:-1]


def p_figure(p):
    r"""figure : FRACTION
    | NUMBER"""
    if p.slice[1].type == "FRACTION":
        p[0] = DoubleLiteral(p.lineno(1), p.lexpos(1), p[1])
    else:
        p[0] = LongLiteral(p.lineno(1), p.lexpos(1), p[1])


def p_empty(p):
    """empty :"""
    pass


def p_error(p):
    if p:
        stack_state_str = ' '.join([symbol.type for symbol in parser.symstack][1:])

        print(
            'Syntax error in input! Parser State:{} {} . {}'.format(
                parser.state, stack_state_str, p
            )
        )

        err = SyntaxError()
        err.lineno = p.lineno
        err.text = p.lexer.lexdata
        err.token_value = p.value

        try:
            text_lines = err.text.split("\n")
            line_lengths = [len(line) + 1 for line in text_lines]
            err_line_offset = sum(line_lengths[: err.lineno - 1])

            if err.lineno - 1 < len(text_lines):
                err.line = text_lines[err.lineno - 1]
                err.offset = p.lexpos - err_line_offset

                pointer = " " * err.offset + "^" * len(err.token_value)
                error_line = err.line + "\n" + pointer
            else:
                error_line = ''
        except Exception:
            raise SyntaxError("The current version does not support this SQL")
        if err.offset:
            err.msg = "The current version does not support this SQL %d (%s) \n %s" % (
                err.offset,
                str(err.token_value),
                error_line,
            )
        else:
            err.msg = "The current version does not support this SQL"

        def _print_error(self):
            pointer = " " * self.offset + "^" * len(self.token_value)
            error_line = self.line + "\n" + pointer
            print(error_line)

        _print_error = types.MethodType(_print_error, err)
        err.print_file_and_line = _print_error

        raise err
    raise SyntaxError("The current version does not support this SQL")


parser = yacc.yacc(tabmodule="parser_table", debugfile="parser.out")
expression_parser = yacc.yacc(
    tabmodule="expression_parser_table",
    start="command",
    debugfile="expression_parser.out",
)
