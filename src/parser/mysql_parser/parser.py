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

from src.parser.tree.window import (
    FrameBound,
    FrameClause,
    FrameExpr,
    WindowFrame,
    WindowFunc,
    WindowSpec,
)

from src.parser.tree.with_stmt import CommonTableExpr, With, WithHasQuery
from src.parser.tree.expression import (
    AggregateFunc,
    ArithmeticBinaryExpression,
    ArithmeticUnaryExpression,
    AssignmentExpression,
    BetweenPredicate,
    Binary,
    Cast,
    CompareSubqueryExpr,
    ComparisonExpression,
    Convert,
    ExistsPredicate,
    FunctionCall,
    GroupConcat,
    InListExpression,
    InPredicate,
    IsPredicate,
    JsonTable,
    LikePredicate,
    ListExpression,
    LogicalBinaryExpression,
    MatchAgainstExpression,
    MemberOf,
    NotExpression,
    QualifiedNameReference,
    RegexpPredicate,
    SimpleCaseExpression,
    SoundLike,
    SubString,
    SubqueryExpression,
    TrimFunc,
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
    TimeLiteral,
)
from src.parser.tree.node import Node
from src.parser.tree.qualified_name import QualifiedName
from src.parser.tree.query_specification import QuerySpecification
from src.parser.tree.relation import AliasedRelation, Join
from src.parser.tree.select import Select
from src.parser.tree.select_item import SingleColumn
from src.parser.tree.set_operation import Except, Intersect, Union
from src.parser.tree.sort_item import ByItem, PartitionByClause, SortItem
from src.parser.tree.statement import Delete, Insert, Query, Update
from src.parser.tree.table import Table
from src.parser.tree.values import Values
from src.parser.tree.field_type import UNSPECIFIEDLENGTH, FieldType, SQLType

from ply import yacc
from src.optimizer.optimizer_enum import IndexType
from src.parser.mysql_parser.lexer import tokens

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
    | dml
    | others"""
    p[0] = p[1]


def p_ddl(p):
    r"""ddl : create_table"""
    p[0] = p[1]


def p_dml(p):
    r"""dml : statement"""
    p[0] = p[1]


def p_others(p):
    r"""others : COMMIT"""
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
    index_end : empty
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
    | query_spec
    | select_stmt_with_clause"""
    if p.slice[1].type == "query_spec":
        order_by = p[1].order_by
        limit, offset = p[1].limit, p[1].offset
        p[1].order_by = []
        # p[1].limit,p[1].offset=0,0
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


def p_select_stmt_with_clause(p):
    r"""select_stmt_with_clause : with_clause simple_table
    | with_clause subquery
    """
    p[0] = WithHasQuery(p.lineno(1), p.lexpos(1), with_list=p[1], query=p[2])


def p_with_clause(p):
    r"""with_clause : WITH with_list"""
    p[0] = With(p.lineno(1), p.lexpos(1), common_table_expr_list=p[2])


def p_with_list(p):
    r"""with_list : with_list COMMA common_table_expr
    | common_table_expr
    """
    if len(p) == 4 and isinstance(p[1], list):
        p[1].append(p[3])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_common_table_expr(p):
    r"""common_table_expr : identifier ident_list_opt AS subquery"""
    p[0] = CommonTableExpr(
        p.lineno(1), p.lexpos(1), table_name=p[1], column_name_list=p[2], subquery=p[4]
    )


def p_ident_list_opt(p):
    r"""ident_list_opt : LPAREN ident_list RPAREN
    | empty"""
    p[0] = [] if len(p) == 2 else p[2]


def p_ident_list(p):
    r"""ident_list : identifier
    | ident_list COMMA identifier"""
    if len(p) == 4 and isinstance(p[1], list):
        p[1].append(p[3])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_for_update_opt(p):
    r"""for_update_opt : FOR UPDATE
    | FOR UPDATE NOWAIT
    | FOR UPDATE SKIP LOCKED
    | FOR UPDATE WAIT figure
    | LOCK IN SHARE MODE
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
    | with_clause set_operation_stmt_wout_order_limit
    | with_clause set_operation_stmt_w_order
    | with_clause set_operation_stmt_w_limit
    | with_clause set_operation_stmt_w_order_limit
    """
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = WithHasQuery(p.lineno(1), p.lexpos(1), with_list=p[1], query=p[2])


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
        # simple_table.limit,simple_table.offset=0,0
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
    p[0] = p[1] if p[1] else None


def p_order_by(p):
    r"""order_by : ORDER BY sort_items"""
    p[0] = p[3]


def p_sort_items(p):
    r"""sort_items : sort_item
    | sort_items COMMA sort_item"""
    _item_list(p)


def p_sort_item(p):
    r"""sort_item : expression null_ordering_opt
    | expression order null_ordering_opt"""
    if len(p) == 3:
        p[0] = SortItem(
            p.lineno(1), p.lexpos(1), sort_key=p[1], ordering='asc', null_ordering=p[2]
        )
    else:
        p[0] = SortItem(
            p.lineno(1), p.lexpos(1), sort_key=p[1], ordering=p[2], null_ordering=p[3]
        )


def p_order(p):
    r"""order : ASC
    | DESC"""
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
    r"""query_spec : SELECT select_items table_expression_opt order_by_opt limit_opt window_clause_opt for_update_opt"""
    select_items = p[2]
    table_expression_opt = p[3]
    from_relations = table_expression_opt.from_ if table_expression_opt else None
    where = table_expression_opt.where if table_expression_opt else None
    group_by = table_expression_opt.group_by if table_expression_opt else None
    having = table_expression_opt.having if table_expression_opt else None
    p_for_update = p[7]
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
        window_spec_list=p[6],
    )


def p_where_opt(p):
    r"""where_opt : WHERE search_condition
    | empty"""
    if p.slice[1].type == "WHERE":
        p[0] = p[2]
    else:
        p[0] = None


def p_group_by_opt(p):
    r"""group_by_opt : GROUP BY by_list
    | empty"""
    p[0] = SimpleGroupBy(p.lineno(1), p.lexpos(1), columns=p[3]) if p[1] else None


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
    r"""derived_column : expression alias_opt
    | ASTERISK
    | identifier PERIOD ASTERISK
    | identifier PERIOD identifier PERIOD ASTERISK"""
    if p.slice[len(p) - 1].type == "ASTERISK":
        parts = [p[1]]
        if len(p) == 4:
            parts.append(p[3])
        if len(p) == 6:
            parts.append(p[5])
        expr = QualifiedNameReference(
            p.lineno(1), p.lexpos(1), name=QualifiedName(parts=parts)
        )
        p[0] = SingleColumn(p.lineno(1), p.lexpos(1), alias=None, expression=expr)
    else:
        p[0] = SingleColumn(p.lineno(1), p.lexpos(1), alias=p[2], expression=p[1])


def p_table_expression_opt(p):
    r"""table_expression_opt : FROM relations force_index where_opt group_by_opt having_opt
    | empty"""
    if p[1]:
        p[0] = Node(
            p.lineno(1), p.lexpos(1), from_=p[2], where=p[4], group_by=p[5], having=p[6]
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
    join_type = (
        p[2].upper() if p[2] and p[2].upper() in ("LEFT", "RIGHT", "FULL") else "INNER"
    )
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
    | LEFT
    | LEFT OUTER
    | RIGHT
    | RIGHT OUTER
    | FULL
    | FULL OUTER
    | empty"""
    p[0] = p[1]


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
    r"""alias_opt : alias
    | empty"""
    if p.slice[1].type == "alias":
        p[0] = p[1]
    else:
        p[0] = ()


def p_alias(p):
    r"""alias : AS identifier
    | identifier
    | AS string_lit
    | string_lit"""
    if len(p) == 3:
        p[0] = (p[1], p[2])
    else:
        p[0] = p[1]


def p_expression(p):
    r"""expression : search_condition"""
    p[0] = p[1]


def p_search_condition(p):
    r"""search_condition : boolean_term
    | search_condition OR boolean_term
    | search_condition logical_and boolean_term
    | search_condition XOR boolean_term"""
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


def p_boolean_term(p):
    r"""boolean_term : NOT search_condition
    | MATCH LPAREN select_items RPAREN AGAINST LPAREN value_expression full_text_search_modifier_opt RPAREN
    | SINGLE_AT_IDENTIFIER ASSIGNMENTEQ search_condition
    | boolean_factor"""
    if len(p) == 2:
        p[0] = p[1]
    elif p.slice[2].type == "ASSIGNMENTEQ":
        p[0] = AssignmentExpression(
            p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
        )
    elif p.slice[1].type == "NOT":
        p[0] = NotExpression(p.lineno(1), p.lexpos(1), value=p[2])
    elif p.slice[1].type == "MATCH":
        p[0] = MatchAgainstExpression(
            p.lineno(1), p.lexpos(1), column_list=p[2], expr=p[7], search_modifier=p[8]
        )


def p_full_text_search_modifier_opt(p):
    """full_text_search_modifier_opt : IN NATURAL LANGUAGE MODE
    | IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
    | IN BOOLEAN MODE
    | WITH QUERY EXPANSION
    | empty"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = []
        for i in range(1, len(p)):
            p[0].append(p[i])


def p_logical_and(p):
    r"""logical_and : AND
    | ANDAND"""
    p[0] = p[1]


def p_boolean_factor(p):
    r"""boolean_factor : boolean_factor comparison_operator predicate
    | boolean_factor comparison_operator ANY subquery
    | boolean_factor comparison_operator SOME subquery
    | boolean_factor comparison_operator ALL subquery
    | boolean_factor comparison_operator SINGLE_AT_IDENTIFIER ASSIGNMENTEQ predicate
    | predicate"""
    if len(p) == 4:
        p[0] = ComparisonExpression(
            p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
        )
    elif len(p) == 5:
        p[0] = CompareSubqueryExpr(
            p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
        )
    elif len(p) == 6:
        assignment = AssignmentExpression(
            p.lineno(1), p.lexpos(1), type=p[4], left=p[3], right=p[5]
        )
        p[0] = ComparisonExpression(
            p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=assignment
        )
    else:
        p[0] = p[1]


def p_predicate(p):
    r"""predicate : between_predicate
    | in_predicate
    | like_predicate
    | regexp_predicate
    | is_predicate
    | member_predicate
    | sounds_predicate
    | value_expression"""
    p[0] = p[1]


def p_between_predicate(p):
    r"between_predicate : value_expression between_opt predicate AND predicate"
    p[0] = BetweenPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], min=p[3], max=p[5]
    )


def p_sounds_predicate(p):
    r"""sounds_predicate : value_expression SOUNDS LIKE factor"""
    p[0] = SoundLike(p.lineno(1), p.lexpos(1), arguments=[p[1], p[2]])


def p_in_predicate(p):
    r"""in_predicate : value_expression in_opt in_value"""
    p[0] = InPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], value_list=p[3]
    )


def p_like_predicate(p):
    r"""like_predicate : value_expression like_opt value_expression escape_opt"""
    p[0] = LikePredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], pattern=p[3], escape=p[4]
    )


def p_regexp_predicate(p):
    r"""regexp_predicate : value_expression reg_sym_opt value_expression"""
    p[0] = RegexpPredicate(
        p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], pattern=p[3]
    )


def p_is_predicate(p):
    r"""is_predicate : value_expression is_opt NULL
    | value_expression is_opt TRUE
    | value_expression is_opt UNKNOWN
    | value_expression is_opt FALSE"""
    p[0] = IsPredicate(p.lineno(1), p.lexpos(1), is_not=p[2], value=p[1], kwd=p[3])


def p_memeber_predicate(p):
    r"""member_predicate : value_expression MEMBER OF LPAREN factor RPAREN"""
    p[0] = MemberOf(p.lineno(1), p.lexpos(1), args=[p[1], p[5]])


def p_between_opt(p):
    r"""between_opt : NOT BETWEEN
    | BETWEEN"""
    p[0] = p.slice[1].type == "NOT"


def p_escape_opt(p):
    r"""escape_opt : ESCAPE string_lit
    | empty
    """
    p[0] = p[2] if len(p) == 3 else p[1]


def p_string_lit(p):
    r"""string_lit : SCONST
    | QUOTED_IDENTIFIER"""
    p[0] = StringLiteral(p.lineno(1), p.lexpos(1), value=p[1])


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
    r"""numeric_value_expression : numeric_value_expression PLUS numeric_value_expression
    | numeric_value_expression MINUS numeric_value_expression
    | numeric_value_expression ASTERISK numeric_value_expression
    | numeric_value_expression SLASH numeric_value_expression
    | numeric_value_expression DIV numeric_value_expression
    | numeric_value_expression MOD numeric_value_expression
    | numeric_value_expression PERCENT numeric_value_expression
    | numeric_value_expression PIPES numeric_value_expression
    | numeric_value_expression bit_opt numeric_value_expression
    | numeric_value_expression MINUS time_interval
    | numeric_value_expression PLUS time_interval
    | time_interval PLUS numeric_value_expression
    | factor"""
    if len(p) == 4:
        if p.slice[2].type == "bit_opt":
            p[0] = LogicalBinaryExpression(
                p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
            )
        else:
            p[0] = ArithmeticBinaryExpression(
                p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3]
            )
    else:
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
    | SINGLE_AT_IDENTIFIER
    | qualified_name
    | subquery
    | function_call
    | LPAREN call_list RPAREN
    | exists_func_call
    | case_specification
    | cast_func_call
    | window_func_call"""
    if p.slice[1].type == "qualified_name":
        p[0] = QualifiedNameReference(p.lineno(1), p.lexpos(1), name=p[1])
    elif len(p) == 4:
        p[0] = ListExpression(p.lineno(1), p.lexpos(1), values=p[2])
    else:
        p[0] = p[1]


def p_exists_func_call(p):
    r"""exists_func_call : EXISTS subquery"""
    p[0] = ExistsPredicate(p.lineno(1), p.lexpos(1), is_not=False, subquery=p[2])


def p_window_clause_opt(p):
    r"""window_clause_opt : WINDOW window_definition_list
    | empty"""
    p[0] = p[1] if len(p) == 2 else p[2]


def p_window_definition_list(p):
    r"""window_definition_list : window_definition
    | window_definition_list COMMA  window_definition"""
    if len(p) == 4 and isinstance(p[1], list):
        p[1].append(p[3])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_window_definition(p):
    r"""window_definition : window_name AS window_spec"""
    p[3].window_name = p[1]
    p[0] = p[3]


def p_window_func_call(p):
    r"""window_func_call : CUME_DIST LPAREN  RPAREN over_clause
    | DENSE_RANK LPAREN RPAREN over_clause
    | FIRST_VALUE LPAREN expression RPAREN null_treat_opt over_clause
    | LAG LPAREN expression lead_lag_info_opt RPAREN null_treat_opt over_clause
    | LAST_VALUE LPAREN expression RPAREN null_treat_opt over_clause
    | LEAD LPAREN expression lead_lag_info_opt RPAREN null_treat_opt over_clause
    | NTH_VALUE LPAREN expression COMMA base_primary_expression RPAREN null_treat_opt over_clause
    | NTILE LPAREN base_primary_expression RPAREN over_clause
    | PERCENT_RANK LPAREN RPAREN over_clause
    | RANK LPAREN RPAREN over_clause
    | ROW_NUMBER LPAREN RPAREN over_clause
    """
    length = len(p)
    window_spec = p[-1]
    args = []
    ignore_null = None

    if p[1].upper() in {
        "FIRST_VALUE",
        "LAST_VALUE",
        "NTILE",
        "LAG",
        "LEAD",
        "NTH_VALUE",
    }:
        args.append(p[3])
        ignore_null = p[length - 2]
    elif p[1].upper() == "NTILE":
        args.append(p[3])

    if p[1].upper() in {"LAG", "LEAD"}:
        if p[4] != None:
            args.append(p[4])
    if p[1].upper() == "NTH_VALUE":
        args.append(p[5])

    p[0] = WindowFunc(
        p.lineno(1),
        p.lexpos(1),
        func_name=p[1],
        func_args=args,
        ignore_null=ignore_null,
        window_spec=window_spec,
    )


def p_null_treat_opt(p):
    r"""null_treat_opt : RESPECT NULLS
    | IGNORE NULLS
    | empty"""
    p[0] = p[1]


def p_over_clause_opt(p):
    r"""over_clause_opt : over_clause
    | empty
    """
    p[0] = p[1]


def p_over_clause(p):
    r"""over_clause : OVER window_name_or_spec"""
    p[0] = p[2]


def p_window_name_or_spec(p):
    r"""window_name_or_spec : window_name
    | window_spec"""
    p[0] = p[1]


def p_window_spec(p):
    r"""window_spec : LPAREN window_name_opt partition_clause_opt order_by_opt frame_clause_opt RPAREN"""
    p[0] = WindowSpec(
        p.lineno(1),
        p.lexpos(1),
        window_name=p[2],
        partition_by=p[3],
        order_by=p[4],
        frame_clause=p[5],
    )


def p_window_name_opt(p):
    r"""window_name_opt : window_name
    | empty"""
    p[0] = p[1]


def p_window_name(p):
    r"""window_name : identifier"""
    p[0] = p[1]


def p_partition_clause_opt(p):
    r"""partition_clause_opt : partition_clause
    | empty"""
    p[0] = p[1]


def p_partition_clause(p):
    r"""partition_clause : PARTITION BY by_list"""
    p[0] = PartitionByClause(p.lineno(1), p.lexpos(1), items=p[3])


def p_by_list(p):
    r"""by_list : by_item
    | by_list COMMA by_item"""
    if len(p) == 4 and isinstance(p[1], list):
        p[1].append(p[3])
        p[0] = p[1]
    else:
        p[0] = [p[1]]


def p_by_item(p):
    r"""by_item : expression
    | expression order"""
    if len(p) == 2:
        p[0] = ByItem(p.lineno(1), p.lexpos(1), item=p[1])
    else:
        p[0] = ByItem(p.lineno(1), p.lexpos(1), item=p[1], order=p[2])


def p_frame_clause_opt(p):
    r"""frame_clause_opt : frame_units frame_extent
    | empty"""
    p[0] = (
        FrameClause(p.lineno(1), p.lexpos(1), type=p[1], frame_range=p[2])
        if len(p) == 3
        else p[1]
    )


def p_frame_units(p):
    r"""frame_units : ROWS
    | RANGE
    """
    p[0] = p[1]


def p_frame_extent(p):
    r"""frame_extent : frame_start
    | frame_between"""
    p[0] = p[1]


def p_frame_start(p):
    r"""frame_start : CURRENT ROW
    | UNBOUNDED PRECEDING
    | UNBOUNDED FOLLOWING
    | frame_expr PRECEDING
    | frame_expr FOLLOWING
    """
    p[0] = FrameBound(p.lineno(1), p.lexpos(1), type=p[2], expr=p[1])


def p_frame_end(p):
    r"""frame_end : frame_start"""
    p[0] = p[1]


def p_frame_between(p):
    r"""frame_between : BETWEEN frame_start AND frame_end"""
    p[0] = WindowFrame(p.lineno(1), p.lexpos(1), start=p[2], end=p[4])


def p_frame_expr(p):
    r"""frame_expr : figure
    | QM
    | INTERVAL expression time_unit
    |"""
    if len(p) == 4:
        p[0] = FrameExpr(p.lineno(1), p.lexpos(1), value=p[2], unit=p[3])
    else:
        p[0] = FrameExpr(p.lineno(1), p.lexpos(1), value=p[1])


def p_lead_lag_info_opt(p):
    r"""lead_lag_info_opt : COMMA figure default_opt
    | COMMA QM default_opt
    | empty"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = [p[2]]
        if p[3] != None:
            p[0].append(p[3])


def p_default_opt(p):
    r"""default_opt : COMMA expression
    | empty"""
    p[0] = p[-1]


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
    r"""function_call : time_function_call
    | operator_func_call
    | flow_control_func_call
    | mathematical_func_call
    | string_comparsion_func_call
    | string_operator_func_call
    | xml_func_call
    | bit_func_call
    | encry_and_comp_func_call
    | locking_func_call
    | json_func_call
    | information_func_call
    | replication_func_call
    | aggreate_func_call
    | miscellaneous_func_call"""
    p[0] = p[1]


def p_operator_func_call(p):
    r"""operator_func_call : ISNULL LPAREN expression RPAREN
    | LEAST LPAREN expression COMMA call_list RPAREN
    | INTERVAL LPAREN expression COMMA call_list RPAREN
    | GREATEST LPAREN expression COMMA call_list RPAREN
    | COALESCE LPAREN expression COMMA call_list RPAREN
    """
    if len(p) == 5:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=[p[3]]
        )
    else:
        arguments = [p[3]]
        arguments.extend(p[5])
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=arguments
        )


def p_flow_control_func_call(p):
    r"""flow_control_func_call : IF LPAREN expression COMMA expression COMMA expression RPAREN
    | IFNULL LPAREN expression COMMA expression RPAREN
    | NULLIF LPAREN expression COMMA expression RPAREN
    """
    if len(p) == 9:
        p[0] = FunctionCall(
            p.lineno(1),
            p.lexpos(1),
            name=p[1],
            distinct=False,
            arguments=[p[3], p[5], p[7]],
        )
    else:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=[p[3], p[5]]
        )


# HEX FORMAT in string_operator_func_call
def p_mathematical_func_call(p):
    r"""mathematical_func_call : ABS LPAREN expression RPAREN
    | ACOS LPAREN expression RPAREN
    | ASIN LPAREN expression RPAREN
    | ATAN LPAREN expression RPAREN
    | ATAN LPAREN expression COMMA expression RPAREN
    | ATAN2 LPAREN expression COMMA expression RPAREN
    | CELL LPAREN expression RPAREN
    | CEILING LPAREN expression RPAREN
    | CONY LPAREN expression COMMA expression COMMA expression RPAREN
    | COS LPAREN expression RPAREN
    | COT LPAREN expression RPAREN
    | CRC32 LPAREN expression RPAREN
    | DEGREES LPAREN expression RPAREN
    | EXP LPAREN expression RPAREN
    | FLOOR LPAREN expression RPAREN
    | LN LPAREN expression RPAREN
    | LOG LPAREN expression RPAREN
    | LOG LPAREN expression COMMA expression RPAREN
    | LOG2 LPAREN expression RPAREN
    | LOG10 LPAREN expression COMMA expression RPAREN
    | MOD LPAREN expression COMMA expression RPAREN
    | PI LPAREN RPAREN
    | POW LPAREN expression COMMA expression RPAREN
    | POWER LPAREN expression COMMA expression RPAREN
    | RADIANS LPAREN expression RPAREN
    | RAND LPAREN RPAREN
    | RAND LPAREN expression RPAREN
    | ROUND LPAREN expression RPAREN
    | ROUND LPAREN expression COMMA expression RPAREN
    | SIGN LPAREN expression RPAREN
    | SIN LPAREN expression RPAREN
    | SQRT LPAREN expression RPAREN
    | TAN LPAREN expression RPAREN
    | TRUNCATE LPAREN expression COMMA expression RPAREN
    """
    if len(p) == 9:
        p[0] = FunctionCall(
            p.lineno(1),
            p.lexpos(1),
            name=p[1],
            distinct=False,
            arguments=[p[3], p[5], p[7]],
        )
    elif len(p) == 7:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=[p[3], p[5]]
        )
    else:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], distinct=False, arguments=[p[3]]
        )


def p_time_function_call(p):
    r"""time_function_call : curdate_and_synonyms_func
    | curtime_and_synonyms_func
    | now_and_synonyms_func
    | from_unixtime_func
    | get_format_func
    | make_time_func
    | timestamp_add_or_diff_func
    | timestamp_func
    | unix_timestamp_func
    | utc_func
    | week_or_year_func
    | sys_date_func
    | add_or_sub_date_func
    | date_one_para_func
    | date_two_para_func"""
    p[0] = p[1]


def p_curdate_and_synonyms_func(p):
    r"""curdate_and_synonyms_func : CURDATE LPAREN RPAREN
    | CURRENT_DATE LPAREN RPAREN
    | CURRENT_DATE"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])


def p_curtime_and_synonyms_func(p):
    r"""curtime_and_synonyms_func : CURTIME LPAREN RPAREN
    | CURRENT_TIME
    | CURRENT_TIME LPAREN RPAREN
    | CURRENT_TIME LPAREN expression RPAREN
    """
    if len(p) <= 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_now_and_synonyms_func(p):
    r"""now_and_synonyms_func : NOW LPAREN RPAREN
    | NOW LPAREN expression RPAREN
    | CURRENT_TIMESTAMP
    | CURRENT_TIMESTAMP LPAREN RPAREN
    | CURRENT_TIMESTAMP LPAREN expression RPAREN
    | LOCALTIME
    | LOCALTIME LPAREN RPAREN
    | LOCALTIME LPAREN expression RPAREN
    | LOCALTIMESTAMP
    | LOCALTIMESTAMP LPAREN RPAREN
    | LOCALTIMESTAMP  LPAREN expression RPAREN
    """
    if len(p) <= 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_from_unixtime_func(p):
    r"""from_unixtime_func : FROM_UNIXTIME LPAREN expression RPAREN
    | FROM_UNIXTIME LPAREN expression COMMA string_lit RPAREN"""
    if len(p) <= 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_get_format_func(p):
    r"""get_format_func : GET_FORMAT LPAREN format_selector COMMA expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_make_time_func(p):
    r"""make_time_func : MAKEDATE LPAREN expression COMMA expression COMMA expression RPAREN"""
    p[0] = FunctionCall(
        p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5], p[7]]
    )


def p_timestamp_add_or_diff_func(p):
    r"""timestamp_add_or_diff_func : TIMESTAMPADD LPAREN time_unit COMMA expression COMMA expression RPAREN
    | TIMESTAMPDIFF LPAREN time_unit COMMA expression COMMA expression RPAREN
    """
    p[0] = FunctionCall(
        p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5], p[7]]
    )


def p_timestamp_func(p):
    r"""timestamp_func : TIMESTAMP LPAREN expression RPAREN
    | TIMESTAMP LPAREN expression COMMA expression RPAREN
    """
    if len(p) <= 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_unix_timestamp_func(p):
    r"""unix_timestamp_func : UNIXTIMESTAMP LPAREN expression RPAREN
    | UNIXTIMESTAMP LPAREN RPAREN"""
    if len(p) <= 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_utc_func(p):
    r"""utc_func : UTC_DATE
    | UTC_DATE LPAREN RPAREN
    | UTC_TIME
    | UTC_TIME LPAREN RPAREN
    | UTC_TIME LPAREN expression RPAREN
    | UTC_TIMESTAMP
    | UTC_TIMESTAMP LPAREN RPAREN
    | UTC_TIMESTAMP LPAREN expression RPAREN
    """
    if len(p) <= 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_week_or_year_week_func(p):
    r"""week_or_year_func : WEEK LPAREN expression RPAREN
    | WEEK LPAREN expression COMMA expression RPAREN
    | YEARWEEK LPAREN expression RPAREN
    | YEARWEEK LPAREN expression COMMA expression RPAREN
    """
    if len(p) <= 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_sys_date_func(p):
    r"""sys_date_func : SYSDATE
    | SYSDATE LPAREN RPAREN
    | SYSDATE LPAREN expression RPAREN"""
    if len(p) <= 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_add_or_sub_date_func(p):
    r"""add_or_sub_date_func : ADDDATE LPAREN expression COMMA time_interval RPAREN
    | SUBDATE LPAREN expression COMMA time_interval RPAREN
    | DATE_ADD LPAREN expression COMMA time_interval RPAREN
    | DATE_SUB LPAREN expression COMMA time_interval RPAREN
    """
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[5]])


def p_date_one_para_func(p):
    r"""date_one_para_func : DATE LPAREN expression RPAREN
    | DAY LPAREN expression RPAREN
    | DAYNAME LPAREN expression RPAREN
    | DAYOFMONTH LPAREN expression RPAREN
    | DAYOFWEEK LPAREN expression RPAREN
    | DAYOFYEAR LPAREN expression RPAREN
    | HOUR LPAREN expression RPAREN
    | LAST_DAY LPAREN expression RPAREN
    | MICROSECOND LPAREN expression RPAREN
    | MINUTE LPAREN expression RPAREN
    | MONTH LPAREN expression RPAREN
    | MONTHNAME LPAREN expression RPAREN
    | QUARTER LPAREN expression RPAREN
    | SECOND LPAREN expression RPAREN
    | SEC_TO_TIME LPAREN expression RPAREN
    | TIME LPAREN expression RPAREN
    | FROM_DAYS LPAREN expression RPAREN
    | TIME_TO_SEC LPAREN expression RPAREN
    | TO_DAYS LPAREN expression RPAREN
    | TO_SECONDS LPAREN expression RPAREN
    | WEEKDAY LPAREN expression RPAREN
    | WEEKOFYEAR LPAREN expression RPAREN
    | YEAR LPAREN expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_date_two_para_func(p):
    r"""date_two_para_func : DATEDIFF LPAREN expression COMMA expression RPAREN
    | SUBTIME LPAREN expression COMMA expression RPAREN
    | ADDTIME LPAREN expression COMMA expression RPAREN
    | DATE_FORMAT LPAREN expression COMMA expression RPAREN
    | STR_TO_DATE LPAREN expression COMMA expression RPAREN
    | MAKEDATE LPAREN expression COMMA expression RPAREN
    | TIMEDIFF LPAREN expression COMMA expression RPAREN
    | PERIOD_ADD LPAREN expression COMMA expression RPAREN
    | PERIOD_DIFF LPAREN expression COMMA expression RPAREN
    | TIME_FORMAT LPAREN expression COMMA expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_string_operator_func_call(p):
    r"""string_operator_func_call : ASCII LPAREN expression RPAREN
    | BIN  LPAREN expression RPAREN
    | BIT_LENGTH LPAREN expression RPAREN
    | CHAR LPAREN expression RPAREN
    | CHAR LPAREN expression USING charset_name RPAREN
    | CHAR_LENGTH LPAREN expression RPAREN
    | CHARACTER_LENGTH LPAREN expression RPAREN
    | CONCAT LPAREN call_list RPAREN
    | CONCAT_WS LPAREN expression COMMA call_list RPAREN
    | ELT LPAREN expression COMMA call_list RPAREN
    | EXPORT_SET LPAREN expression COMMA expression COMMA expression RPAREN
    | EXPORT_SET LPAREN expression COMMA expression COMMA expression COMMA expression RPAREN
    | EXPORT_SET LPAREN expression COMMA expression COMMA expression COMMA expression COMMA expression RPAREN
    | FIELD LPAREN call_list RPAREN
    | FIND_IN_SET LPAREN expression COMMA expression RPAREN
    | FORMAT LPAREN expression COMMA expression RPAREN
    | FORMAT LPAREN expression COMMA expression COMMA expression RPAREN
    | FROM_BASE64 LPAREN expression RPAREN
    | HEX LPAREN expression RPAREN
    | INSERT LPAREN expression COMMA expression COMMA expression RPAREN
    | INSTR LPAREN expression COMMA expression RPAREN
    | LCASE LPAREN expression RPAREN
    | LEFT LPAREN expression COMMA expression RPAREN
    | LENGTH LPAREN expression COMMA expression RPAREN
    | LOAD_FILE LPAREN expression RPAREN
    | LOCATE LPAREN expression COMMA expression RPAREN
    | LOCATE LPAREN expression COMMA expression COMMA expression RPAREN
    | LOWER LPAREN expression RPAREN
    | LPAD LPAREN expression COMMA expression COMMA expression RPAREN
    | LTRIM LPAREN expression RPAREN
    | MAKE_SET LPAREN expression COMMA expression COMMA call_list RPAREN
    | MID LPAREN expression COMMA expression COMMA expression RPAREN
    | OCT LPAREN expression RPAREN
    | OCTET_LENGTH LPAREN expression RPAREN
    | ORD LPAREN expression RPAREN
    | POSITION LPAREN expression IN expression RPAREN
    | QUOTE LPAREN expression RPAREN
    | REPEAT LPAREN expression COMMA expression RPAREN
    | REPLACE LPAREN expression COMMA expression COMMA expression RPAREN
    | REVERSE LPAREN expression RPAREN
    | RIGHT LPAREN expression COMMA expression RPAREN
    | RPAD LPAREN expression COMMA expression COMMA expression RPAREN
    | RTRIM LPAREN expression RPAREN
    | SOUNDEX LPAREN expression RPAREN
    | SPACE LPAREN expression RPAREN
    | SUBSTRING_INDEX LPAREN expression COMMA expression COMMA expression RPAREN
    | TO_BASE64 LPAREN expression RPAREN
    | UCASE LPAREN expression RPAREN
    | UNHEX LPAREN expression RPAREN
    | UPPER LPAREN expression RPAREN
    | substr_and_syn_func_call
    | weight_string_func_call
    | trim_func_call
    """
    length = len(p)
    if length == 2:
        p[0] = p[1]
    else:
        arguments = []
        if p.slice[length - 2].type == "call_list":
            arguments.extend(p[length - 2])
            length = length - 2
        if length > 4:
            for i in range(3, length, 2):
                arguments.append(p[i])
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=arguments)


def p_substr_and_syn_func_call(p):
    r"""substr_and_syn_func_call : SUBSTR LPAREN expression COMMA expression RPAREN
    | SUBSTR LPAREN expression FROM expression RPAREN
    | SUBSTR LPAREN expression COMMA expression COMMA expression RPAREN
    | SUBSTR LPAREN expression FROM expression FOR expression RPAREN
    | SUBSTRING LPAREN expression COMMA expression RPAREN
    | SUBSTRING LPAREN expression FROM expression RPAREN
    | SUBSTRING LPAREN expression COMMA expression COMMA expression RPAREN
    | SUBSTRING LPAREN expression FROM expression FOR expression RPAREN"""
    arguments = []
    for i in range(3, len(p), 2):
        arguments.append(p[i])
    p[0] = SubString(p.lineno(1), p.lexpos(1), name=p[1], arguments=arguments)


def p_weight_string_func_call(p):
    r"""weight_string_func_call : WEIGHT_STRING LPAREN expression RPAREN
    | WEIGHT_STRING LPAREN expression AS binary_or_char RPAREN"""


def p_trim_func_call(p):
    r"""trim_func_call : TRIM LPAREN remstr_position expression FROM expression RPAREN
    | TRIM LPAREN remstr_position FROM expression RPAREN
    | TRIM LPAREN FROM expression RPAREN
    """
    if len(p) == 8:
        p[0] = TrimFunc(
            p.lineno(1), p.lexpos(1), remstr_position=p[3], remstr=p[4], arg=p[6]
        )
    elif len(p) == 7:
        p[0] = TrimFunc(
            p.lineno(1), p.lexpos(1), remstr_position="BOTH", remstr=p[4], arg=p[6]
        )
    else:
        p[0] = TrimFunc(
            p.lineno(1), p.lexpos(1), remstr_position="BOTH", remstr=' ', arg=p[6]
        )


def p_binary_or_char(p):
    r"""binary_or_char : BINARY
    | BINARY LPAREN expression RPAREN
    | CHAR
    | CHAR LPAREN expression RPAREN
    """
    pass


def p_remstr_position(p):
    r"""remstr_position : BOTH
    | LEADING
    | TRAILING"""
    p[0] = p[1]


def p_string_comparsion_func_call(p):
    r"""string_comparsion_func_call : STRCMP LPAREN expression COMMA expression RPAREN
    | REGEXP_INSTR regexp_opt
    | REGEXP_LIKE LPAREN expression COMMA expression RPAREN
    | REGEXP_LIKE LPAREN expression COMMA expression COMMA expression RPAREN
    | REGEXP_REPLACE regexp_opt
    """
    if len(p) == 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])
    elif len(p) == 9:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5], p[7]]
        )
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[2]])


def p_regexp_opt(p):
    r"""regexp_opt : LPAREN expression COMMA expression RPAREN
    | LPAREN expression COMMA expression COMMA expression RPAREN
    | LPAREN expression COMMA expression COMMA expression COMMA expression RPAREN
    | LPAREN expression COMMA expression COMMA expression COMMA expression COMMA expression RPAREN
    | LPAREN expression COMMA expression COMMA expression COMMA expression COMMA expression COMMA expression RPAREN
    """
    p[0] = []
    for i in range(2, len(p), 2):
        p[0].append(p[i])


def p_xml_func_call(p):
    r"""xml_func_call : EXTRACTVALUE LPAREN expression COMMA expression RPAREN
    | UPDATEXML LPAREN expression COMMA expression COMMA expression RPAREN"""
    if len(p) == 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])
    elif len(p) == 9:
        p[0] = FunctionCall(
            p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5], p[7]]
        )


def p_bit_func_call(p):
    r"""bit_func_call : BIT_COUNT LPAREN expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_encry_and_comp_func_call(p):
    """encry_and_comp_func_call : AES_DECRYPT LPAREN expression COMMA expression aes_func_opt RPAREN
    | AES_ENCRYPT LPAREN expression COMMA aes_func_opt RPAREN
    | COMPRESS LPAREN expression RPAREN
    | MD5 LPAREN expression RPAREN
    | RANDOM_BYTES LPAREN expression RPAREN
    | SHA LPAREN expression RPAREN
    | SHA1 LPAREN expression RPAREN
    | SHA2 LPAREN expression COMMA expression RPAREN
    | STATEMENT_DIGEST LPAREN expression RPAREN
    | STATEMENT_DIGEST_TEXT LPAREN expression RPAREN
    | UNCOMPRESS LPAREN expression RPAREN
    | UNCOMPRESSED_LENGTH LPAREN expression RPAREN
    | VALIDATE_PASSWORD_STRENGTH LPAREN expression RPAREN
    """
    if p.slice[1].type == "AES_DECRYPT":
        arguments = [p[3], p[5]]
        arguments.extend(p[6])
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=arguments)
    elif p.slice[1].type == "AES_ENCRYPT":
        arguments = [p[3]]
        arguments.extend(p[6])
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=arguments)
    elif len(p) == 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    elif len(p) == 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_aes_func_opt(p):
    """aes_func_opt : COMMA expression
    | COMMA expression COMMA expression
    | COMMA expression COMMA expression COMMA expression
    | COMMA expression COMMA expression COMMA expression COMMA expression
    | empty
    """
    p[0] = []
    for i in range(2, len(p), 2):
        p[0].append(p[i])


def p_locking_func_call(p):
    r"""locking_func_call : GET_LOCK LPAREN expression COMMA expression RPAREN
    | IS_FREE_LOCK LPAREN expression RPAREN
    | IS_USED_LOCK LPAREN expression RPAREN
    | RELEASE_ALL_LOCKS LPAREN RPAREN
    | RELEASE_LOCK LPAREN expression RPAREN"""
    if len(p) == 4:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    elif len(p) == 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_information_func_call(p):
    """information_func_call : BENCHMARK LPAREN expression COMMA expression RPAREN
    | CHARSET LPAREN expression RPAREN
    | COERCIBILITY LPAREN expression RPAREN
    | COLLATION LPAREN expression RPAREN
    | CONNECTION_ID LPAREN RPAREN
    | CURRENT_ROLE LPAREN RPAREN
    | CURRENT_USER
    | CURRENT_USER LPAREN RPAREN
    | DATABASE LPAREN RPAREN
    | FOUND_ROWS LPAREN RPAREN
    | ICU_VERSION LPAREN RPAREN
    | LAST_INSERT_ID LPAREN RPAREN
    | LAST_INSERT_ID LPAREN expression RPAREN
    | ROLES_GRAPHML LPAREN RPAREN
    | ROW_COUNT LPAREN RPAREN
    | SCHEMA LPAREN RPAREN
    | SESSION_USER LPAREN RPAREN
    | SYSTEM_USER LPAREN RPAREN
    | USER LPAREN RPAREN
    | VERSION LPAREN RPAREN"""
    if len(p) == 4:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    elif len(p) == 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_json_func_call(p):
    """json_func_call : create_json_func_call
    | search_json_func_call
    | modify_json_func_call
    | json_value_attr_func_call
    | json_table_func_call
    | json_schema_func_call
    | json_untility_func_call"""
    p[0] = p[1]


def p_create_json_func_call(p):
    r"""create_json_func_call : JSON_ARRAY LPAREN call_list RPAREN
    | JSON_OBJECT LPAREN call_list RPAREN
    | JSON_QUOTE LPAREN expression RPAREN
    """
    if p.slice[2].type == 'call_list':
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=p[2])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[2]])


def p_search_json_func_call(p):
    r"""search_json_func_call : JSON_CONTAINS LPAREN expression COMMA expression RPAREN
    | JSON_CONTAINS LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_CONTAINS_PATH LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_CONTAINS_PATH LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_EXTRACT LPAREN expression COMMA expression RPAREN
    | JSON_EXTRACT LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_KEYS LPAREN expression RPAREN
    | JSON_KEYS LPAREN expression COMMA expression RPAREN
    | JSON_OVERLAPS LPAREN expression COMMA expression RPAREN
    | JSON_SEARCH LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_SEARCH LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_VALUE LPAREN expression COMMA expression RPAREN
    """
    length = len(p)
    arguments = []
    if p.slice(length - 2).type == "call_list":
        arguments.extend(p[length - 2])
        length -= 2
    for i in range(3, length, 2):
        arguments.append(p[i])
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), args=arguments)


def p_modify_json_func_call(p):
    r"""modify_json_func_call : JSON_ARRAY_APPEND LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_ARRAY_APPEND LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_ARRAY_INSERT LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_ARRAY_INSERT LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_INSERT LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_INSERT LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_MERGE LPAREN expression COMMA expression RPAREN
    | JSON_MERGE LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_MERGE_PATCH LPAREN expression COMMA expression RPAREN
    | JSON_MERGE_PATCH LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_MERGE_PRESERVE LPAREN expression COMMA expression RPAREN
    | JSON_MERGE_PRESERVE LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_REMOVE LPAREN expression COMMA expression RPAREN
    | JSON_REMOVE LPAREN expression COMMA expression COMMA call_list RPAREN
    | JSON_REPLACE LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_REPLACE LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_SET LPAREN expression COMMA expression COMMA expression RPAREN
    | JSON_SET LPAREN expression COMMA expression COMMA expression COMMA call_list RPAREN
    | JSON_UNQUOTE LPAREN expression RPAREN
    """
    length = len(p)
    arguments = []
    if p.slice(length - 2).type == "call_list":
        arguments.extend(p[length - 2])
        length -= 2
    for i in range(3, length, 2):
        arguments.append(p[i])
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), args=arguments)


def p_json_value_attr_func_call(p):
    r"""json_value_attr_func_call : JSON_DEPTH LPAREN expression RPAREN
    | JSON_LENGTH LPAREN expression RPAREN
    | JSON_LENGTH LPAREN expression COMMA expression RPAREN
    | JSON_TYPE LPAREN expression RPAREN
    | JSON_VAILD LPAREN expression RPAREN"""
    if len(p) == 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_json_table_func_call(p):
    r"""json_table_func_call : JSON_TABLE LPAREN expression COMMA expression COLUMNS LPAREN select_items RPAREN alias_opt RPAREN"""
    p[0] = JsonTable(
        p.lineno(1), p.lexpos(1), expr=p[3], path=p[5], column_list=p[8], alias=p[10]
    )


def p_json_schema_func_call(p):
    r"""json_schema_func_call : JSON_SCHEMA_VALID LPAREN expression COMMA expression RPAREN
    | JSON_SCHEMA_VALIDATION_REPORT LPAREN expression COMMA expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[5]])


def p_json_untility_func_call(p):
    r"""json_untility_func_call : JSON_PERTTY LPAREN expression RPAREN
    | JSON_STORAGE_FREE LPAREN expression RPAREN
    | JSON_STORAGE_SIZE LPAREN expression RPAREN"""
    p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])


def p_replication_func_call(p):
    r"""replication_func_call : GROUP_REPLICATION_SET_AS_PRIMARY LPAREN expression RPAREN
    | GROUP_REPLICATION_SWITCH_TO_MULTI_PRIMARY_MODE LPAREN RPAREN
    | GROUP_REPLICATION_SWITCH_TO_SINGLE_PRIMARY_MODE LPAREN expression RPAREN
    | GROUP_REPLICATION_GET_WRITE_CONCURRENCY LPAREN RPAREN
    | GROUP_REPLICATION_SET_WRITE_CONCURRENCY LPAREN expression RPAREN
    | GROUP_REPLICATION_GET_COMMUNICATION_PROTOCOL LPAREN RPAREN
    | GROUP_REPLICATION_SET_COMMUNICATION_PROTOCOL LPAREN expression RPAREN
    | GROUP_REPLICATION_DISABLE_MEMBER_ACTION LPAREN expression COMMA expression RPAREN
    | GROUP_REPLICATION_ENABLE_MEMBER_ACTION LPAREN expression COMMA expression RPAREN
    | GROUP_REPLICATION_RESET_MEMBER_ACTIONS LPAREN RPAREN
    | GTID_SUBSET LPAREN expression COMMA expression RPAREN
    | GTID_SUBTRACT LPAREN expression COMMA expression RPAREN
    | WAIT_FOR_EXECUTED_GTID_SET LPAREN expression RPAREN
    | WAIT_FOR_EXECUTED_GTID_SET LPAREN expression COMMA expression RPAREN
    | WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS LPAREN expression RPAREN
    | WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS LPAREN expression COMMA expression RPAREN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_MANAGED LPAREN expression COMMA expression COMMA expression COMMA expression COMMA expression COMMA expression COMMA expression COMMA expression RPAREN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_SOURCE LPAREN expression COMMA expression COMMA expression COMMA expression COMMA expression RPAREN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_MANAGED LPAREN expression COMMA expression COMMA expression RPAREN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_SOURCE LPAREN expression COMMA expression COMMA expression COMMA expression RPAREN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_RESET LPAREN RPAREN
    | MASTER_POS_WAIT LPAREN expression COMMA expression RPAREN
    | MASTER_POS_WAIT LPAREN expression COMMA expression COMMA expression RPAREN
    | MASTER_POS_WAIT LPAREN expression COMMA expression COMMA expression COMMA expression RPAREN
    | SOURCE_POS_WAIT LPAREN expression COMMA expression RPAREN
    | SOURCE_POS_WAIT LPAREN expression COMMA expression COMMA expression RPAREN
    | SOURCE_POS_WAIT LPAREN expression COMMA expression COMMA expression COMMA expression RPAREN
    """
    arguments = []
    for i in range(3, len(p), 2):
        arguments.append(p[i])


def p_aggregate_func_call(p):
    r"""aggreate_func_call : aggreate_func_without_distinct
    | aggreate_func_wiht_distinct
    | group_concat_func_call
    """
    p[0] = p[1]


def p_aggreate_func_without_distinct(p):
    r"""aggreate_func_without_distinct : BIT_AND LPAREN expression RPAREN over_clause_opt
    | BIT_OR LPAREN expression RPAREN over_clause_opt
    | BIT_XOR LPAREN expression RPAREN over_clause_opt
    | COUNT LPAREN expression RPAREN over_clause_opt
    | COUNT LPAREN ASTERISK RPAREN over_clause_opt
    | JSON_ARRAYAGG LPAREN expression COMMA expression RPAREN over_clause_opt
    | STD LPAREN expression RPAREN over_clause_opt
    | STDDEV LPAREN expression RPAREN over_clause_opt
    | STDDEV_POP LPAREN expression RPAREN over_clause_opt
    | STDDEV_SAMP LPAREN expression RPAREN over_clause_opt
    | VAR_POP LPAREN expression RPAREN over_clause_opt
    | VAR_SAMP LPAREN expression RPAREN over_clause_opt
    | VAR_VARIANCE LPAREN expression RPAREN over_clause_opt
    """
    if len(p) == 6:
        p[0] = AggregateFunc(
            p.lineno(1),
            p.lexpos(1),
            name=p[1],
            arguments=[p[3]],
            over_clause=p[len(p) - 1],
        )
    else:
        p[0] = AggregateFunc(
            p.lineno(1),
            p.lexpos(1),
            name=p[1],
            arguments=[p[3], p[5]],
            over_clause=p[len(p) - 1],
        )


def p_aggreate_func_with_distinct(p):
    r"""aggreate_func_wiht_distinct : AVG LPAREN distinct_opt expression RPAREN over_clause_opt
    | COUNT LPAREN DISTINCT call_list RPAREN over_clause_opt
    | JSON_ARRAYAGG LPAREN distinct_opt expression RPAREN over_clause_opt
    | MAX LPAREN distinct_opt expression RPAREN over_clause_opt
    | SUM LPAREN distinct_opt expression RPAREN over_clause_opt
    | MIN LPAREN distinct_opt expression RPAREN over_clause_opt"""
    arguments = p[4] if p.slice[4].type == "call_list" else [p[4]]
    p[0] = AggregateFunc(
        p.lineno(1),
        p.lexpos(1),
        name=p[1],
        distinct=p[3],
        arguments=arguments,
        over_clause=p[6],
    )


def p_group_concat_func_call(p):
    """group_concat_func_call : GROUP_CONCAT LPAREN distinct_opt call_list order_by_opt separator_opt RPAREN over_clause_opt"""
    p[0] = GroupConcat(
        p.lineno(1),
        p.lexpos(1),
        distinct=p[3],
        args=p[4],
        order_by=p[5],
        separator=p[6],
        over_clause=p[8],
    )


# FORMAT in string_operator_func_call
def p_miscellaneous_func_call(p):
    """miscellaneous_func_call : ANY_VALUE LPAREN expression RPAREN
    | BIN_TO_UUID	LPAREN expression RPAREN
    | BIN_TO_UUID	LPAREN expression COMMA expression RPAREN
    | DEFAULT	LPAREN expression RPAREN
    | GROUPING	LPAREN  call_list RPAREN
    | INET_ATON	LPAREN expression RPAREN
    | INET_NTOA	LPAREN expression RPAREN
    | INET6_ATON	LPAREN expression RPAREN
    | INET6_NTOA	LPAREN expression RPAREN
    | IS_IPV4	LPAREN expression RPAREN
    | IS_IPV4_COMPAT	LPAREN expression RPAREN
    | IS_IPV4_MAPPED	LPAREN expression RPAREN
    | IS_IPV6	LPAREN expression RPAREN
    | IS_UUID	LPAREN expression RPAREN
    | NAME_CONST	LPAREN expression COMMA expression RPAREN
    | SLEEP	LPAREN expression RPAREN
    | UUID	LPAREN RPAREN
    | UUID_SHORT	LPAREN RPAREN
    | UUID_TO_BIN	LPAREN expression RPAREN
    | UUID_TO_BIN	LPAREN expression COMMA expression RPAREN"""
    if p.slice[1].type == "GROUPING":
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=p[3])
    elif len(p) == 4:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[])
    elif len(p) == 5:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3]])
    elif len(p) == 7:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], arguments=[p[3], p[7]])


def p_format_selector(p):
    r"""format_selector : DATE
    | DATETIME
    | TIME
    | TIMESTAMP"""
    p[0] = p[1]


def p_distinct_opt(p):
    r"""distinct_opt : DISTINCT
    | empty"""
    p[0] = p[1]


def p_separator_opt(p):
    r"""separator_opt : SEPARATOR expression
    | empty
    """
    p[0] = p[1]


def p_case_specification(p):
    r"""case_specification : simple_case"""
    p[0] = p[1]


def p_simple_case(p):
    r"""simple_case : CASE expression_opt when_clauses else_opt END"""
    p[0] = SimpleCaseExpression(
        p.lineno(1), p.lexpos(1), operand=p[2], when_clauses=p[3], default_value=p[4]
    )


def p_expression_opt(p):
    r"""expression_opt : expression
    | empty"""
    p[0] = p[1]


def p_cast_func_call(p):
    r"""cast_func_call : BINARY expression %prec NEG
    | _BINARY string_lit %prec NEG
    | CAST LPAREN expression AS cast_field RPAREN
    | CAST LPAREN expression AS cast_field ARRAY RPAREN
    | CONVERT LPAREN expression COMMA cast_field RPAREN
    | CONVERT LPAREN expression USING charset_name RPAREN"""
    if p.slice[1].type == 'CAST':
        if len(p) == 6:
            p[0] = Cast(
                p.lineno(1), p.lexpos(1), expression=p[3], data_type=p[5], array=False
            )
        else:
            p[0] = Cast(
                p.lineno(1), p.lexpos(1), expression=p[3], data_type=p[5], array=True
            )
    elif p.slice[1].type == 'BINARY' or p.slice[1].type == '_BINARY':
        p[0] = Binary(p.lineno(1), p.lexpos(1), expr=p[2])
    elif p.slice[1].type == 'CONVERT':
        if p.slice[4].type == 'USING':
            p[0] = Convert(
                p.lineno(1), p.lexpos(1), expr=p[3], using=True, charset_name=p[5]
            )
        else:
            p[0] = Convert(
                p.lineno(1), p.lexpos(1), expr=p[3], using=False, data_type=p[5]
            )


def p_when_clauses(p):
    r"""when_clauses : when_clauses when_clause
    | when_clause"""
    if len(p) == 2:
        p[0] = [p[1]]
    elif isinstance(p[1], list):
        p[1].append(p[2])
        p[0] = p[1]


def p_charset_name(p):
    r"""charset_name : string_lit
    | BINARY"""
    p[0] = p[1]


def p_when_clause(p):
    r"""when_clause : WHEN expression THEN expression"""
    p[0] = WhenClause(p.lineno(1), p.lexpos(1), operand=p[2], result=p[4])


def p_else_clause(p):
    r"""else_opt : ELSE expression
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
        field.set_tp(SQLType.BINARY, "BINARY")
        field.set_length(p[2])
    elif p.slice[1].type == "char_type":
        field.set_tp(SQLType.CHAR, p[1])
        field.set_length(p[2])
        if p[3] != None:
            field.set_charset_and_collation(f"({','.join(p[3])})")
    elif p.slice[1].type == "DATE":
        field.set_tp(SQLType.DATE, "DATE")
    elif p.slice[1].type == "YEAR":
        field.set_tp(SQLType.YEAR, "YEAR")
    elif p.slice[1].type == 'DATETIME':
        field.set_tp(SQLType.DATETIME, "DATETIME")
        field.set_length(p[2])
    elif p.slice[1].type == 'DECIMAL':
        field.set_tp(SQLType.DECIMAL, "DEMCIMAL")
        field.set_length(p[2]["length"])
        field.set_decimal(p[2]["decimal"])
    elif p.slice[1].type == "TIME":
        field.set_tp(SQLType.TIME, "TIME")
        field.set_length(p[2])
    elif p.slice[1].type == "SIGNED":
        field.set_tp(SQLType.INTEGER, p[2])
    elif p.slice[1].type == "UNSIGNED":
        field.set_tp(SQLType.INTEGER, p[2])
    elif p.slice[1].type == "JSON":
        field.set_tp(SQLType.JSON, "JSON")
    elif p.slice[1].type == "DOUBLE":
        field.set_tp(SQLType.DOUBLE, "DOUBLE")
    elif p.slice[1].type == "FLOAT":
        field.set_tp(SQLType.FLOAT, "FLOAT")
        field.set_length(p[2]["length"])
        field.set_decimal(p[2]["decimal"])
    elif p.slice[1].type == "REAL":
        field.set_tp(SQLType.REAL, "REAL")


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
    | not_keyword_token
    | DIGIT_IDENTIFIER"""
    p[0] = p[1]


# resvered word in mysql but can be used as token
def p_non_reserved(p):
    r"""non_reserved : ABS
    | ACOS
    | AES_DECRYPT
    | AES_ENCRYPT
    | ANY_VALUE
    | ASIN
    | ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_MANAGED
    | ASYNCHRONOUS_CONNECTION_FAILOVER_ADD_SOURCE
    | ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_MANAGED
    | ASYNCHRONOUS_CONNECTION_FAILOVER_DELETE_SOURCE
    | ASYNCHRONOUS_CONNECTION_FAILOVER_RESET
    | ATAN
    | BENCHMARK
    | BIN
    | BIN_TO_UUID
    | BIT_COUNT
    | BIT_LENGTH
    | CALL
    | CAST
    | CELIING
    | CELL
    | CHARACTER_LENGTH
    | CHAR_LENGTH
    | COERCIBILITY
    | COLUMN
    | COMPRESS
    | CONCAT_WS
    | CONNECTION_ID
    | CONY
    | COS
    | COT
    | COUNT
    | CRC32
    | DAYNAME
    | DAYOFMONTH
    | DAYOFWEEK
    | DAYOFYEAR
    | DEGREES
    | ELT
    | EXP
    | EXPORT_SET
    | EXTRACTVALUE
    | EXTRACTVALUEEXP
    | FIELD
    | FIND_IN_SET
    | FLOOR
    | FOUND_ROWS
    | FROM_BASE64
    | FROM_DAYS
    | FROM_UNIXTIME
    | GET_LOCK
    | GREATEST
    | GROUPING
    | GROUP_REPLICATION_DISABLE_MEMBER_ACTION
    | GROUP_REPLICATION_ENABLE_MEMBER_ACTION
    | GROUP_REPLICATION_GET_COMMUNICATION_PROTOCOL
    | GROUP_REPLICATION_GET_WRITE_CONCURRENCY
    | GROUP_REPLICATION_RESET_MEMBER_ACTIONS
    | GROUP_REPLICATION_SET_AS_PRIMARY
    | GROUP_REPLICATION_SET_COMMUNICATION_PROTOCOL
    | GROUP_REPLICATION_SET_WRITE_CONCURRENCY
    | GROUP_REPLICATION_SWITCH_TO_MULTI_PRIMARY_MODE
    | GROUP_REPLICATION_SWITCH_TO_SINGLE_PRIMARY_MODE
    | GTID_SUBSET
    | GTID_SUBTRACT
    | HEX
    | ICU_VERSION
    | IF
    | IFNULL
    | INET6_ATON
    | INET6_NTOA
    | INET_ATON
    | INET_NTOA
    | INSTR
    | INTO
    | IS
    | ISNULL
    | IS_FREE_LOCK
    | IS_IPV4
    | IS_IPV4_COMPAT
    | IS_IPV4_MAPPED
    | IS_IPV6
    | IS_USED_LOCK
    | IS_UUID
    | ITERATE
    | JSON_ARRAY
    | JSON_ARRAYAGG
    | JSON_ARRAY_APPEND
    | JSON_ARRAY_INSERT
    | JSON_CONTAINS
    | JSON_CONTAINS_PATH
    | JSON_DEPTH
    | JSON_EXTRACT
    | JSON_INSERT
    | JSON_KEYS
    | JSON_LENGTH
    | JSON_MERGE
    | JSON_MERGE_PATCH
    | JSON_MERGE_PRESERVE
    | JSON_OBJECT
    | JSON_OVERLAPS
    | JSON_PERTTY
    | JSON_QUOTE
    | JSON_REMOVE
    | JSON_REPLACE
    | JSON_SCHEMA_VALID
    | JSON_SCHEMA_VALIDATION_REPORT
    | JSON_SEARCH
    | JSON_SET
    | JSON_STORAGE_FREE
    | JSON_STORAGE_SIZE
    | JSON_TABLE
    | JSON_TYPE
    | JSON_UNQUOTE
    | JSON_VAILD
    | JSON_VALUE
    | LAST_DAY
    | LAST_INSERT_ID
    | LATERAL
    | LCASE
    | LEAST
    | LENGTH
    | LN
    | LOAD_FILE
    | LOCATE
    | LOG
    | LOG10
    | LOG2
    | LOWER
    | LPAD
    | LTRIM
    | MAKEDATE
    | MAKE_SET
    | MASTER_POS_WAIT
    | MD5
    | MID
    | MONTHNAME
    | NAME_CONST
    | NULLIF
    | OCT
    | OCTET_LENGTH
    | OF
    | OR
    | ORD
    | PARAMETER
    | PERIOD_ADD
    | PERIOD_DIFF
    | PI
    | POW
    | POWER
    | QUOTE
    | RANDOM_BYTES
    | RANK
    | READS
    | REDOFILE
    | RELEASE_ALL_LOCKS
    | RELEASE_LOCK
    | REPLACE
    | REPLICATE
    | RESET
    | RETURN
    | RETURNS
    | ROLES_GRAPHML
    | RPAD
    | RTRIM
    | SCHEMA
    | SEC_TO_TIME
    | SESSION_USER
    | SHA
    | SHA1
    | SHA2
    | SKIP
    | SLEEP
    | SONAME
    | SOUNDEX
    | SOUNDS
    | SOURCE_POS_WAIT
    | SPACE
    | SPATIAL
    | SPECIFIC
    | SQL_AFTER_GTIDS
    | SQL_AFTER_MTS_GAPS
    | SQL_BEFORE_GTIDS
    | SQL_CALC_FOUND_ROWS
    | SQL_SMALL_RESULT
    | SQL_THREAD
    | STACKED
    | STATEMENT_DIGEST
    | STATEMENT_DIGEST_TEXT
    | STOP
    | STORED
    | STRAIGHT_JOIN
    | STRCMP
    | STR_TO_DATE
    | SUBCLASS_ORIGIN
    | SUBSTR
    | SUBSTRING_INDEX
    | SUSPEND
    | SYSTEM_USER
    | TIMEDIFF
    | TIME_FORMAT
    | TIME_TO_SEC
    | TO_BASE64
    | TO_DAYS
    | TO_SECONDS
    | TYPES
    | UCASE
    | UNCOMPRESS
    | UNCOMPRESSED_LENGTH
    | UNDO
    | UNDO_BUFFER_SIZE
    | UNHEX
    | UNINSTALL
    | UNIXTIMESTAMP
    | UNLOCK
    | UPDATEXML
    | UPPER
    | USE
    | UUID
    | UUID_SHORT
    | UUID_TO_BIN
    | VALIDATE_PASSWORD_STRENGTH
    | VALUES
    | VAR_VARIANCE
    | VERSION
    | WAIT_FOR_EXECUTED_GTID_SET
    | WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS
    | WEEKDAY
    | WEEKOFYEAR
    | WITH
    | WORK
    | WRAPPER
    | WRITE
    | XA
    | XID
    | XML
    | XOR
    | YEARWEEK
    | ZEROFILL"""
    p[0] = p[1]


def p_not_keyword_token(p):
    r"""not_keyword_token : ACCOUNT
    | ACTION
    | ADVISE
    | AFTER
    | AGAINST
    | AGO
    | ALGORITHM
    | ALWAYS
    | ANY
    | ASCII
    | AUTO_INCREMENT
    | AVG
    | AVG_ROW_LENGTH
    | BACKUP
    | BEGIN
    | BINLOG
    | BIT
    | BOOL
    | BOOLEAN
    | BTREE
    | BYTE
    | CACHE
    | CAPTURE
    | CASCADED
    | CAUSAL
    | CHARSET
    | CLEANUP
    | CLIENT
    | COALESCE
    | COLLATION
    | COLUMNS
    | COLUMN_FORMAT
    | COMMENT
    | COMMIT
    | COMMITTED
    | COMPACT
    | COMPRESSED
    | COMPRESSION
    | CONCURRENCY
    | CONNECTION
    | CONSISTENCY
    | CONSISTENT
    | CONTEXT
    | CPU
    | CURRENT
    | CYCLE
    | DATA
    | DATE
    | DATETIME
    | DAY
    | DECLARE
    | DISABLE
    | DISABLED
    | DISCARD
    | DISK
    | DO
    | DUPLICATE
    | DYNAMIC
    | ENABLE
    | ENABLED
    | END
    | ENFORCED
    | ENGINE
    | ENUM
    | ERROR
    | ESCAPE
    | EVENT
    | EVENTS
    | EXECUTE
    | EXPANSION
    | EXTENDED
    | FIELDS
    | FILE
    | FIRST
    | FLUSH
    | FOLLOWING
    | FORMAT
    | FOUND
    | FULL
    | FUNCTION
    | GENERAL
    | GLOBAL
    | GRANTS
    | HANDLER
    | HASH
    | HELP
    | HOSTS
    | HOUR
    | IDENTIFIED
    | IMPORT
    | INDEXES
    | INSERT_METHOD
    | INSTANCE
    | INVISIBLE
    | INVOKER
    | IO
    | IPC
    | ISOLATION
    | ISSUER
    | JSON
    | KEY_BLOCK_SIZE
    | LANGUAGE
    | LAST
    | LESS
    | LEVEL
    | LIST
    | LOCAL
    | LOCATION
    | LOCKED
    | LOGS
    | MASTER
    | MAX_ROWS
    | MB
    | MEMBER
    | MEMORY
    | MERGE
    | MICROSECOND
    | MINUTE
    | MINVALUE
    | MIN_ROWS
    | MODE
    | MODIFY
    | MONTH
    | NAMES
    | NATIONAL
    | NEVER
    | NEXT
    | NODEGROUP
    | NONE
    | NOWAIT
    | NULLS
    | NVARCHAR
    | OFF
    | OFFSET
    | ONLINE
    | ONLY
    | ON_DUPLICATE
    | OPEN
    | OPTIONAL
    | PACK_KEYS
    | PAGE
    | PARSER
    | PARTIAL
    | PARTITIONING
    | PARTITIONS
    | PASSWORD
    | PAUSE
    | PERCENT
    | PER_DB
    | PER_TABLE
    | PLUGINS
    | POINT
    | POLICY
    | PRECEDING
    | PRESERVE
    | PRIVILEGES
    | PROCESSLIST
    | PROFILE
    | PROXY
    | PURGE
    | QUARTER
    | QUERY
    | QUICK
    | REBUILD
    | RECOVER
    | REDUNDANT
    | RELOAD
    | REMOVE
    | REORGANIZE
    | REPAIR
    | REPLICATION
    | RESOURCE
    | RESPECT
    | RESTORE
    | RESUME
    | REVERSE
    | ROLLBACK
    | ROW_COUNT
    | ROW_FORMAT
    | RTREE
    | SAVEPOINT
    | SECOND
    | SEPARATOR
    | SERIALIZABLE
    | SESSION
    | SHARE
    | SHUTDOWN
    | SIGNED
    | SIMPLE
    | SLAVE
    | SLOW
    | SNAPSHOT
    | SOME
    | SOURCE
    | SQL_BUFFER_RESULT
    | SQL_CACHE
    | SQL_NO_CACHE
    | SQL_TSI_DAY
    | SQL_TSI_HOUR
    | SQL_TSI_MINUTE
    | SQL_TSI_MONTH
    | SQL_TSI_QUARTER
    | SQL_TSI_SECOND
    | SQL_TSI_WEEK
    | SQL_TSI_YEAR
    | START
    | STATS_AUTO_RECALC
    | STATS_PERSISTENT
    | STATS_SAMPLE_PAGES
    | STATUS
    | STORAGE
    | SUBJECT
    | SUPER
    | TABLES
    | TABLESPACE
    | TABLE_CHECKSUM
    | TEMPORARY
    | TEMPTABLE
    | TIME
    | TIMESTAMP
    | TRANSACTION
    | TRIGGERS
    | TRUNCATE
    | TYPE
    | UNBOUNDED
    | UNCOMMITTED
    | UNDEFINED
    | UNKNOWN
    | USER
    | VALIDATION
    | VALUE
    | VARIABLES
    | WAIT
    | WARNINGS
    | WEEK
    | WEIGHT_STRING
    | WITHOUT
    | X509
    | YEAR"""
    p[0] = p[1]


def p_time_interval(p):
    r"""time_interval : INTERVAL expression time_unit"""
    p[0] = TimeLiteral(p.lineno(1), p.lexpos(1), value=p[2], unit=p[3])


def p_time_unit(p):
    r"""time_unit : timestamp_unit
    | SECOND_MICROSECOND
    | MINUTE_MICROSECOND
    | MINUTE_SECOND
    | HOUR_MICROSECOND
    | HOUR_SECOND
    | HOUR_MINUTE
    | DAY_MICROSECOND
    | DAY_SECOND
    | DAY_MINUTE
    | DAY_HOUR
    | YEAR_MONTH"""
    p[0] = p[1]


def p_timestamp_unit(p):
    r"""timestamp_unit : MICROSECOND
    | SECOND
    | MINUTE
    | HOUR
    | DAY
    | WEEK
    | MONTH
    | QUARTER
    | YEAR
    | SQL_TSI_SECOND
    | SQL_TSI_MINUTE
    | SQL_TSI_HOUR
    | SQL_TSI_DAY
    | SQL_TSI_WEEK
    | SQL_TSI_MONTH
    | SQL_TSI_QUARTER
    | SQL_TSI_YEAR"""
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


parser = yacc.yacc(
    tabmodule="parser_table", start="command", debugfile="parser.out", optimize=True
)
