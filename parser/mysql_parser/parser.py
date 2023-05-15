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

from optimizer.optimizer_enum import IndexType
from parser.mysql_parser.lexer import tokens
from parser.tree import *

tokens = tokens


def p_command(p):
    r""" command : ddl
                 | dml
                 | others """
    p[0] = p[1]


def p_ddl(p):
    r""" ddl : create_table """
    p[0] = p[1]


def p_dml(p):
    r""" dml : statement """
    p[0] = p[1]


def p_others(p):
    r""" others : COMMIT """
    p[0] = p[1]


def p_create_table(p):
    r""" create_table : CREATE TABLE identifier LPAREN column_list RPAREN create_table_end
                      | CREATE TABLE identifier LPAREN column_list COMMA primary_clause RPAREN create_table_end """
    dict = {}
    dict['type'] = 'create_table'
    dict['table_name'] = p[3]
    dict['element_list'] = p[5]
    if len(p) == 10:
        dict['index_list'] = p[7]
    p[0] = dict


def p_create_table_end(p):
    r""" create_table_end : ENGINE EQ identifier DEFAULT CHARSET EQ identifier
                          | DEFAULT CHARSET EQ identifier COLLATE EQ identifier COMPRESSION EQ SCONST REPLICA_NUM EQ INTEGER BLOCK_SIZE EQ INTEGER USE_BLOOM_FILTER EQ FALSE TABLET_SIZE EQ INTEGER PCTFREE EQ INTEGER"""
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
                    | INT LPAREN INTEGER RPAREN column_end
                    | FLOAT column_end
                    | BIGINT column_end
                    | BIGINT LPAREN INTEGER RPAREN column_end
                    | TINYINT LPAREN INTEGER RPAREN column_end
                    | DATETIME column_end
                    | DATETIME LPAREN INTEGER RPAREN column_end
                    | VARCHAR LPAREN INTEGER RPAREN column_end
                    | CHAR LPAREN INTEGER RPAREN column_end
                    | TIMESTAMP column_end
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
        index_end : BLOCK_SIZE INTEGER
                  | empty
    """


def p_statement(p):
    r"""statement : cursor_specification
                  | delete
                  | update
                  | insert """
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
    r"""delete : DELETE FROM relations where_opt order_by_opt limit_opt """
    p_limit = p[6]
    offset = 0
    limit = 0
    if p_limit:
        offset = int(p_limit[0])
        limit = int(p_limit[1])
    p[0] = Delete(table=p[3], where=p[4], order_by=p[5], limit=limit, offset=offset)


def p_update(p):
    r"""update : UPDATE relations SET update_set_list where_opt order_by_opt limit_opt """
    p_limit = p[7]
    offset = 0
    limit = 0
    if p_limit:
        offset = int(p_limit[0])
        limit = int(p_limit[1])
    p[0] = Update(table=p[2], set_list=p[4], where=p[5], order_by=p[6], limit=limit, offset=offset)


def p_update_set_list(p):
    r"""update_set_list : update_set
                        | update_set_list COMMA update_set"""
    _item_list(p)


def p_update_set(p):
    r"""update_set : comparison_predicate"""
    p[0] = p[1]


def p_cursor_specification(p):
    r"""cursor_specification : query_expression order_by_opt limit_opt for_update_opt"""

    p_limit = p[3]
    offset = 0
    limit = 0
    if p_limit:
        offset = p_limit[0]
        limit = p_limit[1]
    p_for_update = p[4]
    for_update = False
    nowait_or_wait = False
    if p_for_update:
        for_update = p_for_update[0]
        nowait_or_wait = p_for_update[1]
    if isinstance(p[1], QuerySpecification):
        # When we have a simple query specification
        # followed by order by limit, fold the order by and limit
        # clauses into the query specification (analyzer/planner
        # expects this structure to resolve references with respect
        # to columns defined in the query specification)
        query = p[1]
        p[0] = Query(p.lineno(1), p.lexpos(1), with_=None,
                     query_body=QuerySpecification(
                         query.line,
                         query.pos,
                         query.select,
                         query.from_,
                         query.where,
                         query.group_by,
                         query.having,
                         p[2],
                         limit,
                         offset,
                         for_update=query.for_update if query.for_update else for_update,
                         nowait_or_wait=query.nowait_or_wait if query.nowait_or_wait else nowait_or_wait
                     ),
                     limit=limit,
                     offset=offset
                     )
    else:
        p[0] = Query(p.lineno(1), p.lexpos(1),
                     with_=None, query_body=p[1], order_by=p[2], limit=limit, offset=offset)


def p_subquery(p):
    r"""subquery : LPAREN query_expression order_by_opt limit_opt RPAREN"""
    p_limit = p[4]
    offset = 0
    limit = 0
    if p_limit:
        offset = p_limit[0]
        limit = p_limit[1]

    if isinstance(p[2], QuerySpecification):
        p[2].limit = limit
        p[2].offset = offset
        p[2].order_by = p[3] or []
    p[0] = SubqueryExpression(p.lineno(1), p.lexpos(1), query=p[2])


def p_for_update_opt(p):
    r"""for_update_opt : FOR UPDATE
                       | FOR UPDATE NOWAIT
                       | FOR UPDATE WAIT INTEGER
                       | LOCK IN SHARE MODE
                       | empty"""
    if len(p) == 3:
        p[0] = (True, False)
    elif len(p) < 3:
        p[0] = (False, False)
    else:
        p[0] = (True, True)


def p_query_expression(p):
    r"""query_expression : query_expression_body"""
    p[0] = p[1]


def p_query_expression_body(p):
    r"""query_expression_body : nonjoin_query_expression
                              | joined_table"""
    p[0] = p[1]


# ORDER BY
def p_order_by_opt(p):
    r"""order_by_opt : ORDER BY sort_items
                     | empty"""
    p[0] = p[3] if p[1] else None


def p_sort_items(p):
    r"""sort_items : sort_item
                   | sort_items COMMA sort_item"""
    _item_list(p)


def p_sort_item(p):
    r"""sort_item : value_expression order_opt null_ordering_opt"""
    p[0] = SortItem(p.lineno(1), p.lexpos(1),
                    sort_key=p[1], ordering=p[2] or 'asc', null_ordering=p[3])


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
    r"""limit_opt : LIMIT INTEGER
                  | LIMIT INTEGER COMMA INTEGER
                  | LIMIT QM
                  | LIMIT QM COMMA QM
                  | LIMIT ALL
                  | LIMIT INTEGER OFFSET INTEGER
                  | empty"""
    if len(p) < 5:
        p[0] = (0, p[2]) if p[1] else None
    else:
        if p[3] == ',':
            p[0] = (p[2], p[4])
        else:
            p[0] = (p[4], p[2])


# non-join query expression
# QUERY TERM
def p_nonjoin_query_expression(p):
    r"""nonjoin_query_expression : nonjoin_query_term
                        | nonjoin_query_expression UNION set_quantifier_opt nonjoin_query_term
                        | nonjoin_query_expression EXCEPT set_quantifier_opt  nonjoin_query_term"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        left = p[1]
        distinct = p[3] is not None and p[3].upper() == "DISTINCT"
        all = p[3] is not None and p[3].upper() == "ALL"
        right = p[4]
        if p.slice[2].type == "UNION":
            p[0] = Union(p.lineno(1), p.lexpos(1), relations=[left, right], distinct=distinct, all=all)
        else:
            p[0] = Except(p.lineno(1), p.lexpos(1), left=p[1], right=p[4], distinct=distinct, all=all)


# non-join query term
def p_nonjoin_query_term(p):
    r"""nonjoin_query_term : nonjoin_query_primary
                         | nonjoin_query_term INTERSECT set_quantifier_opt nonjoin_query_primary"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        distinct = p[3] is not None and p[3].upper() == "DISTINCT"
        p[0] = Intersect(p.lineno(1), p.lexpos(1), relations=[p[1], p[4]], distinct=distinct)


# non-join query primary
def p_nonjoin_query_primary(p):
    r"""nonjoin_query_primary : simple_table
                              | LPAREN nonjoin_query_expression RPAREN"""
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = TableSubquery(p.lineno(1), p.lexpos(1), query=p[2])


def p_simple_table(p):
    r"""simple_table : query_spec
                     | explicit_table
                     | table_value_constructor"""
    p[0] = p[1]


def p_explicit_table(p):
    r"""explicit_table : TABLE qualified_name"""
    p[0] = Table(p.lineno(1), p.lexpos(1), name=p[2])


def p_table_value_constructor(p):
    r"""table_value_constructor : VALUES values_list"""
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
    r"""query_spec : SELECT select_items table_expression_opt"""
    select_items = p[2]
    table_expression_opt = p[3]
    from_relations = table_expression_opt.from_ if table_expression_opt else None
    where = table_expression_opt.where if table_expression_opt else None
    group_by = table_expression_opt.group_by if table_expression_opt else None
    having = table_expression_opt.having if table_expression_opt else None
    p_for_update = table_expression_opt.for_update if table_expression_opt else None
    for_update = None
    nowait_or_wait = None

    if p_for_update:
        for_update = p_for_update[0]
        nowait_or_wait = p_for_update[1]

    # Reduce the implicit join relations
    from_ = None
    if from_relations:
        from_ = from_relations[0]
        for rel in from_relations[1:]:  # Skip first one
            from_ = Join(p.lineno(3), p.lexpos(3), join_type="IMPLICIT", left=from_, right=rel)

    p[0] = QuerySpecification(p.lineno(1), p.lexpos(1),
                              select=Select(p.lineno(1), p.lexpos(1), select_items=select_items),
                              from_=from_,
                              where=where,
                              group_by=group_by,
                              having=having,
                              for_update=for_update,
                              nowait_or_wait=nowait_or_wait)


def p_where_opt(p):
    r"""where_opt : WHERE search_condition
                  | WHERE LPAREN search_condition RPAREN
                  | empty"""
    if p.slice[1].type == "WHERE":
        p[0] = p[2] if len(p) == 3 else p[3]
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
    r"""derived_column : value_expression alias_opt"""
    p[0] = SingleColumn(p.lineno(1), p.lexpos(1), alias=p[2], expression=p[1])


def p_table_expression_opt(p):
    r"""table_expression_opt : FROM relations force_index where_opt group_by_opt having_opt for_update_opt
                             | empty"""
    if p[1]:
        p[0] = Node(p.lineno(1), p.lexpos(1), from_=p[2], where=p[4], group_by=p[5], having=p[6], for_update=p[7])
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
                      | derived_table"""
    p[0] = p[1]


# joined table
def p_joined_table(p):
    r"""joined_table : cross_join
                     | qualified_join
                     | natural_join"""
    p[0] = p[1]


def p_cross_join(p):
    r"""cross_join : table_reference CROSS JOIN table_primary"""
    p[0] = Join(p.lineno(1), p.lexpos(1), join_type="CROSS",
                left=p[1], right=p[4], criteria=None)


def p_qualified_join(p):
    r"""qualified_join : table_reference join_type JOIN table_reference join_criteria"""
    right = p[4]
    criteria = p[5]
    join_type = p[2].upper() if p[2] and p[2].upper() in ("LEFT", "RIGHT", "FULL") else "INNER"
    p[0] = Join(p.lineno(1), p.lexpos(1), join_type=join_type,
                left=p[1], right=right, criteria=criteria)


def p_natural_join(p):
    r"""natural_join : table_reference NATURAL join_type JOIN table_primary"""
    right = p[5]
    criteria = NaturalJoin()
    join_type = "INNER"
    p[0] = Join(p.lineno(1), p.lexpos(1), join_type=join_type,
                left=p[1], right=right, criteria=criteria)


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
        p[0] = AliasedRelation(p.lineno(1), p.lexpos(1),
                               relation=rel, alias=p[2])
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
    p[0] = p[1]


def p_alias(p):
    r"""alias : as_opt identifier"""
    p[0] = (p[1], p[2])


def p_expression(p):
    r"""expression : search_condition"""
    p[0] = p[1]


def p_search_condition(p):
    r"""search_condition : boolean_term
                         | LPAREN search_condition RPAREN
                         | search_condition OR search_condition
                         | search_condition AND search_condition"""
    if len(p) == 2:
        p[0] = p[1]
    elif p.slice[1].type == "LPAREN":
        p[0] = p[2]
    elif p.slice[2].type == "OR":
        p[0] = LogicalBinaryExpression(p.lineno(1), p.lexpos(1), type="OR", left=p[1], right=p[3])
    elif p.slice[2].type == "AND":
        p[0] = LogicalBinaryExpression(p.lineno(1), p.lexpos(1), type="AND", left=p[1], right=p[3])


def p_boolean_term(p):
    r"""boolean_term : boolean_factor
                     | LPAREN boolean_term RPAREN """
    if len(p) == 2:
        p[0] = p[1]
    elif len(p) == 4 and p.slice[1].type == "LPAREN":
        p[0] = p[2]


def p_boolean_factor(p):
    r"""boolean_factor : not_opt boolean_test"""
    if p[1]:
        p[0] = NotExpression(p.lineno(1), p.lexpos(1), value=p[2])
    else:
        p[0] = p[2]


def p_boolean_test(p):
    r"""boolean_test : boolean_primary"""
    # No IS NOT? (TRUE|FALSE)
    p[0] = p[1]


def p_boolean_primary(p):
    r"""boolean_primary : predicate
                        | value_expression"""
    p[0] = p[1]


def p_predicate(p):
    r"""predicate : comparison_predicate
                  | between_predicate
                  | in_predicate
                  | like_predicate
                  | regexp_predicate
                  | null_predicate
                  | exists_predicate"""
    p[0] = p[1]


def p_comparison_predicate(p):
    r"""comparison_predicate : value_expression comparison_operator value_expression"""
    p[0] = ComparisonExpression(p.lineno(1), p.lexpos(1), type=p[2], left=p[1], right=p[3])


def p_between_predicate(p):
    r"between_predicate : value_expression not_opt BETWEEN value_expression AND value_expression"
    p[0] = BetweenPredicate(p.lineno(1), p.lexpos(1), value=p[1], min=p[4], max=p[6])
    _check_not(p)


def p_in_predicate(p):
    r"""in_predicate : value_expression not_opt IN in_value"""
    p[0] = InPredicate(p.lineno(1), p.lexpos(1), value=p[1], value_list=p[4])
    _check_not(p)


def p_in_value(p):
    r"""in_value : LPAREN in_expressions RPAREN
                 | subquery"""
    if p.slice[1].type == "subquery":
        p[0] = p[1]
    else:
        p[0] = InListExpression(p.lineno(1), p.lexpos(1), values=p[2])


def p_in_expressions(p):
    r"""in_expressions : value_expression
                       | in_expressions COMMA value_expression"""
    _item_list(p)


def p_like_predicate(p):
    r"""like_predicate : value_expression not_opt LIKE value_expression"""
    p[0] = LikePredicate(p.lineno(1), p.lexpos(1), value=p[1], pattern=p[4])
    _check_not(p)


def p_regexp_predicate(p):
    r"""regexp_predicate : value_expression REGEXP value_expression"""
    p[0] = RegexpPredicate(p.lineno(1), p.lexpos(1), value=p[1], pattern=p[3])


def _check_not(p):
    if p[2] and p.slice[2].type == "not_opt":
        p[0] = NotExpression(line=p[0].line, pos=p[0].pos, value=p[0])


def p_null_predicate(p):
    r"""null_predicate : value_expression IS not_opt NULL"""
    if p[3]:  # Not null
        p[0] = IsNotNullPredicate(p.lineno(1), p.lexpos(1), value=p[1])
    else:
        p[0] = IsNullPredicate(p.lineno(1), p.lexpos(1), value=p[1])


def p_exists_predicate(p):
    r"""exists_predicate : EXISTS subquery"""
    p[0] = ExistsPredicate(p.lineno(1), p.lexpos(1), subquery=p[2])


def p_value_expression(p):
    r"""value_expression : numeric_value_expression"""
    p[0] = p[1]


def p_numeric_value_expression(p):
    r"""numeric_value_expression : numeric_value_expression PLUS term
                                 | numeric_value_expression MINUS term
                                 | term"""
    if p.slice[1].type == "numeric_value_expression":
        p[0] = ArithmeticBinaryExpression(p.lineno(1), p.lexpos(1),
                                          type=p[2], left=p[1], right=p[3])
    else:
        p[0] = p[1]


def p_term(p):
    r"""term : term ASTERISK factor
             | term SLASH factor
             | term PERCENT factor
             | term CONCAT factor
             | factor"""
    if p.slice[1].type == "factor":
        p[0] = p[1]
    else:
        p[0] = ArithmeticBinaryExpression(p.lineno(1), p.lexpos(1),
                                          type=p[2], left=p[1], right=p[3])


def p_factor(p):
    r"""factor : sign_opt primary_expression"""
    if p[1]:
        p[0] = ArithmeticUnaryExpression(p.lineno(1), p.lexpos(1), value=p[2], sign=p[1])
    else:
        p[0] = p[2]


def p_primary_expression(p):
    r"""primary_expression : parenthetic_primary_expression
                           | base_primary_expression"""
    p[0] = p[1]


def p_parenthetic_primary_expression(p):
    r"""parenthetic_primary_expression : LPAREN value_expression RPAREN
                                       | LPAREN parenthetic_primary_expression RPAREN"""
    p[0] = p[2]


def p_base_primary_expression(p):
    r"""base_primary_expression : value
                                | qualified_name
                                | subquery
                                | function_call
                                | date_time
                                | case_specification
                                | cast_specification"""
    if p.slice[1].type == "qualified_name":
        p[0] = QualifiedNameReference(p.lineno(1), p.lexpos(1), name=p[1])
    else:
        p[0] = p[1]


def p_value(p):
    r"""value : NULL
              | SCONST
              | number
              | boolean_value
              | QUOTED_IDENTIFIER
              | QM """
    if p.slice[1].type == "NULL":
        p[0] = NullLiteral(p.lineno(1), p.lexpos(1))
    elif p.slice[1].type == "SCONST" or p.slice[1].type == "QUOTED_IDENTIFIER":
        p[0] = StringLiteral(p.lineno(1), p.lexpos(1), p[1][1:-1])
    else:
        p[0] = p[1]


def p_function_call(p):
    r"""function_call : qualified_name LPAREN call_args RPAREN
                      | qualified_name LPAREN DISTINCT call_args RPAREN"""
    if len(p) == 5:
        distinct = p[3] is None or (isinstance(p[3], str) and p[3].upper() == "DISTINCT")
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], distinct=distinct, arguments=p[3])
    else:
        p[0] = FunctionCall(p.lineno(1), p.lexpos(1), name=p[1], distinct=True, arguments=p[4])


def p_call_args(p):
    r"""call_args : call_list
                  | empty_call_args"""
    p[0] = p[1]


def p_empty_call_args(p):
    r"""empty_call_args : ASTERISK
                        | empty"""
    p[0] = []


def p_case_specification(p):
    r"""case_specification : simple_case
                           | searched_case"""
    p[0] = p[1]


def p_simple_case(p):
    r"""simple_case : CASE value_expression when_clauses else_opt END"""
    p[0] = SimpleCaseExpression(p.lineno(1), p.lexpos(1),
                                operand=p[2], when_clauses=p[3], default_value=p[4])


def p_searched_case(p):
    r"""searched_case : CASE when_clauses else_opt END"""
    p[0] = SearchedCaseExpression(p.lineno(1), p.lexpos(1), when_clauses=p[2], default_value=p[3])


def p_cast_specification(p):
    r"""cast_specification : CAST LPAREN value_expression AS data_type RPAREN"""
    p[0] = Cast(p.lineno(1), p.lexpos(1), expression=p[3], data_type=p[5], safe=False)


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


def p_else_clause(p):
    r"""else_opt : ELSE value_expression
                 | empty"""
    p[0] = p[2] if p[1] else None


def p_call_list(p):
    r"""call_list : call_list COMMA expression
                  | expression"""
    _item_list(p)


def p_data_type(p):
    r"""data_type : base_data_type type_param_list_opt"""
    signature = p[1]
    if p[2]:
        # Normalize param list
        type_params = [str(_type) for _type in p[2]]
        signature += "(" + ','.join(type_params) + ")"
    p[0] = signature


def p_type_param_list_opt(p):
    r"""type_param_list_opt : LPAREN type_param_list RPAREN
                            | empty"""
    p[0] = p[2] if p[1] else p[1]


def p_type_param_list(p):
    r"""type_param_list : type_param_list COMMA type_parameter
                        | type_parameter"""
    _item_list(p)


def p_type_parameter(p):
    r"""type_parameter : INTEGER
                       | base_data_type"""
    p[0] = p[1]


def p_base_data_type(p):
    r"""base_data_type : identifier"""
    p[0] = p[1]


def p_date_time(p):
    r"""date_time : CURRENT_DATE
                  | CURRENT_TIME      integer_param_opt
                  | CURRENT_TIMESTAMP integer_param_opt
                  | LOCALTIME         integer_param_opt
                  | LOCALTIMESTAMP    integer_param_opt"""
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


def p_as_opt(p):
    r"""as_opt : AS
               | empty"""
    p[0] = p[1]


def p_not_opt(p):
    r"""not_opt : NOT
                | empty"""
    p[0] = p[1]


def p_boolean_value(p):
    r"""boolean_value : TRUE
                      | FALSE"""
    p[0] = BooleanLiteral(p.lineno(1), p.lexpos(1), value=p[1])


def p_sign_opt(p):
    r"""sign_opt : sign
                 | empty"""
    p[0] = p[1]


def p_sign(p):
    r"""sign : PLUS
             | MINUS"""
    p[0] = p[1]


def p_integer_param_opt(p):
    """integer_param_opt : LPAREN INTEGER RPAREN
                         | LPAREN RPAREN
                         | empty"""
    p[0] = int(p[2]) if len(p) == 4 else None


def p_qualified_name(p):
    r"""qualified_name : qualified_name PERIOD qualified_name
                       | identifier"""
    parts = [p[1]] if len(p) == 2 else p[1].parts + p[3].parts
    p[0] = QualifiedName(parts=parts)


def p_identifier(p):
    r"""identifier : IDENTIFIER
                   | quoted_identifier
                   | non_reserved
                   | DIGIT_IDENTIFIER
                   | ASTERISK"""
    p[0] = p[1]


def p_non_reserved(p):
    r"""non_reserved : NON_RESERVED """
    p[0] = p[1]


def p_quoted_identifier(p):
    r"""quoted_identifier : BACKQUOTED_IDENTIFIER"""
    p[0] = p[1][1:-1]


def p_number(p):
    r"""number : DECIMAL
               | INTEGER"""
    if p.slice[1].type == "DECIMAL":
        p[0] = DoubleLiteral(p.lineno(1), p.lexpos(1), p[1])
    else:
        p[0] = LongLiteral(p.lineno(1), p.lexpos(1), p[1])


def p_empty(p):
    """empty :"""
    pass


def p_error(p):
    if p:
        stack_state_str = ' '.join([symbol.type for symbol in parser.symstack][1:])

        print('Syntax error in input! Parser State:{} {} . {}'
              .format(parser.state,
                      stack_state_str,
                      p))

        err = SyntaxError()
        err.lineno = p.lineno
        err.text = p.lexer.lexdata
        err.token_value = p.value

        try:
            text_lines = err.text.split("\n")
            line_lengths = [len(line) + 1 for line in text_lines]
            err_line_offset = sum(line_lengths[:err.lineno - 1])

            if err.lineno - 1 < len(text_lines):
                err.line = text_lines[err.lineno - 1]
                err.offset = p.lexpos - err_line_offset

                pointer = " " * err.offset + "^" * len(err.token_value)
                error_line = err.line + "\n" + pointer
            else:
                error_line = ''
        except Exception as e:
            raise SyntaxError("The current version does not support this SQL")
        if err.offset:
            err.msg = ("The current version does not support this SQL %d (%s) \n %s"
                       % (err.offset, str(err.token_value), error_line))
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
expression_parser = yacc.yacc(tabmodule="expression_parser_table",
                              start="command",
                              debugfile="expression_parser.out")
