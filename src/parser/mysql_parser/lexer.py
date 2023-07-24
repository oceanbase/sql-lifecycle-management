# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from ply import lex

from src.parser.mysql_parser.reserved import *

reserved = sorted(set(presto_tokens).difference(presto_nonreserved))

tokens = ['IDENTIFIER', 'DIGIT_IDENTIFIER',
          'QUOTED_IDENTIFIER', 'BACKQUOTED_IDENTIFIER',
          'PERIOD',
          'COMMA',
          'PLUS', 'MINUS',
          'LPAREN', 'RPAREN',
          'ANDAND',
          'ASSIGNMENTEQ',
          'GT', 'GE',
          'LT', 'LE',
          'EQ', 'NE',
          'BIT_OR', 'BIT_AND',
          'BIT_XOR', 'BIT_OPPOSITE', 'EXCLA_MARK',
          'BIT_MOVE_LEFT', 'BIT_MOVE_RIGHT',
          'CONCAT', 'SLASH',
          'ASTERISK', 
          'NON_RESERVED',
          'QM', 'SCONST'
          ] + list(presto_reserved) + list(presto_nonreserved) + list(presto_not_keyword_token)

t_LPAREN = '\('
t_RPAREN = '\)'

t_ASSIGNMENTEQ = r':='
t_EQ = r'='
t_NE = r'<>|!='
t_LT = r'<'
t_LE = r'<='
t_GT = r'>'
t_GE = r'>='
t_PERIOD = r'\.'
t_COMMA = r','
t_PLUS = r'\+'
t_MINUS = r'-'
t_ASTERISK = r'\*'
t_SLASH = r'/'
t_PERCENT = r'%'
t_QM = r'\?'

# TODO
# By default, || is a logical OR operator. 
# With PIPES_AS_CONCAT enabled, || is string concatenation.
# Need support or semantics in future development
t_CONCAT = r'\|\|'

t_ignore = ' \t'

t_ANDAND=r'\&\&'
t_BIT_OR = r'\|'
t_BIT_AND = r'\&'
t_BIT_XOR = r'\^'
t_BIT_OPPOSITE = r'\~'
t_BIT_MOVE_LEFT = r'<<'
t_BIT_MOVE_RIGHT = r'>>'
t_EXCLA_MARK =r'!'

precedence = (
    ('right','ASSIGNMENTEQ'),
    ('left', 'CONCAT','OR'),
    ('left', 'XOR'),
    ('left', 'AND','ADNAND'),
    ('right','NOT'),
    ('left','BETWEEN','CASE','WHEN','THEN','ELSE'),
    ('left','EQ','NE','LT','LE','GT','GE','IS','LIKE','RLIKE','REGEXP','IN'),
    ('left','BIT_OR'),
    ('left','BIT_AND'),
    ('left','BIT_MOVE_LEFT','BIT_MOVE_RIGHT'),
    ('left','PLUS','MINUS'),
    ('left','ASTERISK','SLASH','PERCENT','DIV','MOD'),
    ('left','BIT_XOR'),
    ('left','BIT_OPPOSITE','NEG'),
    ('left','EXCLA_MARK'),
    ('left','LPAREN '),
    ('right','RPAREN ')
)

def t_DOUBLE(t):
    r"""(\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\d*(?:\.\d+)(?:[eE][+-]?\d+)?)"""
    if 'e' in t.value or 'E' in t.value or '.' in t.value:
        t.type = 'DOUBLE'
    else:
        t.type = "INTEGER"
    return t


def t_INTEGER(t):
    r'\d+'
    t.type = "INTEGER"
    return t


# String literal
def t_SCONST(t):
    r'\'([^\\\n]|(\\.))*?\''
    t.type = "SCONST"
    return t


def t_IDENTIFIER(t):
    r"""[a-zA-Z\u4e00-\u9fa5_][a-zA-Z\u4e00-\u9fa50-9_@:]*"""
    val = t.value.lower()
    if val.upper() in tokens:
        t.type = val.upper()
    if val in presto_nonreserved:
        t.type = "NON_RESERVED"
    return t


def t_QUOTED_IDENTIFIER(t):
    r'"([^"]|"")*"'
    val = t.value.lower()
    if val in tokens:
        t.type =tokens[val]
    return t


def t_BACKQUOTED_IDENTIFIER(t):
    r'`([^`]|``)*`'
    val = t.value.lower()
    if val in tokens:
        t.type =tokens[val]
    return t


def t_newline(t):
    r'[\r\n]+'
    t.lexer.lineno += t.value.count("\n")


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


lexer = lex.lex()
