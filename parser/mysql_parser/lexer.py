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

from parser.mysql_parser.reserved import *

reserved = sorted(set(presto_tokens).difference(presto_nonreserved))

tokens = ['INTEGER', 'DECIMAL', 'NUMBER',
          'IDENTIFIER', 'DIGIT_IDENTIFIER',
          'QUOTED_IDENTIFIER', 'BACKQUOTED_IDENTIFIER',
          'STRING', 'PERIOD',
          'COMMA',
          'PLUS', 'MINUS',
          'LPAREN', 'RPAREN',
          'GT', 'GE',
          'LT', 'LE',
          'EQ', 'NE',
          'CONCAT', 'SLASH',
          'ASTERISK', 'PERCENT',
          'TOP',  # ADQL
          'NON_RESERVED',
          'QM', 'SCONST'
          ] + reserved + list(presto_nonreserved)

t_LPAREN = '\('
t_RPAREN = '\)'

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

t_CONCAT = r'\|\|'

t_ignore = ' \t'


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
    if val.upper() in reserved:
        t.type = val.upper()
    if val in presto_nonreserved:
        t.type = "NON_RESERVED"
    if val.upper() == "TOP":
        t.type = "TOP"
    return t


def t_QUOTED_IDENTIFIER(t):
    r'"([^"]|"")*"'
    val = t.value.lower()
    if val in reserved:
        t.type = reserved[val]
    return t


def t_BACKQUOTED_IDENTIFIER(t):
    r'`([^`]|``)*`'
    val = t.value.lower()
    if val in reserved:
        t.type = reserved[val]
    return t


def t_newline(t):
    r'[\r\n]+'
    t.lexer.lineno += t.value.count("\n")


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


lexer = lex.lex()
