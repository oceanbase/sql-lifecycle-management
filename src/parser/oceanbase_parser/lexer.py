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

from src.parser.oceanbase_parser.reserved import reserved, nonreserved

tokens = (
    [
        'IDENTIFIER',
        'DIGIT_IDENTIFIER',
        'QUOTED_IDENTIFIER',
        'BACKQUOTED_IDENTIFIER',
        'PERIOD',
        'COMMA',
        'PLUS',
        'MINUS',
        'LPAREN',
        'RPAREN',
        'ANDAND',
        'ASSIGNMENTEQ',
        'GT',
        'GE',
        'LT',
        'LE',
        'EQ',
        'NE',
        'BIT_OR',
        'BIT_AND',
        'BIT_XOR',
        'BIT_OPPOSITE',
        'EXCLA_MARK',
        'BIT_MOVE_LEFT',
        'BIT_MOVE_RIGHT',
        'PIPES',
        'SLASH',
        'ASTERISK',
        'QM',
        'SCONST',
        'PERCENT',
        'IGNORE',
        'FRACTION',
        'NUMBER',
    ]
    + list(reserved)
    + list(nonreserved)
)

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

t_PIPES = r'\|\|'

t_ignore = ' \t'

t_ANDAND = r'\&\&'
t_BIT_OR = r'\|'
t_BIT_AND = r'\&'
t_BIT_XOR = r'\^'
t_BIT_OPPOSITE = r'\~'
t_BIT_MOVE_LEFT = r'<<'
t_BIT_MOVE_RIGHT = r'>>'
t_EXCLA_MARK = r'!'


def t_DOUBLE(t):
    r"""(\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\d*(?:\.\d+)(?:[eE][+-]?\d+)?)"""
    if 'e' in t.value or 'E' in t.value or '.' in t.value:
        t.type = "FRACTION"
    else:
        t.type = "NUMBER"
    return t


def t_INTEGER(t):
    r'\d+'
    t.type = "NUMBER"
    return t


# String literal
def t_SCONST(t):
    r'\'([^\']|\'\')*\' '
    t.type = "SCONST"
    return t


def t_IDENTIFIER(t):
    r"""[a-zA-Z\u4e00-\u9fa5_][a-zA-Z\u4e00-\u9fa50-9_@:]*"""
    val = t.value.lower()
    if val.upper() in tokens:
        t.type = val.upper()
    return t


def t_QUOTED_IDENTIFIER(t):
    r'"([^"]|"")*"'
    val = t.value.lower()
    if val in tokens:
        t.type = tokens[val]
    return t


def t_BACKQUOTED_IDENTIFIER(t):
    r'`([^`]|``)*`'
    val = t.value.lower()
    if val in tokens:
        t.type = tokens[val]
    return t


def t_newline(t):
    r'[\r\n]+'
    t.lexer.lineno += t.value.count("\n")


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


lexer = lex.lex()
