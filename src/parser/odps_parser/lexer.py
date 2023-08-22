# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import re
from ply import lex

from src.parser.odps_parser.reserved import reserved, nonreserved

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
        'NULL_SAFE_EQ',
        'NE',
        'BIT_OR',
        'BIT_AND',
        'BIT_XOR',
        'BIT_OPPOSITE',
        'SINGLE_AT_IDENTIFIER',
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
        'HEX_NUMBER',
    ]
    + list(reserved)
    + list(nonreserved)
)

sql_tokens = list(reserved) + list(nonreserved)

t_LPAREN = r'\('
t_RPAREN = r'\)'

t_ASSIGNMENTEQ = r':='
t_EQ = r'='
t_NE = r'<>|!='
t_LT = r'<'
t_LE = r'<='
t_GT = r'>'
t_GE = r'>='
t_NULL_SAFE_EQ = r'<=>'
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
    r"[0-9]*\.[0-9]+([eE][-+]?[0-9]+)?|[-+]?[0-9]+([eE][-+]?[0-9]+)"
    if 'e' in t.value or 'E' in t.value or '.' in t.value:
        t.type = "FRACTION"
    else:
        t.type = "NUMBER"
    return t


# String literal
# double ' means single '
def t_SCONST(t):
    r"""'(\\['\\]|[^']|[']{2})*'"""
    t.type = "SCONST"
    return t


def t_NUMBER_START_WITH_XB(t):
    r"""[xX]'[0-9A-Fa-f]*'|[bB]'[0-1]*'"""
    t.type = "NUMBER"
    return t


def t_IDENTIFIER(t):
    r"""[a-zA-Z\u4e00-\u9fa50-9_][a-zA-Z\u4e00-\u9fa50-9_@:]*"""
    if re.match(
        r'(^0[xX][0-9a-fA-F]+$)|(^[xX]\'[0-9a-fA-F]+\'$)|(^0[bB][01]+$)|(^[bB]\'[01]+\'$)|(^\d+$)',
        t.value,
    ):
        t.type = "NUMBER"
    else:
        val = t.value.lower()
        if val.upper() in sql_tokens:
            t.type = val.upper()
    return t


def t_QUOTED_IDENTIFIER(t):
    r'"(\\[\\"]|[^"]|["]{2})*"'
    t.type = "QUOTED_IDENTIFIER"
    return t


# start with single @
def t_SINGLE_AT_IDENTIFIER(t):
    r"""@[^@][\w@\u4e00-\u9fa5]*"""
    t.type = "SINGLE_AT_IDENTIFIER"
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


def t_COMMENT(t):
    r'(\/\*\*\/)|(/\*.+\*/)'
    pass


lexer = lex.lex()
