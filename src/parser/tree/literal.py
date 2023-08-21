# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from builtins import int

from .node import Node


class Literal(Node):
    def __init__(self, line=None, pos=None):
        super(Literal, self).__init__(line, pos)


class IntervalLiteral(Literal):
    def __init__(
        self,
        line=None,
        pos=None,
        value=None,
        sign=None,
        start_field=None,
        end_field=None,
    ):
        super(IntervalLiteral, self).__init__(line, pos)
        self.value = value
        self.sign = sign
        self.start_field = start_field
        self.end_field = end_field

    def accept(self, visitor, context):
        return visitor.visit_interval_literal(self, context)


class TimestampLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None):
        super(TimestampLiteral, self).__init__(line, pos)
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_timestamp_literal(self, context)


class NullLiteral(Literal):
    def __init__(self, line=None, pos=None):
        super(NullLiteral, self).__init__(line, pos)

    def accept(self, visitor, context):
        return visitor.visit_null_literal(self, context)


class DoubleLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None):
        super(DoubleLiteral, self).__init__(line, pos)
        self.value = float(value)

    def accept(self, visitor, context):
        return visitor.visit_double_literal(self, context)


class StringLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None):
        super(StringLiteral, self).__init__(line, pos)
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_string_literal(self, context)


class TimeLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None, unit=None):
        super(TimeLiteral, self).__init__(line, pos)
        self.value = value
        self.unit = unit

    def accept(self, visitor, context):
        return visitor.visit_time_literal(self, context)

    def __str__(self):
        return "TIME '%s'" % self.value


class DateLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None, unit=None):
        super(DateLiteral, self).__init__(line, pos)
        self.value = value
        self.unit = unit

    def accept(self, visitor, context):
        return visitor.visit_date_literal(self, context)


def convert_to_decimal(text):
    if text.startswith("0x") or text.startswith("0X"):
        return int(text, 16)
    elif text.startswith("0b") or text.startswith("0B"):
        return int(text, 2)
    elif (text.startswith("x'") or text.startswith("X'")) and text.endswith("'"):
        number = text[2:-1]
        return int(number, 16) if len(number) != 0 else 0
    elif (text.startswith("b'") or text.startswith("B'")) and text.endswith("'"):
        number = text[2:-1]
        return int(number, 2) if len(number) != 0 else 0
    else:
        return int(text)


class LongLiteral(Literal):
    def __init__(self, line=None, pos=None, value=None):
        super(LongLiteral, self).__init__(line, pos)
        self.value = convert_to_decimal(value)
        self.orgin = value

    def accept(self, visitor, context):
        return visitor.visit_long_literal(self, context)

    def __str__(self):
        return self.value


class BooleanLiteral(Literal):
    TRUE_LITERAL = None
    FALSE_LITERAL = None

    def __init__(self, line=None, pos=None, value=None):
        super(BooleanLiteral, self).__init__(line, pos)
        self.value = value.lower() == "true"

    def accept(self, visitor, context):
        return visitor.visit_boolean_literal(self, context)


BooleanLiteral.TRUE_LITERAL = BooleanLiteral(value="TRUE")
BooleanLiteral.FALSE_LITERAL = BooleanLiteral(value="FALSE")
