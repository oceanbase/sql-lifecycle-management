# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .node import Node


class Expression(Node):
    def __init__(self, line=None, pos=None):
        super(Expression, self).__init__(line, pos)

    def accept(self, visitor, context):
        return visitor.visit_expression(self, context)


class Extract(Expression):
    def __init__(self, line=None, pos=None, expression=None, field=None):
        super(Extract, self).__init__(line, pos)
        self.expression = expression
        self.field = field

    def accept(self, visitor, context):
        return visitor.visit_extract(self, context)


class ArithmeticBinaryExpression(Expression):
    def __init__(self, line=None, pos=None, type=None, left=None, right=None):
        super(ArithmeticBinaryExpression, self).__init__(line, pos)
        self.type = type
        self.left = left
        self.right = right

    def accept(self, visitor, context):
        return visitor.visit_arithmetic_binary(self, context)


class SubscriptExpression(Expression):
    def __init__(self, line=None, pos=None, base=None, index=None):
        super(SubscriptExpression, self).__init__(line, pos)
        self.base = base
        self.index = index

    def accept(self, visitor, context):
        return visitor.visit_subscript_expression(self, context)


class IsPredicate(Expression):
    def __init__(self, line=None, pos=None, is_not=False, value=None, kwd=None):
        super(IsPredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.value = value
        self.kwd = kwd

    def accept(self, visitor, context):
        return visitor.visit_is_predicate(self, context)


class IfExpression(Expression):
    def __init__(
        self, line=None, pos=None, condition=None, true_value=None, false_value=None
    ):
        super(IfExpression, self).__init__(line, pos)
        self.condition = condition
        self.true_value = true_value
        self.false_value = false_value

    def accept(self, visitor, context):
        return visitor.visit_if_expression(self, context)


class BetweenPredicate(Expression):
    def __init__(
        self, line=None, pos=None, is_not=False, value=None, min=None, max=None
    ):
        super(BetweenPredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.value = value
        self.min = min
        self.max = max

    def accept(self, visitor, context):
        return visitor.visit_between_predicate(self, context)


class InPredicate(Expression):
    def __init__(self, line=None, pos=None, is_not=False, value=None, value_list=None):
        super(InPredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.value = value
        self.value_list = value_list

    def accept(self, visitor, context):
        return visitor.visit_in_predicate(self, context)


class SimpleCaseExpression(Expression):
    def __init__(
        self, line=None, pos=None, operand=None, when_clauses=None, default_value=None
    ):
        super(SimpleCaseExpression, self).__init__(line, pos)
        self.operand = operand
        self.when_clauses = when_clauses
        self.default_value = default_value

    def accept(self, visitor, context):
        return visitor.visit_simple_case_expression(self, context)


class ComparisonExpression(Expression):
    def __init__(self, line=None, pos=None, type=None, left=None, right=None):
        super(ComparisonExpression, self).__init__(line, pos)
        self.type = type
        self.left = left
        self.right = right

    def accept(self, visitor, context):
        return visitor.visit_comparison_expression(self, context)


class CompareSubqueryExpr(ComparisonExpression):
    def __init__(
        self,
        line=None,
        pos=None,
        type=None,
        left=None,
        right=None,
        all_or_some_or_any=None,
    ):
        super(CompareSubqueryExpr, self).__init__(
            line, pos, type=type, left=left, right=right
        )
        self.type = type
        self.left = left
        self.right = right
        self.all_or_some_or_any = all_or_some_or_any

    def accept(self, visitor, context):
        return visitor.visit_compare_subquery_expression(self, context)


class AssignmentExpression(Expression):
    def __init__(self, line=None, pos=None, type=None, left=None, right=None):
        super(AssignmentExpression, self).__init__(line, pos)
        self.type = type
        self.left = left
        self.right = right

    def accept(self, visitor, context):
        return visitor.visit_assignment_expression(self, context)


class SearchedCaseExpression(Expression):
    def __init__(self, line=None, pos=None, when_clauses=None, default_value=None):
        super(SearchedCaseExpression, self).__init__(line, pos)
        self.when_clauses = when_clauses
        self.default_value = default_value

    def accept(self, visitor, context):
        return visitor.visit_searched_case_expression(self, context)


class LambdaExpression(Expression):
    def __init__(self, line=None, pos=None, arguments=None, body=None):
        super(LambdaExpression, self).__init__(line, pos)
        self.arguments = arguments
        self.body = body

    def accept(self, visitor, context):
        return visitor.visit_lambda_expression(self, context)


class Cast(Expression):
    def __init__(
        self, line=None, pos=None, expression=None, data_type=None, array=None
    ):
        super(Cast, self).__init__(line, pos)
        self.expression = expression
        self.data_type = data_type
        self.array = array

    def accept(self, visitor, context):
        return visitor.visit_cast(self, context)


class QualifiedNameReference(Expression):
    def __init__(self, line=None, pos=None, name=None):
        super(QualifiedNameReference, self).__init__(line, pos)
        self.name = name

    def accept(self, visitor, context):
        return visitor.visit_qualified_name_reference(self, context)


class FunctionCall(Expression):
    def __init__(
        self, line=None, pos=None, name=None, window=None, distinct=None, arguments=None
    ):
        super(FunctionCall, self).__init__(line, pos)
        self.name = name
        self.window = window
        self.distinct = distinct
        self.arguments = arguments

    def accept(self, visitor, context):
        return visitor.visit_function_call(self, context)


class SoundLike(FunctionCall):
    def __init__(self, line=None, pos=None, arguments=None):
        super(SoundLike, self).__init__(line, pos, arguments=arguments)
        self.arguments = arguments

    def accept(self, visitor, context):
        return visitor.visit_sound_like(self, context)


class SubString(FunctionCall):
    def __init__(self, line=None, pos=None, name=None, arguments=None):
        super().__init__(line, pos, name, arguments)
        self.name = name
        self.arguments = arguments

    def accept(self, visitor, context):
        return visitor.visit_sub_string(self, context)


class TrimFunc(FunctionCall):
    def __init__(
        self,
        line=None,
        pos=None,
        name=None,
        remstr_position=None,
        remstr=None,
        arg=None,
    ):
        super().__init__(line, pos, name)
        self.name = name
        self.remstr_position = remstr_position
        self.remstr = remstr
        self.arg = arg


class Binary(FunctionCall):
    def __init__(self, line=None, pos=None, expr=None):
        super(Binary, self).__init__(line, pos)
        self.expr = expr

    def accept(self, visitor, context):
        return visitor.visit_binary(self, context)


class Convert(FunctionCall):
    def __init__(
        self,
        line=None,
        pos=None,
        expr=None,
        data_type=None,
        using=None,
        charset_name=None,
    ):
        super(Convert, self).__init__(line, pos)
        self.expr = expr
        self.data_type = data_type
        self.using = using
        self.charset_name = charset_name

    def accept(self, visitor, context):
        return visitor.visit_convert(self, context)


class MemberOf(FunctionCall):
    def __init__(self, line=None, pos=None, args=None):
        super(Convert, self).__init__(line, pos)
        self.args = args

    def accept(self, visitor, context):
        return visitor.visit_memberof(self, context)


class JsonTable(FunctionCall):
    def __init__(
        self, line=None, pos=None, expr=None, path=None, column_list=None, alias=None
    ):
        super(JsonTable, self).__init__(line, pos)
        self.expr = expr
        self.path = path
        self.column_list = column_list
        self.alias = alias

    def accept(self, visitor, context):
        return visitor.visit_json_table(self, context)


class AggregateFunc(FunctionCall):
    def __init__(
        self,
        line=None,
        pos=None,
        name=None,
        distinct=None,
        arguments=None,
        over_clause=None,
    ):
        super(AggregateFunc, self).__init__(line, pos)
        self.name = name
        self.distinct = distinct
        self.arguments = arguments
        self.over_clause = over_clause

    def accept(self, visitor, context):
        return visitor.visit_aggregate_func(self, context)


class GroupConcat(AggregateFunc):
    def __init__(
        self,
        line=None,
        pos=None,
        distinct=None,
        args=None,
        order_by=None,
        separator=None,
        over_clause=None,
    ):
        super(GroupConcat, self).__init__(line, pos)
        self.distinct = distinct
        self.args = args
        self.order_by = order_by
        self.separator = separator
        self.over_clause = over_clause

    def accept(self, visitor, context):
        return visitor.visit_group_concat(self, context)


class DereferenceExpression(Expression):
    def __init__(self, line=None, pos=None, base=None, field_name=None):
        super(DereferenceExpression, self).__init__(line, pos)
        self.base = base
        self.fieldName = field_name

    def accept(self, visitor, context):
        return visitor.visit_dereference_expression(self, context)


class LogicalBinaryExpression(Expression):
    def __init__(self, line=None, pos=None, type=None, left=None, right=None):
        super(LogicalBinaryExpression, self).__init__(line, pos)
        self.type = type
        self.left = left
        self.right = right

    def accept(self, visitor, context):
        return visitor.visit_logical_binary_expression(self, context)


class CoalesceExpression(Expression):
    def __init__(self, line=None, pos=None, operands=None):
        super(CoalesceExpression, self).__init__(line, pos)
        self.operands = operands

    def accept(self, visitor, context):
        return visitor.visit_coalesce_expression(self, context)


class WhenClause(Expression):
    def __init__(self, line=None, pos=None, operand=None, result=None):
        super(WhenClause, self).__init__(line, pos)
        self.operand = operand
        self.result = result

    def accept(self, visitor, context):
        return visitor.visit_when_clause(self, context)


class Literal(Expression):
    def __init__(self, line=None, pos=None):
        super(Literal, self).__init__(line, pos)

    def accept(self, visitor, context):
        return visitor.visit_literal(self, context)


class InputReference(Expression):
    def __init__(self, line=None, pos=None, channel=None):
        super(InputReference, self).__init__(line, pos)
        self.channel = channel

    def accept(self, visitor, context):
        return visitor.visit_input_reference(self, context)


class LikePredicate(Expression):
    def __init__(
        self, line=None, pos=None, is_not=False, value=None, pattern=None, escape=None
    ):
        super(LikePredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.value = value
        self.pattern = pattern
        self.escape = escape

    def accept(self, visitor, context):
        return visitor.visit_like_predicate(self, context)


class RegexpPredicate(Expression):
    def __init__(self, line=None, pos=None, is_not=False, value=None, pattern=None):
        super(RegexpPredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.value = value
        self.pattern = pattern

    def accept(self, visitor, context):
        return visitor.visit_regexp_predicate(self, context)


class ExistsPredicate(Expression):
    def __init__(self, line=None, pos=None, is_not=False, subquery=None):
        super(ExistsPredicate, self).__init__(line, pos)
        self.is_not = is_not
        self.subquery = subquery

    def accept(self, visitor, context):
        return visitor.visit_exists_predicate(self, context)


class NotExpression(Expression):
    def __init__(self, line=None, pos=None, value=None):
        super(NotExpression, self).__init__(line, pos)
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_not_expression(self, context)


class InListExpression(Expression):
    def __init__(self, line=None, pos=None, values=None):
        super(InListExpression, self).__init__(line, pos)
        self.values = values

    def accept(self, visitor, context):
        return visitor.visit_in_list_expression(self, context)


class ListExpression(Expression):
    def __init__(self, line=None, pos=None, values=None):
        super(ListExpression, self).__init__(line, pos)
        self.values = values

    def accept(self, visitor, context):
        return visitor.visit_list_expression(self, context)


class Row(Expression):
    def __init__(self, line=None, pos=None, items=None):
        super(Row, self).__init__(line, pos)
        self.items = items

    def accept(self, visitor, context):
        return visitor.visit_row(self, context)


class SubqueryExpression(Expression):
    def __init__(self, line=None, pos=None, query=None):
        super(SubqueryExpression, self).__init__(line, pos)
        self.query = query

    def accept(self, visitor, context):
        return visitor.visit_subquery_expression(self, context)


class ArithmeticUnaryExpression(Expression):
    def __init__(self, line=None, pos=None, value=None, sign=None):
        super(ArithmeticUnaryExpression, self).__init__(line, pos)
        self.value = value
        self.sign = sign

    @staticmethod
    def negative(value):
        return ArithmeticUnaryExpression(value=value, sign="-")

    @staticmethod
    def positive(value):
        return ArithmeticUnaryExpression(value=value, sign="+")

    def accept(self, visitor, context):
        return visitor.visit_arithmetic_unary(self, context)


class NullIfExpression(Expression):
    def __init__(self, line=None, pos=None, first=None, second=None):
        super(NullIfExpression, self).__init__(line, pos)
        self.first = first
        self.second = second

    def accept(self, visitor, context):
        return visitor.visit_null_if_expression(self, context)


class IsNotNullPredicate(Expression):
    def __init__(self, line=None, pos=None, value=None):
        super(IsNotNullPredicate, self).__init__(line, pos)
        self.value = value

    def accept(self, visitor, context):
        return visitor.visit_is_not_null_predicate(self, context)


class CurrentTime(Expression):
    def __init__(self, line=None, pos=None, type=None, precision=None):
        super(CurrentTime, self).__init__(line, pos)
        self.type = type
        self.precision = precision

    def accept(self, visitor, context):
        return visitor.visit_current_time(self, context)


class ArrayConstructor(Expression):
    """
    {'type': String, 'name': ARRAY_CONSTRUCTOR = "ARRAY_CONSTRUCTOR", 'order': 0}
    """

    def __init__(self, line=None, pos=None, values=None):
        super(ArrayConstructor, self).__init__(line, pos)
        self.values = values

    def accept(self, visitor, context):
        return visitor.visit_array_constructor(self, context)


class MatchAgainstExpression(Expression):
    def __init__(
        self, line=None, pos=None, column_list=None, expr=None, search_modifier=None
    ):
        super(MatchAgainstExpression, self).__init__(line, pos)
        self.column_list = column_list
        self.expr = expr
        self.search_modifier = search_modifier

    def accept(self, visitor, context):
        return visitor.visit_match_against_expression(self, context)
