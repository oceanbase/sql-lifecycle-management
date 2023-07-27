# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class ExpressionRewriter(object):
    def __init__(self, line=None, pos=None):
        self.line = line
        self.pos = pos

    def rewrite_expression(self, node, context, tree_rewriter):
        return None

    def rewrite_row(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_arithmetic_unary(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_arithmetic_binary(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_comparison_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_between_predicate(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_logical_binary_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_not_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_is_None_predicate(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_is_not_None_predicate(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_None_if_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_if_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_searched_case_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_simple_case_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_when_clause(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_coalesce_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_in_list_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_list_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_function_call(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_lambda_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_like_predicate(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_in_predicate(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_subquery_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_literal(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_array_constructor(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_subscript_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_qualified_name_reference(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_dereference_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_extract(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_current_time(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_cast(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_try_expression(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)

    def rewrite_at_time_zone(self, node, context, tree_rewriter):
        return self.rewrite_expression(node, context, tree_rewriter)


class ExpressionTreeRewriter(object):
    def __init__(self, line=None, pos=None, rewriter=None, visitor=None):
        self.line = line
        self.pos = pos
        self.rewriter = rewriter
        self.visitor = visitor


"""
    _class Rewriting_visitor(AstVisitor):

        def visit_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            throw new UnsupportedOperationException("not yet implemented: " + get_class().get_simple_name() + " for " + node.class.name)


        def visit_row(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_row(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Immutable_list.Builder<Expression> builder = Immutable_list.builder()
            for expression in node.items:
                builder.add(rewrite_rule(expression, context.get()))

            if !same_elements(node.items, builder.build()):
                return Row(builder.build())

            return node


        def visit_arithmetic_unary(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_arithmetic_unary(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            child = self.rewrite_rule(node.value, context.get())
            if child != node.value:
                return ArithmeticUnaryExpression(node.sign, child)

            return node


        def visit_arithmetic_binary(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_arithmetic_binary(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            left = self.rewrite_rule(node.left, context.get())
            right = self.rewrite_rule(node.right, context.get())

            if left != node.left || right != node.right:
                return ArithmeticBinaryExpression(node.type, left, right)

            return node


        def visit_array_constructor(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_array_constructor(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Immutable_list.Builder<Expression> builder = Immutable_list.builder()
            for expression in node.values:
                builder.add(rewrite_rule(expression, context.get()))

            if !same_elements(node.values, builder.build()):
                return ArrayConstructor(builder.build())

            return node


        def visit_at_time_zone(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_at_time_zone(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())
            Expression time_zone = rewrite_rule(node.get_time_zone(), context.get())

            if value != node.value || time_zone != node.get_time_zone():
                return AtTimeZone(value, time_zone)

            return node


        def visit_subscript_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_subscript_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            base = self.rewrite_rule(node.base, context.get())
            index = self.rewrite_rule(node.index, context.get())

            if base != node.base || index != node.index:
                return SubscriptExpression(base, index)

            return node


        def visit_comparison_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_comparison_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            left = self.rewrite_rule(node.left, context.get())
            right = self.rewrite_rule(node.right, context.get())

            if left != node.left || right != node.right:
                return ComparisonExpression(node.type, left, right)

            return node


        def visit_between_predicate(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_between_predicate(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())
            min = self.rewrite_rule(node.min, context.get())
            max = self.rewrite_rule(node.max, context.get())

            if value != node.value || min != node.min || max != node.max:
                return BetweenPredicate(value, min, max)

            return node


        def visit_logical_binary_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_logical_binary_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            left = self.rewrite_rule(node.left, context.get())
            right = self.rewrite_rule(node.right, context.get())

            if left != node.left || right != node.right:
                return LogicalBinaryExpression(node.type, left, right)

            return node


        def visit_not_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_not_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())

            if value != node.value:
                return NotExpression(value)

            return node


        def visit_is_None_predicate(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_is_None_predicate(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())

            if value != node.value:
                return Is_None_predicate(value)

            return node


        def visit_is_not_None_predicate(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_is_not_None_predicate(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())

            if value != node.value:
                return IsNot_None_predicate(value)

            return node


        def visit_None_if_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_None_if_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            first = self.rewrite_rule(node.first, context.get())
            second = self.rewrite_rule(node.second, context.get())

            if first != node.first || second != node.second:
                return NullIfExpression(first, second)

            return node


        def visit_if_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_if_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            condition = self.rewrite_rule(node.condition, context.get())
            Expression True_value = rewrite_rule(node.get_true_value(), context.get())
            Expression False_value = None
            if node.get_false_value().is_present():
                False_value = rewrite_rule(node.get_false_value().get(), context.get())

            if (condition != node.condition) || (true_value != node.get_true_value()) || (false_value != node.get_false_value().or_else(None)):
                return IfExpression(condition, True_value, False_value)

            return node


        def visit_searched_case_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_searched_case_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Immutable_list.Builder<When_clause> builder = Immutable_list.builder()
            for (When_clause expression : node.get_when_clauses():
                builder.add(rewrite_rule(expression, context.get()))

            Optional<Expression> default_value = node.get_default_value()
                    .map(value -> rewrite_rule(value, context.get()))

            if !same_elements(node.get_default_value(), default_value) || !same_elements(node.get_when_clauses(), builder.build())):
                return SearchedCaseExpression(builder.build(), default_value)

            return node


        def visit_simple_case_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_simple_case_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            operand = self.rewrite_rule(node.operand, context.get())

            Immutable_list.Builder<When_clause> builder = Immutable_list.builder()
            for (When_clause expression : node.get_when_clauses():
                builder.add(rewrite_rule(expression, context.get()))

            Optional<Expression> default_value = node.get_default_value()
                    .map(value -> rewrite_rule(value, context.get()))

            if operand != node.operand ||
                    !same_elements(node.get_default_value(), default_value) ||
                    !same_elements(node.get_when_clauses(), builder.build()):
                return SimpleCaseExpression(operand, builder.build(), default_value)

            return node


        def visit_when_clause(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_when_clause(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            operand = self.rewrite_rule(node.operand, context.get())
            result = self.rewrite_rule(node.result, context.get())

            if operand != node.operand || result != node.result:
                return WhenClause(operand, result)
            return node


        def visit_coalesce_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_coalesce_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Immutable_list.Builder<Expression> builder = Immutable_list.builder()
            for expression in node.operands:
                builder.add(rewrite_rule(expression, context.get()))

            if !same_elements(node.operands, builder.build()):
                return CoalesceExpression(builder.build())

            return node


        def visit_try_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_try_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            expression = self.rewrite_rule(node.get_inner_expression(), context.get())

            if node.get_inner_expression() != expression:
                return TryExpression(expression)

            return node


        def visit_function_call(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_function_call(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Optional<Window> rewritten_window = node.window
            if node.window.is_present():
                Window window = node.window.get()

                Immutable_list.Builder<Expression> partition_by = Immutable_list.builder()
                for expression in window.get_partition_by():
                    partition_by.add(rewrite_rule(expression, context.get()))

                // Since Sort_item is not an Expression, but contains Expressions, just process the default rewrite_rule inline with Function_call
                Immutable_list.Builder<Sort_item> order_by = Immutable_list.builder()
                for (Sort_item sort_item : window.get_order_by():
                    Expression sort_key = rewrite_rule(sort_item.get_sort_key(), context.get())
                    if sort_item.get_sort_key() != sort_key:
                        order_by.add(new SortItem(sort_key, sort_item.ordering, sort_item.get_None_ordering()))
                    else:
                        order_by.add(sort_item)

                Optional<Window_frame> rewritten_frame = window.frame
                if rewritten_frame.is_present():
                    Window_frame frame = rewritten_frame.get()

                    Frame_bound start = frame.start
                    if start.value.is_present():
                        value = self.rewrite_rule(start.value.get(), context.get())
                        if value != start.value.get():
                            start = new FrameBound(start.type, value)

                    Optional<Frame_bound> rewritten_end = frame.end
                    if rewritten_end.is_present():
                        Optional<Expression> value = rewritten_end.get().value
                        if value.is_present():
                            Expression rewritten_value = rewrite_rule(value.get(), context.get())
                            if rewritten_value != value.get():
                                rewritten_end = Optional.of(new FrameBound(rewritten_end.get().type, rewritten_value))

                    if (frame.start != start) || !same_elements(frame.end, rewritten_end):
                        rewritten_frame = Optional.of(new WindowFrame(frame.type, start, rewritten_end))

                if !same_elements(window.get_partition_by(), partition_by.build()) ||
                        !same_elements(window.get_order_by(), order_by.build()) ||
                        !same_elements(window.frame, rewritten_frame):
                    rewritten_window = Optional.of(new Window(partition_by.build(), order_by.build(), rewritten_frame))

            Immutable_list.Builder<Expression> arguments = Immutable_list.builder()
            for expression in node.arguments:
                arguments.add(rewrite_rule(expression, context.get()))

            if !same_elements(node.arguments, arguments.build()) || !same_elements(rewritten_window, node.window):
                return FunctionCall(node.name, rewritten_window, node.is_distinct(), arguments.build())

            return node


        def visit_lambda_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_lambda_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            body = self.rewrite_rule(node.body, context.get())
            if body != node.body:
                return LambdaExpression(node.arguments, body)

            return node


        def visit_like_predicate(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_like_predicate(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())
            pattern = self.rewrite_rule(node.pattern, context.get())
            Expression escape = None
            if node.escape is not None:
                escape = rewrite_rule(node.escape, context.get())

            if value != node.value || pattern != node.pattern || escape != node.escape:
                return LikePredicate(value, pattern, escape)

            return node


        def visit_in_predicate(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_in_predicate(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            value = self.rewrite_rule(node.value, context.get())
            list = self.rewrite_rule(node.get_value_list(), context.get())

            if node.value != value || node.get_value_list() != list:
                return InPredicate(value, list)

            return node


        def visit_in_list_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_in_list_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            Immutable_list.Builder<Expression> builder = Immutable_list.builder()
            for expression in node.values:
                builder.add(rewrite_rule(expression, context.get()))

            if !same_elements(node.values, builder.build()):
                return InListExpression(builder.build())

            return node


        def visit_subquery_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_subquery_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            // No default rewrite_rule for Subquery_expression since we do not want to traverse subqueries
            return node


        def visit_literal(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_literal(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            return node


        def visit_qualified_name_reference(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_qualified_name_reference(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            return node


        def visit_dereference_expression(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_dereference_expression(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            base = self.rewrite_rule(node.base, context.get())
            if base != node.base:
                return DereferenceExpression(base, node.get_field_name())

            return node


        def visit_extract(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_extract(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            expression = self.rewrite_rule(node.expression, context.get())

            if node.expression != expression:
                return Extract(expression, node.field)

            return node


        def visit_current_time(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_current_time(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            return node


        def visit_cast(self, node, context):
            if !context.is_default_rewrite()):
                result = self.rewriter.rewrite_cast(node, context.get(), Expression_tree_rewriter.this)
                if result is not None:
                    return result

            expression = self.rewrite_rule(node.expression, context.get())

            if node.expression != expression:
                return Cast(expression, node.type, node.is_safe(), node.is_type_only())

            return node

    class Context<C>
        _default_rewrite
        _context

        _Context(context, default_rewrite)
            this.context = context
            this.default_rewrite = default_rewrite

        C get()
            return context

        is_default_rewrite()
            return default_rewrite

    _same_elements(Optional<T> a, Optional<T> b)
        if !a.is_present() && !b.is_present():
            return True
        else if a.is_present() != b.is_present():
            return False

        return a.get() == b.get()

    @Suppress_warnings("Object_equality")
    _same_elements(Iterable<? extends T> a, Iterable<? extends T> b)
        if Iterables.size(a) != Iterables.size(b):
            return False

        Iterator<? extends T> first = a.iterator()
        Iterator<? extends T> second = b.iterator()

        while (first.has_next() && second.has_next():
            if first.next() != second.next():
                return False

        return True


"""
