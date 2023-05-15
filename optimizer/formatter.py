# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from parser.parser_utils import FIELD_REFERENCE_PREFIX
from parser.tree import *


class Formatter(AstVisitor):

    def visit_row(self, row, unmangle=True):
        return "ROW (%s)" % ", ".join([self.process(item, unmangle) for item in row.items])

    def visit_expression(self, node, unmangle_names):
        raise NotImplementedError(
            "not implemented: %s.visit%s" % (self.__class__.__name__, node.__class__.__name__))

    def visit_at_time_zone(self, node, context):
        return "%s AT TIME ZONE %s" \
               % (self.process(node.value, context), self.process(node.time_zone, context))

    def visit_current_time(self, node, unmangle_names):
        return "%s%s" % (node.type, "(%s)" % node.precision if node.precision else "")

    def visit_extract(self, node, unmangle_names):
        return "EXTRACT(%s FROM %s )" % (node.field, self.process(node.expression, unmangle_names))

    def visit_boolean_literal(self, node, unmangle_names):
        return str(node.value)

    def visit_string_literal(self, node, unmangle_names):
        return format_string_literal(node.value)

    def visit_subscript_expression(self, node, unmangle_names):
        return format_sql(node.base, unmangle_names) \
               + "[%s]" % format_sql(node.index, unmangle_names)

    def visit_long_literal(self, node, unmangle_names):
        return str(node.value)

    def visit_double_literal(self, node, unmangle_names):
        return str(node.value)

    def visit_time_literal(self, node, unmangle_names):
        return "TIME '%s'" % node.value

    def visit_timestamp_literal(self, node, unmangle_names):
        return "TIMESTAMP '%s'" % node.value

    def visit_null_literal(self, node, unmangle_names):
        return "null"

    def visit_interval_literal(self, node, unmangle_names):
        sign = "-" if node.sign == "-" else ""
        interval = "INTERVAL " + sign + " '" + node.value + "' " + node.start_field

        if node.end_field:
            interval += " TO " + node.end_field
        return interval

    def visit_subquery_expression(self, node, unmangle_names):
        return "(" + format_sql(node.query, unmangle_names) + ")"

    def visit_exists(self, node, unmangle_names):
        return "EXISTS (" + format_sql(node.subquery, unmangle_names) + ")"

    def visit_qualified_name_reference(self, node, unmangle_names):
        return _format_qualified_name(node.name)

    def visit_dereference_expression(self, node, unmangle_names):
        base_string = self.process(node.base, unmangle_names)
        return base_string + "." + _format_identifier(node.field_name)

    def visit_function_call(self, node, unmangle_names):
        ret = ""
        arguments = self._join_expressions(node.arguments, unmangle_names)
        if not node.arguments and "count" == node.name.parts[0].lower():
            arguments = "*"
        if node.distinct:
            arguments = "DISTINCT " + arguments

        if unmangle_names and str(node.name).startswith(FIELD_REFERENCE_PREFIX):
            raise NotImplementedError("Mangled names not supported right now")
        else:
            ret += _format_qualified_name(node.name) + '(' + arguments + ')'

        if node.window:
            ret += " OVER " + self.visit_window(node.window, unmangle_names)

        return ret

    def visit_logical_binary_expression(self, node, unmangle_names):
        return self._format_binary_expression(node.type, node.left, node.right, unmangle_names)

    def visit_not_expression(self, node, unmangle_names):
        return self.process(node.value.value, unmangle_names) + " NOT IN " + self.process(node.value.value_list, unmangle_names)

    def visit_comparison_expression(self, node, unmangle_names):
        return self._format_binary_expression(node.type, node.left, node.right, unmangle_names)

    def visit_is_null_predicate(self, node, unmangle_names):
        return "(" + self.process(node.value, unmangle_names) + " IS NULL)"

    def visit_is_not_null_predicate(self, node, unmangle_names):
        return "(" + self.process(node.value, unmangle_names) + " IS NOT NULL)"

    def visit_none_if_expression(self, node, unmangle_names):
        return "NULLIF(%s, %s)" % (self.process(node.first, unmangle_names),
                                   self.process(node.second, unmangle_names))

    def visit_if_expression(self, node, unmangle_names):
        ret = "IF(" + self.process(node.condition, unmangle_names)
        ret += ", " + self.process(node.true_value, unmangle_names) if node.true_value else ""
        if node.false_value:
            ret += ", " + self.process(node.false_value, unmangle_names)
        ret += ")"
        return ret

    def visit_try_expression(self, node, unmangle_names):
        return "TRY(" + self.process(node.inner_expression, unmangle_names) + ")"

    def visit_coalesce_expression(self, node, unmangle_names):
        return "COALESCE(" + self._join_expressions(node.operands, unmangle_names) + ")"

    def visit_arithmetic_unary(self, node, unmangle_names):
        value = self.process(node.value, unmangle_names)

        if node.sign == "=":
            # this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
            separator = " " if node.value.startswith("-") else ""
            return "-" + separator + value
        return "+" + value

    def visit_arithmetic_binary(self, node, unmangle_names):
        return self._format_binary_expression(node.type, node.left, node.right, unmangle_names)

    def visit_like_predicate(self, node, unmangle_names):
        ret = ""

        ret += self.process(node.value, unmangle_names) + \
               " LIKE " + self.process(node.pattern, unmangle_names)

        if node.escape is not None:
            ret += " ESCAPE " + self.process(node.escape, unmangle_names)

        return ret

    def visit_all_columns(self, node, unmangle_names):
        if node.prefix:
            return node.prefix + ".*"
        return "*"

    def visit_cast(self, node, unmangle_names):
        return ("TRY_CAST" if node.safe else "CAST") + \
               "(" + self.process(node.expression, unmangle_names) + " AS " + node.data_type + ")"

    def visit_searched_case_expression(self, node, unmangle_names):
        parts = ["CASE"]
        for when_clause in node.when_clauses:
            parts.append(self.process(when_clause, unmangle_names))

        if node.default_value:
            parts.append("ELSE")
            parts.append(self.process(node.default_value, unmangle_names))

        parts.append("END")

        return "(" + ' '.join(parts) + ")"

    def visit_simple_case_expression(self, node, unmangle_names):
        parts = ["CASE", self.process(node.operand, unmangle_names)]

        for when_clause in node.when_clauses:
            parts.append(self.process(when_clause, unmangle_names))

        if node.default_value:
            parts.append("ELSE")
            parts.append(self.process(node.default_value, unmangle_names))

        parts.append("END")

        return "(" + ' '.join(parts) + ")"

    def visit_when_clause(self, node, unmangle_names):
        return "WHEN %s THEN %s" % (self.process(node.operand, unmangle_names),
                                    self.process(node.result, unmangle_names))

    def visit_between_predicate(self, node, unmangle_names):
        return "(%s BETWEEN %s AND %s)" % (self.process(node.value, unmangle_names),
                                           self.process(node.min, unmangle_names),
                                           self.process(node.max, unmangle_names))

    def visit_in_predicate(self, node, unmangle_names):
        return "%s IN %s" % (self.process(node.value, unmangle_names),
                             self.process(node.value_list, unmangle_names))

    def visit_in_list_expression(self, node, unmangle_names):
        return "(%s)" % self._join_expressions(node.values, unmangle_names)

    def visit_window(self, node, unmangle_names):
        parts = []

        if node.partition_by:
            parts.append(
                "PARTITION BY " + self._join_expressions(node.partition_by, unmangle_names))
        if node.order_by:
            parts.append("ORDER BY " + format_sort_items(node.order_by, unmangle_names))
        if node.frame:
            parts.append(self.process(node.frame, unmangle_names))

        return '(' + ' '.join(parts) + ')'

    def visit_window_frame(self, node, unmangle_names):
        ret = node.type + " "

        if node.end:
            ret += "BETWEEN %s AND %s" % (self.process(node.start, unmangle_names),
                                          self.process(node.end, unmangle_names))
        else:
            ret += self.process(node.start, unmangle_names)

        return ret

    def _format_binary_expression(self, operator, left, right, unmangle_names):
        return "%s %s %s" % (self.process(left, unmangle_names),
                             operator,
                             self.process(right, unmangle_names))

    def _join_expressions(self, expressions, unmangle_names):
        return ", ".join([self.process(e, unmangle_names) for e in expressions])


# noinspection SqlNoDataSourceInspection
class SqlFormatter(AstVisitor):
    INDENT = "   "

    def __init__(self, builder, unmangle_names):
        super(SqlFormatter, self).__init__()
        self.builder = builder
        self._unmangle_names = unmangle_names

    def visit_node(self, node, indent):
        raise NotImplementedError("not yet implemented: " + node)

    def visit_expression(self, node, indent):
        if indent != 0:
            raise ValueError("visit_expression should only be called at root")
        self.builder.append(format_expression(node, self._unmangle_names))
        return None

    def visit_unnest(self, node, indent):
        self.builder.append(node)
        return None

    def visit_query(self, node, indent):
        if node.with_:
            with_ = node.with_
            self._append(indent, "WITH")
            if with_.recursive:
                self.builder.append(" RECURSIVE")
                self.builder.append("\n  ")
            queries = with_.queries
            for query in queries:
                self._append(indent, query.name)
                append_alias_columns(self.builder, query.column_names)
                self.builder.append(" AS ")
                self.process(TableSubquery(query=query.query), indent)
                self.builder.append('\n')
                if queries.has_next():
                    self.builder.append(", ")

        self._process_relation(node.query_body, indent)

        if node.order_by:
            self._append(indent, "ORDER BY " + format_sort_items(node.order_by))
            self.builder.append('\n')

        if node.limit:
            self._append(indent, "LIMIT " + str(node.limit))
            self.builder.append('\n')

        if node.offset:
            self._append(indent, "OFFSET " + str(node.limit))
            self.builder.append('\n')

        return None

    def visit_query_specification(self, node, indent):
        self.process(node.select, indent)

        if node.from_:
            self._append(indent, "FROM")
            self.builder.append('\n')
            self._append(indent, "  ")
            self.process(node.from_, indent)

        self.builder.append('\n')

        if node.where:
            self._append(indent, "WHERE " + format_expression(node.where))
            self.builder.append('\n')

        if node.group_by:
            self._append(indent, "GROUP BY " + format_group_by(node.group_by))
            self.builder.append('\n')

        if node.having:
            self._append(indent, "HAVING " + format_expression(node.having))
            self.builder.append('\n')

        if node.order_by:
            self._append(indent, "ORDER BY " + format_sort_items(node.order_by))
            self.builder.append('\n')

        if node.limit:
            if str(node.limit).isnumeric():
                self._append(indent, "LIMIT %d" % int(node.limit))
                self.builder.append('\n')
            else:
                self._append(indent, "LIMIT %s" % node.limit)
                self.builder.append('\n')
        return None

    def visit_select(self, node, indent):
        self._append(indent, "SELECT")
        if node.distinct:
            self.builder.append(" DISTINCT")

        if len(node.select_items) > 1:
            first = True
            for item in node.select_items:
                self.builder.append("\n")
                self.builder.append(_indent_string(indent))
                self.builder.append("  " if first else ", ")

                self.process(item, indent)
                first = False
        else:
            self.builder.append(' ')
            self.process(node.select_items[0], indent)

        self.builder.append('\n')
        return None

    def visit_single_column(self, node, indent):
        self.builder.append(format_expression(node.expression))

        if node.alias:
            self.builder.append(' AS ')
            self.builder.append(node.alias[1])
        return None

    def visit_all_columns(self, node, context):
        self.builder.append(str(node))
        return None

    def visit_table(self, node, indent):
        self.builder.append(str(node.name))
        return None

    def visit_join(self, node, indent):
        criteria = node.criteria
        join_type = node.join_type
        if isinstance(criteria, NaturalJoin):
            join_type = "NATURAL " + join_type

        if node.join_type != "IMPLICIT":
            self.builder.append('(')
        self.process(node.left, indent)

        self.builder.append('\n')
        if node.join_type == "IMPLICIT":
            self._append(indent, ", ")
        else:
            self._append(indent, join_type)
            self.builder.append(" JOIN ")

        self.process(node.right, indent)

        if node.join_type not in ("CROSS", "IMPLICIT"):
            if isinstance(criteria, JoinUsing):
                self.builder.append(" USING (")
                self.builder.append(", ".join(node.columns))
                self.builder.append(")")
            elif isinstance(criteria, JoinOn):
                self.builder.append(" ON (")
                self.builder.append(format_expression(node.criteria.expression))
                self.builder.append(")")
            elif not isinstance(criteria, NaturalJoin):
                raise ValueError("unknown join criteria: " + criteria)

        if node.join_type != "IMPLICIT":
            self.builder.append(")")

        return None

    def visit_aliased_relation(self, node, indent):
        self.process(node.relation, indent)

        self.builder.append(' ')
        if node.alias[0]:
            self.builder.append('AS')
            self.builder.append(' ')
        self.builder.append(node.alias[1])

        append_alias_columns(self.builder, node.column_names)

        return None

    def visit_sampled_relation(self, node, indent):
        self.process(node.relation, indent)

        self.builder.append(" TABLESAMPLE ")
        self.builder.append(node.type)
        self.builder.append(" (")
        self.builder.append(node.sample_percentage)
        self.builder.append(')')

        if node.columns_to_stratify_on:
            self.builder.append(" STRATIFY ON (")
            self.builder.append(",".join(node.columns_to_stratify_on))
            self.builder.append(')')

        return None

    def visit_values(self, node, indent):
        self.builder.append(" VALUES ")

        first = True
        for row in node.rows:
            self.builder.append("\n")
            self.builder.append(_indent_string(indent))
            self.builder.append("  " if first else ", ")

            self.builder.append(format_expression(row))
            first = False
        self.builder.append('\n')

        return None

    def visit_table_subquery(self, node, indent):
        self.builder.append('(\n')
        self.process(node.query, indent + 1)
        self._append(indent, ") ")

        return None

    def visit_union(self, node, indent):
        all = node.all
        for (i, relation) in enumerate(node.relations):
            self._process_relation(relation, indent)
            if i != len(node.relations) - 1:
                if all:
                    self.builder.append("UNION " + "ALL ")
                else:
                    self.builder.append("UNION ")

        return None

    def visit_except(self, node, indent):
        self._process_relation(node.left, indent)
        self.builder.append("EXCEPT " + "ALL " if not node.distinct else "")
        self._process_relation(node.right, indent)

        return None

    def visit_subquery_expression(self, node, indent):
        self._append(indent,"(" + format_sql(node.query) + ")")
        return None

    def visit_update(self, node, indent):
        self.builder.append("UPDATE ")
        table = node.table
        where = node.where
        set_list = node.set_list
        if len(table) == 1:
            self.builder.append(table[0].name.parts[0])
        else:
            self._process_relation(table, indent)

        if set_list:
            self.builder.append(" SET ")
            for (i, _set) in enumerate(set_list):
                self.builder.append(format_expression(_set))
                if i != len(set_list) - 1:
                    self.builder.append(" , ")
                else:
                    self.builder.append(" ")

        if where:
            self._append(indent, "WHERE " + format_expression(node.where))
            self.builder.append('\n')

        if node.order_by:
            self._append(indent, "ORDER BY " + format_sort_items(node.order_by))
            self.builder.append('\n')

        if node.limit:
            self._append(indent, "LIMIT " + str(node.limit))
            self.builder.append('\n')

        if node.offset:
            self._append(indent, "OFFSET " + str(node.limit))
            self.builder.append('\n')

        return None

    def visit_intersect(self, node, indent):
        relations = [self._process_relation(relation, indent) for relation in node.relations]
        intersect = "INTERSECT " + "ALL " if not node.distinct else ""
        self.builder.append(intersect.join(relations))
        return None

    def visit_create_view(self, node, indent):
        self.builder.append("CREATE " + "OR REPLACE" if node.replace else "")
        self.builder.append("VIEW ")
        self.builder.append(node.name)
        self.builder.append(" AS\n")
        self.process(node.query, indent)

        return None

    def visit_drop_view(self, node, context):
        self.builder.append("DROP VIEW ")
        if node.exists:
            self.builder.append("IF EXISTS ")
        self.builder.append(node.name)

        return None

    def visit_explain(self, node, indent):
        self.builder.append("EXPLAIN ")
        if node.analyze:
            self.builder.append("ANALYZE ")

        options = []

        for option in node.options:
            if isinstance(option, ExplainType):
                options.append("TYPE " + option.type)
            elif isinstance(option, ExplainFormat):
                options.append("FORMAT " + option.type)
            else:
                raise ValueError("unhandled explain option: " + option)

        if options:
            self.builder.append("(")
            self.builder.append(", ".join(options))
            self.builder.append(")")

        self.builder.append("\n")

        self.process(node.statement, indent)

        return None

    def visit_show_catalogs(self, node, context):
        self.builder.append("SHOW CATALOGS")

        return None

    def visit_show_schemas(self, node, context):
        self.builder.append("SHOW SCHEMAS")

        if node.catalog:
            self.builder.append(" FROM ")
            self.builder.append(node.catalog)

        return None

    def visit_show_tables(self, node, context):
        self.builder.append("SHOW TABLES")

        if node.schema:
            self.builder.append(" FROM ")
            self.builder.append(node.schema)

        if node.like_pattern:
            self.builder.append(" LIKE ")
            self.builder.append(format_string_literal(node.like_pattern))

        return None

    def visit_show_columns(self, node, context):
        self.builder.append("SHOW COLUMNS FROM ")
        self.builder.append(node.table)
        return None

    def visit_show_partitions(self, node, context):
        self.builder.append("SHOW PARTITIONS FROM ")
        self.builder.append(node.table)
        return None

    def visit_show_functions(self, node, context):
        self.builder.append("SHOW FUNCTIONS")
        return None

    def visit_show_session(self, node, context):
        self.builder.append("SHOW SESSION")
        return None

    def visit_delete(self, node, indent):
        self.builder.append("DELETE FROM ")
        self._append(indent, node.table[0].name.parts[0])
        self.builder.append(" ")

        if node.where:
            self._append(indent, "WHERE " + format_expression(node.where))
            self.builder.append('\n')

        if node.order_by:
            self._append(indent, "ORDER BY " + format_sort_items(node.order_by))
            self.builder.append('\n')

        if node.limit:
            self._append(indent, "LIMIT " + str(node.limit))
            self.builder.append('\n')

        if node.offset:
            self._append(indent, "OFFSET " + str(node.limit))
            self.builder.append('\n')

        return None

    def visit_create_table_as_select(self, node, indent):
        self.builder.append("CREATE TABLE ")
        if node.not_exists:
            self.builder.append("IF NOT EXISTS ")
        self.builder.append(node.name)

        if node.properties:
            self.builder.append(" WITH (")
            self.builder.append(
                ", ".join(["%s = %s" % (k, v) for k, v in node.properties.items()]))
            self.builder.append(")")

        self.builder.append(" AS ")
        self.process(node.query, indent)

        if not node.with_data:
            self.builder.append(" WITH NO DATA")

        return None

    def visit_create_table(self, node, indent):
        self.builder.append("CREATE TABLE ")
        if node.not_exists:
            self.builder.append("IF NOT EXISTS ")
        self.builder.append(node.name)
        self.builder.append(" (")

        self.builder.append(", ".join(["%s %s" % (e.name, e.type) for e in node.elements]))

        self.builder.append(")")

        if node.properties:
            self.builder.append(" WITH (")
            self.builder.append(
                ", ".join(["%s = %s" % (k, v) for k, v in node.properties.items()]))
            self.builder.append(")")

        return None

    def visit_drop_table(self, node, context):
        self.builder.append("DROP TABLE ")
        if node.exists:
            self.builder.append("IF EXISTS ")
        self.builder.append(node.table_name)

        return None

    def visit_rename_table(self, node, context):
        self.builder.append("ALTER TABLE ")
        self.builder.append(node.source)
        self.builder.append(" RENAME TO ")
        self.builder.append(node.target)

        return None

    def visit_rename_column(self, node, context):
        self.builder.append("ALTER TABLE ")
        self.builder.append(node.table)
        self.builder.append(" RENAME COLUMN ")
        self.builder.append(node.source)
        self.builder.append(" TO ")
        self.builder.append(node.target)

        return None

    def visit_add_column(self, node, indent):
        self.builder.append("ALTER TABLE ")
        self.builder.append(node.name)
        self.builder.append(" ADD COLUMN ")
        self.builder.append(node.column.name)
        self.builder.append(" ")
        self.builder.append(node.column.type)

        return None

    def visit_insert(self, node, indent):
        self.builder.append("INSERT INTO ")
        self.builder.append(node.target)
        self.builder.append(" ")

        if node.columns:
            self.builder.append("(")
            self.builder.append(", ".join(node.columns))
            self.builder.append(") ")

        self.process(node.query, indent)

        return None

    def visit_set_session(self, node, context):
        self.builder.append("SET SESSION ")
        self.builder.append(node.name)
        self.builder.append(" = ")
        self.builder.append(format_expression(node.value))

        return None

    def visit_reset_session(self, node, context):
        self.builder.append("RESET SESSION ")
        self.builder.append(node.name)

        return None

    def visit_call_argument(self, node, indent):
        if node.name:
            self.builder.append(node.name)
            self.builder.append(" => ")
        self.builder.append(format_expression(node.value))

        return None

    def visit_call(self, node, indent):
        self.builder.append("CALL ")
        self.builder.append(node.name)
        self.builder.append("(")

        args = [self.process(arg, indent) for arg in node.arguments]
        self.builder.append(", ".join(args))
        self.builder.append(")")

        return None

    def visit_start_transaction(self, node, indent):
        self.builder.append("START TRANSACTION")

        if node.transaction_modes:
            self.builder.append(" ")
            modes = [self.process(mode, indent) for mode in node.transaction_modes]
            self.builder.append(", ".join(modes))
        return None

    def visit_isolation_level(self, node, indent):
        self.builder.append("ISOLATION LEVEL ").append(node.level.text)
        return None

    def visit_transaction_access_mode(self, node, context):
        self.builder.append("READ ONLY" if node.read_only else "READ WRITE")
        return None

    def visit_commit(self, node, context):
        self.builder.append("COMMIT")
        return None

    def visit_rollback(self, node, context):
        self.builder.append("ROLLBACK")
        return None

    def visit_grant(self, node, indent):
        self.builder.append("GRANT ")

        if node.privileges:
            self.builder.append(", ".join(node.privileges))
        else:
            self.builder.append("ALL PRIVILEGES")

        self.builder.append(" ON ")
        if node.table:
            self.builder.append("TABLE ")
        self.builder.append(node.table_name)
        self.builder.append(" TO ")
        self.builder.append(node.grantee)
        if node.with_grant_option:
            self.builder.append(" WITH GRANT OPTION")

        return None

    def _process_relation(self, relation, indent):
        if isinstance(relation, Table):
            self.builder.append("TABLE ").append(relation.name).append('\n')
        else:
            self.process(relation, indent)

    def _append(self, indent, value):
        self.builder.append(_indent_string(indent))
        self.builder.append(value)


"""
Convenience methods
"""


def format_expression(expression, unmangle_names=True):
    return Formatter().process(expression, unmangle_names)


def sort_item_formatter(sort_item, unmangle_names):
    builder = [format_expression(sort_item.sort_key, unmangle_names),
               " ASC" if sort_item.ordering.lower() == "asc" else " DESC"]
    return "".join(builder)


def _format_identifier(s):
    return s


def _format_qualified_name(name):
    parts = [_format_identifier(part) for part in name.parts]
    return '.'.join(parts)


def format_group_by(grouping_element):
    result_strings = []

    result = ""
    if isinstance(grouping_element, SimpleGroupBy):
        columns = grouping_element.columns
        for column in columns:
            result_strings.append(format_expression(column))
    return ", ".join(result_strings)


def format_string_literal(s):
    return "'" + s.replace("'", "''") + "'" if s != '?' else '?'


def _indent_string(indent):
    return SqlFormatter.INDENT * indent


def format_sort_items(sort_items, unmangle_names=True):
    return ", ".join([sort_item_formatter(sort_item, unmangle_names) for sort_item in sort_items])


def format_sql(root, unmangle_names=True):
    builder = []
    SqlFormatter(builder, unmangle_names).process(root, 0)
    return "".join(builder)


def append_alias_columns(builder, columns):
    if columns:
        builder.append(" (")
        builder.append(", ".join(columns))
        builder.append(')')
