from .node import Node

UNSPECIFIEDLENGTH = -1


class SQLType:
    BINARY = 1
    CHAR = 2
    DATE = 3
    YEAR = 4
    DATETIME = 5
    DECIMAL = 6
    TIME = 7
    INTEGER = 8
    JSON = 9
    DOUBLE = 10
    FLOAT = 11
    REAL = 12


class FieldType(Node):
    def __init__(
        self,
        line=None,
        pos=None,
    ) -> None:
        super(FieldType, self).__init__(line, pos)

    def set_tp(self, tp, type_name):
        self.tp = tp
        self.type_name = type_name

    def set_length(self, length):
        self.length = length

    def set_decimal(self, decimal):
        self.decimal = decimal

    def set_flag(self, flag):
        self.flag = flag

    def set_is_signed(self, is_signed):
        self.is_signed = is_signed

    def set_charset_and_collation(self, charset_and_collation):
        self.charset_and_collation = charset_and_collation

    def accept(self, visitor, context):
        return visitor.visit_field_type(self, context)

    def __str__(self):
        result = ""
        if self.type_name != None:
            result = self.type_name

        if (
            self.tp is SQLType.BINARY
            or self.tp is SQLType.CHAR
            or self.tp is SQLType.TIME
            or self.tp is SQLType.DATETIME
        ):
            if "length" in dir(FieldType) and self.length != UNSPECIFIEDLENGTH:
                result += f" ({self.length})"

        if self.tp is SQLType.CHAR:
            if (
                "charset_and_collation" in dir(FieldType)
                and self.charset_and_collation != None
            ):
                result += f" ({self.charset_and_collation})"

        if self.tp is SQLType.INTEGER:
            result = "SIGNED " + result if self.is_signed else "UNSIGNED " + result

        if self.tp is SQLType.FLOAT or self.tp is SQLType.DECIMAL:
            if (
                "length" in dir(FieldType)
                and self.length != UNSPECIFIEDLENGTH
                and "decimal" in dir(FieldType)
                and self.decimal != UNSPECIFIEDLENGTH
            ):
                result += f" ({self.length},{self.decimal})"
            elif "length" in dir(FieldType) and self.length != UNSPECIFIEDLENGTH:
                result += f" ({self.length})"

        return result
