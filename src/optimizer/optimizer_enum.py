# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from enum import Enum, unique


@unique
class SQLOperator(Enum):
    # TODO(tingkai.ztk):后续要把SQL_PREDICATE重构到枚举中
    LIMIT = 'limit'
    ORDER = 'order'
    ORDER_BY = 'order by'
    ORDER_BY_LIMIT = 'order_by_limit'


@unique
class IndexType(Enum):
    PRIMARY = '1.primary'
    UNIQUE = '2.unique'
    NORMAL = '3.normal'


@unique
class OptType(Enum):
    """
    Operator type, used to determine the calculation formula of cost
    Operators of the same category have the same cost calculation formula
    The value here indicates that when the same variable is used, whose range is better, the larger the better value is
    For example, a > ? and a = ? = is better than in,
    where the cost calculation of a needs to be calculated according to =
    """
    EQUAL = 7
    IN = 6
    # a > ? and a < ?
    CLOSED_RANGE = 5
    # a >= and a < 、 a > and a <=
    CLOSED_RANGE_HALF_EQUAL = 4
    # >= and <=、like、between
    CLOSED_RANGE_EQUAL_ALL = 3
    # > 、 <
    HALF_OPEN_RANGE = 2
    # >= 、 <=
    HALF_OPEN_RANGE_EQUAL = 1
    # !=、Since the count of in is not calculated when not in, it is temporarily classified with !=
    NOT_EQUAL = 0
    # others
    OTHER = -1
    UNKNOW = -999999
