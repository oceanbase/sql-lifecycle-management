# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


"""
This module contains classes related to the representation, processing, and manipulation of tree structures.
"""

"""
The following are intended to manipulate, validate, and match specific queries
"""
# from .approximate import Approximate
from .query_specification import QuerySpecification
from .rewrite import ExpressionRewriter, ExpressionTreeRewriter
from .visitor import AstVisitor, DefaultTraversalVisitor, DefaultExpressionTraversalVisitor

"""
The following classes are intended for the representation of a query
"""

"""
Lots of classes in the literal and statement modules...
"""
from .literal import *
from .statement import *
from .expression import *

from .call_argument import CallArgument
from .explain import ExplainFormat, ExplainType, ExplainOption
from .grouping import GroupingElement, GroupingSets, SimpleGroupBy
from .join_criteria import JoinCriteria, JoinOn, JoinUsing, NaturalJoin
from .set_operation import SetOperation, Except, Intersect, Union
from .qualified_name import QualifiedName
from .relation import Relation, AliasedRelation, Join, QueryBody  # , Unnest
from .select import Select
from .select_item import SelectItem, AllColumns, SingleColumn
from .sort_item import SortItem
from .table import Table, TableSubquery
from .table_element import TableElement
from .transaction import TransactionMode, Isolation, TransactionAccessMode
from .values import Values
# from .window import Window, WindowFrame, FrameBound
