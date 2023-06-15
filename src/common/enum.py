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
class AnalysisFileTypeEunm(Enum):
    XML = 'xml'
    SLOW_LOG = 'slow_log'


@unique
class OptimizationTypeEunm(Enum):
    OPTIMIZE = 'optimize'
    ANALYSIS = 'analysis'
    REVIEW = 'review'


@unique
class ApproveScopeEunm(Enum):
    SQL = 'sql'
    PLAN = 'plan'
    SCHEMA = 'schema'
    STATISTICS = 'statistics'
