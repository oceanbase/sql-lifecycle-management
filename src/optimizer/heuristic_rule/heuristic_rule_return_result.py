# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class HeuristicRuleReturnResult(object):

    def __init__(self, index_name, index_column_list, rule, message):
        self.index_name = index_name
        self.index_column_list = index_column_list
        self.rule = rule
        self.message = message
