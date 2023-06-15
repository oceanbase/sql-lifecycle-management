# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""


class CBO:
    def __init__(self, candidate_index_list, table_list, order_list, limit_number, statistics):
        self.candidate_index_list = candidate_index_list
        self.table_list = table_list
        self.order_list = order_list
        self.limit_number = limit_number
        self.statistics = statistics
