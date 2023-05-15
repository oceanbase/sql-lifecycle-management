# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .full_scan_rule import FullScanRule
from .normal_index_without_index_back import NormalIndexWithoutIndexBackRule
from .unique_index_with_index_back import UniqueIndexWithIndexBackRule

heuristic_rule_list = [
    FullScanRule(),
    NormalIndexWithoutIndexBackRule(),
    UniqueIndexWithIndexBackRule()
]
