# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

from .pmd_arithmetic_rule import PMDArithmeticRule
from .pmd_count_rule import PMDCountRule
from .pmd_full_scan_rule import PMDFullScanRule
from .pmd_is_null_rule import PMDIsNullRule
from .pmd_multi_table import PMDMultiTableRule
from .pmd_nowait_wait_rule import PMDNowaitWaitRule
from .pmd_select_all_rule import PMDSelectAllRule
from .pmd_update_delete_multi_table_rule import PMDUpdateDeleteMultiTableRule

common_pmd_list = [
    PMDSelectAllRule(),
    PMDIsNullRule(),
    PMDCountRule(),
    PMDArithmeticRule(),
    PMDUpdateDeleteMultiTableRule(),
    PMDNowaitWaitRule(),
    PMDMultiTableRule(),
    # This is a damaging rule and must be placed last
    PMDFullScanRule()
]
