# coding=utf-8
"""

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
from .rewrite_supplement_column_rule import RewriteSupplementColumnRule
from .rewrite_or_rule import RewriteMySQLORRule
from .remove_order_by_in_delete_update_rule import RemoveOrderByInDeleteUpdateRule

common_rules = [RewriteSupplementColumnRule(),
                RemoveOrderByInDeleteUpdateRule()]

mysql_rules = [RewriteMySQLORRule()]
