# Copyright 2021-2026 SingleStore, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MY_SEED = """
id,value,record_valid_date
1,100,2023-01-01 00:00:00
2,200,2023-01-02 00:00:00
3,300,2023-01-02 00:00:00
""".strip()


MY_TABLE = """
{{ config(
    materialized='table',
) }}
select *
from {{ ref('my_seed') }}
"""


MY_VIEW = """
{{ config(
    materialized='view',
) }}
select *
from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
) }}
select *
from {{ ref('my_seed') }}
"""
