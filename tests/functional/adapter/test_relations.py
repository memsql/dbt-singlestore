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

from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator
from dbt.tests.adapter.relations.test_dropping_schema_named import BaseDropSchemaNamed


class TestChangeRelationTypes(BaseChangeRelationTypeValidator):
    pass


class TestDropSchemaNamed(BaseDropSchemaNamed):
    pass
