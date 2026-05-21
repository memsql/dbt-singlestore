#!/usr/bin/env python
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

from pathlib import Path
from setuptools import find_namespace_packages, setup

package_name = "dbt-singlestore"
# make sure this always matches dbt/adapters/singlestore/__version__.py
package_version = "1.10.0"
description = """The SingleStore adapter plugin for dbt"""

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="SingleStore Inc.",
    author_email="support@singlestore.com",
    url="https://github.com/memsql/dbt-singlestore",
    license="Apache License 2.0",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-adapters>=1.0.0, <2.0.0",
        "dbt-core>=1.8",
        "singlestoredb==1.15.8",
        "dataclasses_json>=0.5.6"
    ],
    entry_points={
        "dbt.adapters": [
            "singlestore = dbt.adapters.singlestore:Plugin",
        ],
    },
)
