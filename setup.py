#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-singlestore"
# make sure this always matches dbt/adapters/singlestore/__version__.py
package_version = "1.5.0"
description = """The singlestore adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="SingleStore Inc.",
    author_email="support@singlestore.com",
    url="https://github.com/memsql/dbt-singlestore",
    license="Apache License 2.0",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.0.0",
        "singlestoredb==0.8.1",
        "dataclasses_json>=0.5.6"
    ]
)
