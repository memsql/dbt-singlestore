#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-singlestore"
# make sure this always matches dbt/adapters/singlestore/__version__.py
package_version = "1.8.2"
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
        "dbt-adapters>=1.0.0, <2.0.0",
        "dbt-core>=1.8",
        "singlestoredb==1.2.0",
        "dataclasses_json>=0.5.6"
    ]
)
