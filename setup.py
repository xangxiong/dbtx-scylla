#!/usr/bin/env python
import os
import sys

if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)


from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)

package_name = "dbtx-scylla"
# For test build we should suffix it with -#, like: 1.7.5-1.
# This will generate a build version as 1.7.5.post1.
# When updating the package_version here, you must update the version in dbt.adapters.cockroachdb.__version__ as well
package_version = "1.7.0"
description = """The cockroachdb adapter plugin for dbt (data build tool)"""

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Xang Xiong",
    author_email="xang.xiong.email@gmail.com",
    url="https://github.com/xangxiong/dbtx-scylla",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~=1.7.0",
        "cassandra-driver~=3.29",
        # installed via dbt-core, but referenced directly, don't pin to avoid version conflicts with dbt-core
        "agate",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
