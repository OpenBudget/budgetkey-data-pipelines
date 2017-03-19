#!/usr/bin/env python
from setuptools import setup, find_packages
import os, time

if os.path.exists("VERSION.txt"):
    # this file can be written by CI tools to set the version (e.g. from tag name)
    with open("VERSION.txt") as version_file:
        version = version_file.read().strip().strip("v")
else:
    # no version - ensure it gets a new version every install to force upgrade
    version = str(time.time())

print(find_packages(exclude=["tests", "test.*"]))
setup(
    name='budgetkey-data-pipelines',
    version=version,
    packages=find_packages(exclude=["tests", "test.*"]),
    install_requires=['datapackage-pipelines'],
    entry_points={'console_scripts': ['budgetkey-dpp = budgetkey_data_pipelines.cli:main']}
)
