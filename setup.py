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
    name='datapackage-pipelines-budgetkey',
    version=version,
    packages=find_packages(exclude=["tests", "test.*"]),
    install_requires=['pyquery',
                      'demjson',
                      'requests',
                      'selenium',
                      'fuzzywuzzy[speedup]',
                      'plyvel',
                      'datapackage-pipelines-elasticsearch>=0.0.5',
                      'textract==1.5.0',  # later versions of textract introduce unnecesary dependency on swig
                                          # see this issue - https://github.com/deanmalmgren/textract/issues/159
                      'geocoder'],
    extras_require={'develop': ["tox", "datapackage-pipelines"]},
    entry_points={'console_scripts': ['budgetkey-dpp = datapackage_pipelines_budgetkey.cli:main']}
)
