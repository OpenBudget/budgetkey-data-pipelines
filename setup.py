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
                    #   'demjson',
                      'requests',
                      'selenium<4.3',
                      'fuzzywuzzy[speedup]',
                      'plyvel',
                      'filemagic',
                      'dataflows-elasticsearch>=0.1.2',
                      'tableschema_elasticsearch>=2.1.3',
                      'datapackage-pipelines-aws',
                      'textract==1.5.0',  # later versions of textract introduce unnecessary dependency on swig
                                          # see this issue - https://github.com/deanmalmgren/textract/issues/159
#                      'urllib3==1.21.1',
                      'geocoder',
                      'boto3',
                      'paramiko',
                      'dataflows>=0.5.12',
                      'lxml',
                      'lxml_html_clean',
                      'elasticsearch<9.0.0',
                      'openai',
                      'kvfile>=1.1.2',
                      'pyproj'
		     ],
    extras_require={'develop': ["tox", "datapackage-pipelines"]},
    entry_points={'console_scripts': ['budgetkey-dpp = datapackage_pipelines_budgetkey.cli:main']}
)
