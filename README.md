# budgetkey-data-pipelines

[![Build Status](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines.svg?branch=master)](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines)

Budget Key data processing pipelines

## Quickstart
```bash
$ sudo apt-get install build-essential python3-dev libxml2-dev libxslt1-dev
$ python --version
Python 3.6.1
$ sudo mkdir -p /var/datapackages && sudo chown $USER /var/datapackages/
$ make install
$ budgetkey-dpp
INFO    :Main                            :Skipping redis connection, host:None, port:6379
Available Pipelines:
- ./budget/national/changes/original/national-budget-changes
...
```

### next steps

#### unit tests
```
$ make test
```

##### run a specific test / modify test arguments

any arguments added to tox will be added to the underlying py.test command

```
$ tox tests/tenders/test_fixtures.py
```

tox can be a bit slow, especially when doing tdd

to run tests faster you can run py.test directly, but you will need to setup the test environment first

```
$ pip install pytest
$ py.test tests/tenders/test_fixtures.py -svk test_tenders_fixtures_publishers
```

#### running a pipeline
```bash
$ budgetkey-dpp run ./entities/companies/registrar/registry
```

following files will be created:
* /var/datapackages - data saved in datapackages
* budgetkey_data_pipelines/.data.db - data saved in DB (to use a different DB, set DPP_DB_ENGINE env var using sqlalchemy connection url format)
* budgetkey_data_pipelines/pipelines/.dpp.db - metadata about the pipelines themselves and run status
