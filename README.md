# budgetkey-data-pipelines

[![Build Status](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines.svg?branch=master)](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines)

Budget Key data processing pipelines

## Quickstart
```bash
$ sudo apt-get install build-essential python3-dev libxml2-dev libxslt1-dev
$ python --version
Python 3.6.0+
$ sudo mkdir -p /var/datapackages && sudo chown $USER /var/datapackages/
$ make install
$ budgetkey-dpp
INFO    :Main                            :Skipping redis connection, host:None, port:6379
Available Pipelines:
- ./budget/national/changes/original/national-budget-changes
...
```

## What needs to be done?
Take a peek at the [pipelines dashboard](http://next.obudget.org/pipelines).
You will see there the current status of pipelines, as well as stuff that's still missing.

## Installing Python 3.6+
We recommend using [pyenv](https://github.com/pyenv/pyenv) for managing your installed python versions.

On Ubuntu, use these commands:
```bash
sudo apt-get install git python-pip make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev
sudo pip install virtualenvwrapper

git clone https://github.com/yyuu/pyenv.git ~/.pyenv
git clone https://github.com/yyuu/pyenv-virtualenvwrapper.git ~/.pyenv/plugins/pyenv-virtualenvwrapper

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo 'pyenv virtualenvwrapper' >> ~/.bashrc

exec $SHELL
```

On OSX, you can run
```bash
brew install pyenv
echo 'eval "$(pyenv init -)"' >> ~/.bash_profile
```

After installation, running:
```bash
pyenv install 3.6.1
pyenv global 3.6.1
```

Will set your Python version to 3.6.1


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
