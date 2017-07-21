# budgetkey-data-pipelines

[![Build Status](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines.svg?branch=master)](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines)

Budget Key data processing pipelines

## What are we doing here?

The heart of the BudgetKey project is its rich, up-to-date quality data collection. Data is collected from over 20 different data sources, cleaned, normalised, validated, combined and analysed - to create the most extensive repository of fiscal data in Israel.
 
 In order to get that data, we have an extensive set of downloaders and scrapers which get the data from government publications and other web-sites. The fetched data is then processed and combined, and eventually saved to disk (so that people can download the raw data without hassle), loaded to a relational database (so that analysts can do in-depths queries of the data) and pushed to a key-store value (elasticsearch) which serves our main website (obudget.org).
 
 The framework we're using to accomplish all of this is called `datapackage-pipelines`. This framework allows us to write simple 'pipelines', each consisting of a set of predefined processing steps. These pipelines are not coded, but rather defined in a set of YAML files. Most of the pipelines use of a set of common building-blocks, and some custom processors - mainly custom scrapers for exotic sources.
     
 To see what's the current processing status of each pipeline, just hop to the (dashboard)[https://next.obudget.org/pipelines/].
 
 ## The directory structure
 
 All the pipeline definitions can be found under `budgetkey_data_pipeline/pipelines`.
 
 There we can see the following directory structure (the most interesting parts of it, anyway):
 - `budget/`
    - `national/`
        - `original`: Pipelines for retrieving the national budget
        - `processed`: Pipelines for processing and analysing that budget
        - `changes/`
            - `original`: Pipelines for retrieving the national budget changes information
            - `processed`: Pipelines for processing and analysing these changes (detecting transactions etc.)
            - `explanations`: Pipelines for retrieving and extracting the text off the national budget change explnataion documents
- `entities/`
    - `associations`: Pipelines retrieving information regarding NGOs 
    - `companies`: Pipelines retrieving information regarding companies 
    - `ottoman`: Pipelines retrieving information regarding Ottoman Associations 
    - `special`: Pipelines retrieving information regarding other entities 
- `procurement/`
    - `spending`: Pipelines for retrieving and processing government spending reports
    - `tenders`: Pipelines for retrieving data on government tendering process (and the lack of it)
- `supports/`: Pipelines for retrieving data on government supports and its relevant processes

_note: To understand a bit more on the difference between the different types of government spending, please read this excellent (blog post)[https://blog.okfn.org/2017/05/18/what-is-the-difference-between-budget-spending-and-procurement-data/]._

## Developing a new pipeline

- Read about `datapackage-pipelines` (here)[https://github.com/frictionlessdata/datapackage.pipelines/]
- Install this package (`budgetkey-data-pipelines`) using the instructions in the Quickstart section below
- Try to understand where is the change that you want to make supposed to reside? 
    - Is it related to one of the existing pipelines?
    - Is it something new altogether?
- Find one or more similar existing pipelines and read their code to get some ideas, inspiration and know-how regarding our custom processors as well as 


## Common

## Quickstart

### Installation of the Package
```bash
$ sudo apt-get install build-essential python3-dev libxml2-dev libxslt1-dev libleveldb-dev
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

### Installing Python 3.6+
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

### Running a Pipeline
```bash
$ budgetkey-dpp run ./entities/companies/registrar/registry
```

The following files will be created:
* `/var/datapackages` - data saved in datapackages
* `budgetkey_data_pipelines/.data.db` - data saved in DB (to use a different DB, set DPP_DB_ENGINE env var using sqlalchemy connection url format)
* `budgetkey_data_pipelines/pipelines/.dpp.db` - metadata about the pipelines themselves and run status


### Writing Tests

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
