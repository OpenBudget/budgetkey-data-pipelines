# budgetkey-data-pipelines

[![Build Status](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines.svg?branch=master)](https://travis-ci.org/OpenBudget/budgetkey-data-pipelines)

Budget Key data processing pipelines

## What are we doing here?

The heart of the BudgetKey project is its rich, up-to-date quality data collection. Data is collected from over 20 different data sources, cleaned, normalised, validated, combined and analysed - to create the most extensive repository of fiscal data in Israel.
 
 In order to get that data, we have an extensive set of downloaders and scrapers which get the data from government publications and other web-sites. The fetched data is then processed and combined, and eventually saved to disk (so that people can download the raw data without hassle), loaded to a relational database (so that analysts can do in-depths queries of the data) and pushed to a key-store value (elasticsearch) which serves our main website (obudget.org).
 
 The frameworks we're using to accomplish all of this are called `dataflows` and `datapackage-pipelines`. These frameworks allow us to write simple 'pipelines', each consisting of a set of predefined processing steps. Most of the pipelines use of a set of common building-blocks, and some custom processors - mainly custom scrapers for exotic sources.

 `dataflows` is used for writing processing flows individual sources.

 `datapackage-pipelines` runs the flows, combines the results and creates aggregated datasets.
 
 ## Quickstart on `dataflows`

The recommended way to start is by reading the README of `dataflows`[here](https://github.com/datahq/dataflows) - and then follow on to the interactive tutorial on Binder (you'll find the link there).

 Then, try to write a very simple pipeline - just to test your understanding. A good task for that would be:
 - Load the first page of ynet/youtube/reddit/
 - Scrape the list of items from that page
 - Set proper datatypes for the fields (e.g. for title, dates etc)
 - Dump the results into a csv file or an sqlite file

## Working on issues (for newcomers)

Most issues you'd be working on will require you to write a 'Flow' to perform some scraping, analysing or data integration.

1. The first step will always be to write that flow and make sure it works.
2. Then, we would need to integrate it with the rest of the pipelines and perhaps with other, existing datasets.

Finish step (1) before starting step (2) - the entire system is quite complex but you shouldn't worry about understanding everything at first. Consult with Adam when (1) is done for guidance and support...

 ## Quickstart on `datapackage-pipelines`
 
 The recommended way to start is by reading the README of `datapackage-pipelines`[here](https://github.com/frictionlessdata/datapackage-pipelines)- it's a bit long, so at least read the beginning and skim the rest.
 
 ## I want to help - how?
 
 - Make sure you've completed the `dataflowes` quickstart above - you need to know how it works to start contributing
 - We have a few issues for newcomers - these are marked using the 'help-wanted' label. Just take a peek in the [issues](https://github.com/OpenBudget/budgetkey-data-pipelines/issues) section.
   - If unsure, just consult on Slack in the #obudget-chat channel or with @adam.kariv
 - Either way, make sure you assign yourself to an issue when you start working on it (or even leave a comment that you're considering it). 
   - (If you stop working on an issue, un-assigning yourself would be highly appreciated :smile: )
     
 ## What's currently implemented (the directory structure)
 
 All the pipeline definitions can be found under `datapackage_pipelines_budgetkey/pipelines`.
 
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

As you can see, pipelines are sorted by domain and data source.

Higher up the tree, we find pipelines that aggregate different sources (e.g. companies + associations + ... -> all-entities). Finally, under the `budgetkey` directory we have pipelines that process and store the data for displaying on the website.

_note: To understand a bit more on the difference between the different types of government spending, please read this excellent [blog post](https://blog.okfn.org/2017/05/18/what-is-the-difference-between-budget-spending-and-procurement-data/)._

## What's currenty running?

 To see what's the current processing status of each pipeline, just hop to the [dashboard](https://next.obudget.org/pipelines/).
 
## Developing a new pipeline

- Read about `dataflows` [here](https://github.com/datahq/dataflows)
- Install this package (`datapackage-pipelines-budgetkey`) using the instructions in the Quickstart section below
- Try to understand where is the change that you want to make supposed to reside? 
    - Is it related to one of the existing pipelines?
    - Is it something new altogether?
- Find one or more similar existing pipelines and read their code to get some ideas, inspiration and know-how regarding our custom processors as well as common solutions to common problems.
  -But note that some of our legacy code is not using `dataflows` (but running on `datapackage-pipelines` directly)

## Installation

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

### Installing dataflows

```bash
$ pip install dataflows
```

### Installation of the Package (if you want to run the system - not needed for a single pipeline development)

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

### Running a Pipeline (if you want to run the system - not needed for a single pipeline development)
```bash
$ budgetkey-dpp run ./entities/companies/registrar/registry
```

The following files will be created:
* `/var/datapackages` - data saved in datapackages
* `datapackage_pipelines_budgetkey/.data.db` - data saved in DB (to use a different DB, set DPP_DB_ENGINE env var using sqlalchemy connection url format)
* `datapackage_pipelines_budgetkey/pipelines/.dpp.db` - metadata about the pipelines themselves and run status


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

## Using Docker Compose

Docker Compose can be used to run a full environment with all required services - similar to the production environment.

### Installation

* Install Docker and Docker Compose (Refer to Docker documentation)
* (Optional) Build the image from current directory: `docker-compose build pipelines`
* Run the minimal required environment services in the background:
  * `docker-compose up -d redis db pipelines`
* You should have the following endpoints available:
  * Pipelines dashboard - `http://localhost:5000/` (doesn't run any workers by default)
  * DB - `postgresql://postgres:123456@localhost:15432/postgres`
* You can run budgetkey-dpp command from inside the docker container:
  * `docker-compose exec pipelines sh -c "budgetkey-dpp"`
* Elasticsearch and Kibana are also available, to start:
  * `docker-compose up -d elasticsearch kibana`
* To connect to the service in docker from local PC:
  * `source .env.example`
  * `dpp`

## Loading datapackages to Elasticsearch

This method allows to load the prepared datapackages to elasticsearch, the data is then available for exploration via Kibana

This snippet will delete all local docker-compose volumes - so make sure you don't have anything important there beforehand..

It loads the first 100 rows from each pipeline, you can modify ES_LIMIT_ROWS below or remove it to load all data

```
docker-compose down -v && docker-compose pull elasticsearch db && docker-compose up -d elasticsearch db
export DPP_DB_ENGINE="postgresql://postgres:123456@localhost:15432/postgres"
export DPP_ELASTICSEARCH="localhost:19200"
for doctype in `budgetkey-dpp | grep .budgetkey/elasticsearch/index_ | cut -d"_" -f2 - | cut -d" " -f1 -`; do
    echo " > Loading ${doctype}"
    ES_LOAD_FROM_URL=1 ES_LIMIT_ROWS=100 budgetkey-dpp run ./budgetkey/elasticsearch/index_$doctype
done
```

Now you can start Kibana to explore the data

```
docker-compose up -d kibana
```

Kibana should be available at http://localhost:15601/ (It might take some time to start up properly)

Index name is `budgetkey`
