# budgetkey-data-pipelines
Budget Key data processing pipelines

## Quickstart
```bash
$ sudo apt-get install build-essential python3-dev libxml2-dev libxslt1-dev
$ python --version
Python 3.6.0+
$ pip install -e .
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

