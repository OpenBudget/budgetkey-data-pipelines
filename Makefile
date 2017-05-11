.PHONY: install test

install:
	pip install --upgrade -e .[develop]

test:
	tox -r
