.PHONY: install test

install:
	pip install --upgrade -e .[develop]
	pip install --upgrade https://github.com/OriHoch/datapackage-pipelines/archive/add-support-for-streaming-non-tabular-plain-text-files.zip

test:
	tox
