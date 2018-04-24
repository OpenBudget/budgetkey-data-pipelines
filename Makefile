.PHONY: install test clean

install:
	pip install --upgrade -e .[develop]

test:
	tox -r

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
