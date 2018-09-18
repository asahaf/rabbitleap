.PHONY: help clean clean-pyc clean-build list test test-all coverage docs publish sdist

init:
	pip install pipenv --upgrade
	pipenv install --dev --skip-lock

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "test-all - run tests on every Python version with tox"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "publish - package and upload a release"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

lint:
	pipenv run flake8 rabbitleap

test:
	py.test

test-all:
	tox

coverage:
	pipenv run py.test --cov-config .coveragerc --verbose --cov-report term --cov-report xml --cov=rabbitleap tests

docs:
	rm -f docs/rabbitleap.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ rabbitleap
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	@echo View the docs homepage at docs/_build/html/index.html

publish: clean
	python setup.py sdist bdist_wheel
	twine upload dist/*
	rm -fr build dist .egg rabbitleap.egg-info
