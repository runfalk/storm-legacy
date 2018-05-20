PYTHON_VERSION = 2.7
STORM_POSTGRES_URI ?= postgres:storm_test
export STORM_POSTGRES_URI

build:
	venv/bin/python setup.py build_ext

develop:
	[ ! -d "venv" ]
	virtualenv --python=python$(PYTHON_VERSION) --prompt="(storm)" venv
	venv/bin/pip install --upgrade pip setuptools
	venv/bin/pip install -e .[doc,dev]

clean-build:
	rm -rf build/
	rm -rf doc-build/

clean-pyc:
	find . -type f -name "*.pyc" -exec rm -f {} \;
	find . -type d -name "__pycache__" -exec rmdir {} \;

clean: clean-build clean-pyc

realclean: clean
	find . -name "*.so" -type f -exec rm -f {} \;
	rm -rf *.egg-info/
	rm -rf venv/

doc:
	@venv/bin/sphinx-build "doc/" "doc-build/"

test:
	@venv/bin/pytest

full-test:
	STORM_CEXTENSIONS=0 make test
	STORM_CEXTENSIONS=1 make test

.PHONY : build clean clean-build clean-pyc develop doc realclean
