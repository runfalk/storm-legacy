PYTHON = python

TEST_COMMAND = $(PYTHON) test

STORM_POSTGRES_URI = postgres:storm_test
STORM_POSTGRES_HOST_URI = postgres://localhost/storm_test
STORM_MYSQL_URI = mysql:storm_test
STORM_MYSQL_HOST_URI = mysql://localhost/storm_test

export STORM_POSTGRES_URI
export STORM_POSTGRES_HOST_URI
export STORM_MYSQL_URI
export STORM_MYSQL_HOST_URI

all: build

build:
	$(PYTHON) setup.py build_ext -i

check: build
	# Run the tests once with cextensions and once without them.
	$(TEST_COMMAND) && STORM_CEXTENSIONS=1 $(TEST_COMMAND)

.PHONY: all build test
