# stub Makefile for launchpad testing

PYTHON = python$(PYTHON_VERSION)
PYTHON_VERSION = 2.4
TESTDB = storm_test

all:
	@:

check:
	@echo "* Creating $(TESTDB)"
	@if psql -l | grep -q "$(TESTDB)"; then \
	    dropdb $(TESTDB) >/dev/null; \
	fi
	createdb $(TESTDB)
	STORM_POSTGRES_URI=postgres:$(TESTDB) $(PYTHON) test --verbose

.PHONY: all check
