PYTHON = python$(PYTHON_VERSION)
PYTHON_VERSION = 2.4
TESTDB = storm_test

all: build

build:
	$(PYTHON) setup.py build_ext -i

check: build
	@echo "* Creating $(TESTDB)"
	@if psql -l | grep -q " $(TESTDB) "; then \
	    dropdb $(TESTDB) >/dev/null; \
	fi
	createdb $(TESTDB)
	# Run the tests once ...
	STORM_POSTGRES_URI=postgres:$(TESTDB) $(PYTHON) test --verbose
	# And again with the C extensions disabled
	STORM_POSTGRES_URI=postgres:$(TESTDB) STORM_DISABLE_CEXTENSIONS=1 \
	  $(PYTHON) test --verbose

.PHONY: all build check
