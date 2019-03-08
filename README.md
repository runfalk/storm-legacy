What is this fork?
==================
I use Storm in a big private project and due to Storm I have
been stuck with Python 2.7. This forks fixes that by adding
support for Python 3.3 and later. To do that I removed
functionality that I don't use myself, and feel is outdated. In
the long run I think completely switching to
[SQLAlchemy](https://www.sqlalchemy.org/) is the only correct
choice. But since it was easier to port Storm I decided to go
with that approach for now.

I'll maintain this fork until I've replaced Storm, which likely
won't happen for years.

The reason for forking is that Storm hasn't seen a release in
almost five years; the last release was 0.20 2013-06-28. I forked
the current trunk on 2018-05-04 which had quite a few fixes for
things that are broken in 0.20. The trunk revision at the time
was [#484](http://bazaar.launchpad.net/~storm/storm/trunk/files/484?start_revid=484).


Changelog
---------
The `CHANGELOG` file will not be updated as it is from the unforked
version. I've decided to restart the version numbering.


### Version 0.2.0 (alpha)
Released on 8th Mars 2019

 * Made `PropertyRegistry.add_class` optionally work as a decorator
 * Re-added `storm.version` and `storm.version_info` for libraries and
   applications that have specific Storm version requirements. The version
   is specified as `0.21.0` (issue
   [#1](https://github.com/runfalk/storm-legacy/issues/1)).
 * Removed `CaptureTracer` support
 * Removed `TimeoutTracer` support
 * Refactored test suite to use pytest. This solves a lot of
   warnings during tests.


### Version 0.1.0 (alpha)
Released on 8th October 2018

This release mostly removes features. The release is mostly a test
of how well it works with other applications. This release comes
after storm 0.20.

 * Added `Database.get_uri()` method
 * Added `ResultSet` support to be used as subquery
 * Added support for Python 3.3
 * Added support for Python 3.4
 * Added support for Python 3.5
 * Added support for Python 3.6
 * Added support for Python 3.7
 * Added support for Postgresql `CASE` statement
 * Added support for Union of different classes as long as the columns
   are of the same type
 * Changed psycopg2 requirement to `>=2.5`
 * Changed test runner to pytest
 * Fixed storm accidentally installing `tests` directory in `site-packages`
 * Fixed `psycopg2>=2.5` support using ABCMeta base class injection for
   exceptions
 * Removed MySQL support
 * Removed Pickle column type (use JSON or re-implement it yourself)
 * Removed Python 2.6 and earlier support
 * Removed Schema management
 * Removed SQLObject support
 * Removed Twisted integration
 * Removed WSGI debug timeline
 * Removed Zope integration


Original description
====================

Storm is an Object Relational Mapper for Python developed at
Canonical.  API docs, a manual, and a tutorial are available from:

http://storm.canonical.com/


Introduction
============

The project was in development for more than a year for use in
Canonical projects such as Launchpad and Landscape before being
released as free software on July 9th, 2007.

Design:

 * Clean and lightweight API offers a short learning curve and
   long-term maintainability.
 * Storm is developed in a test-driven manner. An untested line of
   code is considered a bug.
 * Storm needs no special class constructors, nor imperative base
   classes.
 * Storm is well designed (different classes have very clear
   boundaries, with small and clean public APIs).
 * Designed from day one to work both with thin relational
   databases, such as SQLite, and big iron systems like PostgreSQL
   and MySQL.
 * Storm is easy to debug, since its code is written with a KISS
   principle, and thus is easy to understand.
 * Designed from day one to work both at the low end, with trivial
   small databases, and the high end, with applications accessing
   billion row tables and committing to multiple database backends.
 * It's very easy to write and support backends for Storm (current
   backends have around 100 lines of code).

Features:

 * Storm is fast.
 * Storm lets you efficiently access and update large datasets by
   allowing you to formulate complex queries spanning multiple
   tables using Python.
 * Storm allows you to fallback to SQL if needed (or if you just
   prefer), allowing you to mix "old school" code and ORM code
 * Storm handles composed primary keys with ease (no need for
   surrogate keys).
 * Storm doesn't do schema management, and as a result you're free
   to manage the schema as wanted, and creating classes that work
   with Storm is clean and simple.
 * Storm works very well connecting to several databases and using
   the same Python types (or different ones) with all of them.
 * Storm can handle `obj.attr = <An SQL expression>` assignments, when
   that's really needed (the expression is executed at INSERT/UPDATE
   time).
 * Storm handles relationships between objects even before they were
   added to a database.
 * Storm works well with existing database schemas.
 * Storm will flush changes to the database automatically when
   needed, so that queries made affect recently modified objects.


License
=======

Copyright (C) 2006-2009 Canonical, Ltd.  All contributions must have
copyright assigned to Canonical.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301 USA

On Ubuntu systems, the complete text of the GNU Lesser General
Public Version 2.1 License is in /usr/share/common-licenses/LGPL-2.1


Developing Storm
================

SHORT VERSION:  If you are running ubuntu, or probably debian, the
following should work.  If not, and for reference, the long version
is below.

```bash
dev/ubuntu-deps
make develop
dev/db-setup
make check
```

LONG VERSION:

The following instructions describe the procedure for setting up a
development environment and running the test suite.

Installing dependencies
-----------------------

The following instructions assume that you're using Ubuntu.  The
same procedure will probably work without changes on a Debian system
and with minimal changes on a non-Debian-based linux distribution.
In order to run the test suite, and exercise all supported backends,
you will need to install MySQL and PostgreSQL, along with the
related Python database drivers:

```bash
sudo apt-get install \
    python-mysqldb mysql-server \
    postgresql pgbouncer \
    build-essential
```

These will take a few minutes to download (its a bit under 200MB all
together).  Once the download is complete, a screen called
"configuring mysql-server-5.0" will be shown.  When asked for a
password for the root user leave the field blank and hit enter to
continue.  This is not a recommended setting for a production
server, but makes life easier on a development machine.  You may be
asked to enter a password multiple times.  Leave it blank in each
case.

The Python dependencies for running tests can mostly be installed with
apt-get:

```bash
apt-get install \
    python-fixtures python-psycopg2 \
    python-testresources python-transaction python-twisted \
    python-zope.component python-zope.security
```

One module - pgbouncer - is not yet packaged in Ubuntu. It can be
installed from PyPI: http://pypi.python.org/pypi/pgbouncer

Alternatively, dependencies can be downloaded as eggs into the current
directory with: `make develop`

This ensures that all dependencies are available, downloading from
PyPI as appropriate.

Setting up database users and access security
---------------------------------------------

PostgreSQL needs to be setup to allow TCP/IP connections from
localhost.  Edit `/etc/postgresql/8.3/main/pg_hba.conf` and make
sure the following line is present:

```
host all all 127.0.0.1/32 trust
```

This will probably (with PostgresSQL 8.4) entail changing 'md5' to
'trust'.

In order to run the two-phase commit tests, you will also need to
change the max_prepared_transactions value in postgres.conf to
something like

```
max_prepared_transactions = 200
```

Now save and close, then restart the server:

```bash
sudo systemctl restart postgresql
```

Lets create our PostgreSQL user now. As noted in the Ubuntu PostgreSQL
documentation, the easiest thing is to create a user with the same
name as your username.  Run the following command to create a user
for yourself (if prompted for a password, leave it blank):

```bash
sudo -u postgres createuser --superuser $USER
```

Creating test databases
-----------------------

The test suite needs some local databases in place to exercise
PostgreSQL functionality:

```bash
createdb storm_test
```

Running the tests
-----------------

Finally, its time to run the tests!  Go into the base directory of
the storm branch you want to test, and run:

```bash
make check
```

They'll take a while to run.  All tests should pass: failures mean
there's a problem with your environment or a bug in Storm.
