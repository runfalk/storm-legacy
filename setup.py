#!/usr/bin/env python
import os
import re

from setuptools import setup, Extension, find_packages


if os.path.isfile("MANIFEST"):
    os.unlink("MANIFEST")


BUILD_CEXTENSIONS = True


VERSION = re.search('version = "([^"]+)"',
                    open("storm/__init__.py").read()).group(1)


setup(
    name="storm",
    version=VERSION,
    description=(
        "Storm is an object-relational mapper (ORM) for Python "
        "developed at Canonical."),
    author="Gustavo Niemeyer",
    author_email="gustavo@niemeyer.net",
    maintainer="Storm Developers",
    maintainer_email="storm@lists.canonical.com",
    license="LGPL",
    url="https://storm.canonical.com",
    download_url="https://launchpad.net/storm/+download",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={"": ["*.zcml"]},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        ("License :: OSI Approved :: GNU Library or "
         "Lesser General Public License (LGPL)"),
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    ext_modules=(BUILD_CEXTENSIONS and
                 [Extension("storm.cextensions", ["storm/cextensions.c"])]),
    # The following options are specific to setuptools but ignored (with a
    # warning) by distutils.
    include_package_data=True,
    zip_safe=False,
    test_suite = "tests.find_tests",
    tests_require=[
        # Versions based on Lucid, where packaged.
        "fixtures >= 0.3.5",
        # pgbouncer (the Python module) is not yet packaged in Ubuntu.
        "pgbouncer >= 0.0.7",
        "psycopg2 >= 2.5",
        "testresources >= 0.2.4",
        "testtools >= 0.9.8",
        ],
    )
