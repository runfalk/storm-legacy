#!/usr/bin/env python
import os
import re

from setuptools import setup, Extension, find_packages


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
    extras_require={
        "doc": [
            "sphinx",
            "sphinx_epytext",
        ],
        "dev": [
            "fixtures>=0.3.5",
            "psycopg2-binary>=2.5",
            "pytest>=3",
            "testresources>=0.2.4",
            "testtools>=0.9.8",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        ("License :: OSI Approved :: GNU Library or "
         "Lesser General Public License (LGPL)"),
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    ext_modules=(BUILD_CEXTENSIONS and
                 [Extension("storm.cextensions", ["storm/cextensions.c"])]),
    zip_safe=False,
)
