#!/usr/bin/env python
import os
import re

from setuptools import setup, Extension, find_packages


BUILD_CEXTENSIONS = True

readme = None
with open("README.md") as f:
    readme = f.read()

setup(
    name="storm-legacy",
    version="0.2.0",
    description=(
        "Storm is an object-relational mapper (ORM) for Python developed at "
        "Canonical. This is a fork maintained by Andreas Runfalk"),
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Gustavo Niemeyer",
    author_email="gustavo@niemeyer.net",
    maintainer="Andreas Runfalk",
    maintainer_email="andreas@runfalk.se",
    license="LGPL",
    url="https://github.com/runfalk/storm-legacy",
    download_url="https://pypi.org/project/storm-legacy/",
    packages=find_packages(exclude=["tests", "tests.*"]),
    extras_require={
        "doc": [
            "sphinx",
            "sphinx_epytext",
        ],
        "dev": [
            "docutils",
            "fixtures>=0.3.5",
            "freezegun",
            "mock>=2.0.0",
            "psycopg2-binary>=2.5",
            "pytest>=3",
            "pytest-lazy-fixture",
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
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    ext_modules=(BUILD_CEXTENSIONS and
                 [Extension("storm.cextensions", ["storm/cextensions.c"])]),
    zip_safe=False,
)
