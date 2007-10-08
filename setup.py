#!/usr/bin/env python

import os

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension


BUILD_CEXTENSIONS = False


def find_packages():
    # implement a simple find_packages so we don't have to depend on
    # setuptools
    packages = []
    for directory, subdirectories, files in os.walk("storm"):
        if '__init__.py' in files:
            packages.append(directory.replace(os.sep, '.'))
    return packages

setup(
    name="storm",
    version="0.10",
    description="Storm is an object-relational mapper (ORM) for Python developed at Canonical.",
    author="Gustavo Niemeyer",
    author_email="gustavo@niemeyer.net",
    maintainer="Storm Developers",
    maintainer_email="storm@lists.canonical.com",
    license="LGPL",
    url="https://storm.canonical.com",
    download_url="https://launchpad.net/storm/+download",
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    package_data={"": ["*.zcml"]},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    ext_modules=(BUILD_CEXTENSIONS and
                 [Extension("storm.cextensions", ["storm/cextensions.c"])])
)
