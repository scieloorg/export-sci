#!/usr/bin/env python
import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()
with open(os.path.join(here, 'VERSION')) as f:
    VERSION = f.read()


requires = [
    'requests>=2.18.1',
    'pymongo>=3.4.0',
    'lxml>=3.8.0'
]

setup(
    name="exportsci",
    version=VERSION,
    description="Export metadata to SciELO CI",
    author="SciELO",
    author_email="scielo-dev@googlegroups.com",
    license="BSD 2-clause",
    url="http://docs.scielo.org",
    keywords='scielo export citation index',
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Operating System :: POSIX :: Linux",
        "Topic :: System",
        "Topic :: Utilities",
    ],
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "docs"]
    ),
    test_suite="tests",
    install_requires=requires,
    entry_points="""\
    [console_scripts]
    exportsci=exportsci:main
    """,
)
