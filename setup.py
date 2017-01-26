#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
import re

setup(
    name='pdict',
    description='Persistent dictionary',
    version=re.search("__version__ = '(.+)'", open("pdict/_version.py").readlines()[0].rstrip()).group(1),
    author='Alexander Tkachenko',
    author_email='alex.tk.fb@gmail.com',
    packages=['pdict'],
    license='GNU GPL version 2',
    url="https://github.com/AleksTk/pdict",
    install_requires=['mmh3', 'six', 'msgpack-python'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
    ]
)
