#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
from rungamess import __version__

test_requirements=[
]

setup(
    name='rungamess',
    license='BSD',
    version=__version__,
    description='Python based job submission tools for GAMESS',
    author=u'',
    author_email='',
    url='http://github.com/DaveTomlinson/rungamess',
    packages=['rungamess'],
    scripts=['scripts/run_gamess'],
    classifiers=[
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
    ],
    tests_require=test_requirements,
    install_requires=[
    ] + test_requirements,
    dependency_links = [
    ]
)
