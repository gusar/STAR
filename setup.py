#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

test_requirements = [
    'tox'
]

setup(
    name='star',
    version='0.1.0',
    description="Stocktwits Analytics and Reporting",
    long_description=readme + '\n\n' + history,
    author="Andy Lahs",
    author_email='andy.lahs@gmail.com',
    url='https://github.com/gusar/star',
    packages=[
        'star',
    ],
    package_dir={'star':
                 'star'},
    entry_points={
        'console_scripts': [
            'star=star.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='star',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='star/tests',
    tests_require=test_requirements
)
