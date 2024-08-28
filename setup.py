#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from os.path import join as opj, dirname
import re


readme = opj(dirname(__file__), 'README.md')
long_description = open(readme).read()

version_file = 'grue/_version.py'
mo = re.search(
    r"^__version__ = ['\"]([^'\"]*)['\"]", open(version_file, "rt").read(),
    re.M)

if mo:
    version = mo.group(1)
else:
    raise RuntimeError('Unable to find version string in %s.' % version_file)

setup(
    name='grue',
    version=version,
    author='Niels Reuter',
    author_email='n.reuter@fz-juelich.de',
    description='Turn machines on/off automatically for HTCondor jobs',
    entry_points={
        'console_scripts': [
            'grue-daemon=grue.daemon:main',
            'grue=grue.client:main']},
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='ISC',
    classifiers=[
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Operating System :: POSIX :: Linux'],
    python_requires='>=3.11.0',
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    install_requires=['htcondor', 'python-ipmi'],
    tests_require=['pytest', 'pytest-mock'],
    extras_require={'grue-status': ['rich']})
