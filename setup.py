#!/usr/bin/env python
#
# Copyright 2019-2020 Flavio Garcia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages
try:
    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    # for pip <= 9.0.3
    from pip.req import parse_requirements
import coelho
import os


# Solution from http://bit.ly/29Yl8VN
def resolve_requires(requirements_file):
    requirements = parse_requirements("./%s" % requirements_file,
            session=False)
    return [str(ir.req) for ir in requirements]


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
# Solution from: http://bit.ly/2mig8RT
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# We still running: python setup.py sdist upload --repository=testpypi
# Twine isn't handling long_descriptions as per:
# https://github.com/pypa/twine/issues/262
setup(
    name="coelho",
    version=coelho.get_version(),
    description="Candango Coelho AMPQ Toolkit",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    license=coelho.__licence__,
    author=coelho.get_author(),
    author_email=coelho.get_author_email(),
    maintainer=coelho.get_author(),
    maintainer_email=coelho.get_author_email(),
    install_requires=resolve_requires("requirements.txt"),
    url="https://github.com/candango/coelho",
    packages=find_packages(),
    package_dir={'coelho': "coelho"},
    include_package_data=True,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Environment :: Web Environment",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
        "Topic :: Internet :: WWW/HTTP :: WSGI",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
