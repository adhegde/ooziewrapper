#! /usr/bin/env python

import os
from setuptools import setup, find_packages

# Utility function for reading from README.md
# Reference: https://pythonhosted.org/an_example_pypi_project/setuptools.html
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# Call setup function.
setup(
    name = "ooziewrapper",
    version = "0.1.0",
    author = "Anthony Gatti",
    author_email = "anthony.j.gatti@gmail.com",
    description = "A friendly wrapper for Apache Oozie.",
    license = "Apache 2",
    keywords = "oozie",
    url = "https://github.com/anthonyjgatti/ooziewrapper",
    packages = find_packages(),
    install_requires = [
        "networkx",
        "PyYAML"
    ] # Add more dependencies here.
)
