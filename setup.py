import os
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

NAME = 'numflow'
DESCRIPTION = "Easily run Python functions in parallel and cache the results"
URL = ''
EMAIL = 'japarker@uchicago.edu'
AUTHOR = 'John Parker'
KEYWORDS = 'cache pipeline hdf5 parallel cluster'
# REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.1'
LICENSE = 'MIT'

REQUIRED = [
    'numpy', 
    'h5py', 
    'termcolor', 
]

setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    license=LICENSE,
    keywords=KEYWORDS,
    url=URL,
    packages=find_packages(),
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    install_requires=REQUIRED,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Development Status :: 3 - Alpha',
    ],
)
