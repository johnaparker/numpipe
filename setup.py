import os
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
import pathlib

NAME = 'numpipe'
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

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def post_install():
    config_str = """
[notifications]
    [notifications.telegram]
    token = ""
    chat_id = 0
""".strip()

    config_path = pathlib.Path('~/.config/numpipe/numpipe.conf').expanduser()
    if not os.path.exists(config_path):
        os.makedirs(config_path.parent, exist_ok=True)
        with open(config_path, 'w') as f:
            f.write(config_str)

class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        post_install()
        install.run(self)

class PostDevelopCommand(develop):
    """Post-installation for installation mode."""
    def run(self):
        post_install()
        develop.run(self)

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
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Development Status :: 3 - Alpha',
    ],
)
