import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "pinboard",
    version = "0.1.0",
    author = "John Parker",
    author_email = "japarker@uchicago.com",
    description = ("Cached simulation results and metadata across multiple devices"),
    license = "MIT",
    packages=['pinboard'],
    long_description=read('README.md'),
    install_requires=['numpy', 'h5py'],
    include_package_data = True,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
    ],
)
