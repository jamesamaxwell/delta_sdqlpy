import os
from setuptools import setup

setup(
    name = "delta_sdqlpy",
    version = "1.0.0",
    author = "James Maxwell",
    author_email = "s2017578@ed.ac.uk",
    description = ("Compiled Delta Query Processing in Python"),
    license = "-",
    keywords = "-",
    url = "-",
    packages=["sdqlpy"],
    long_description='README.md',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
    include_package_data=True,
)
