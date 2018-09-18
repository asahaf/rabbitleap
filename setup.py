# -*- coding: utf-8 -*-
import os
from io import open

from setuptools import setup

current_dir = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(
        os.path.join(current_dir, 'rabbitleap', '__version__.py'),
        mode='r',
        encoding='utf-8') as f:
    exec(f.read(), version)

setup(
    version=version['__version__'],
    packages=['rabbitleap'],
    install_requires=['pika'])
