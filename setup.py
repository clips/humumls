# -*- coding: utf-8 -*-
"""Setup file."""
from setuptools import setup
from setuptools import find_packages

setup(name='humumls',
      version='1.0.0',
      description='UMLS in mongoDB',
      author='Stéphan Tulkens',
      author_email='stephan.tulkens@uantwerpen.be',
      url='https://github.com/clips/humumls',
      license='GPL v3',
      packages=find_packages(exclude=['examples']),
      classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3'],
      keywords='umls snomed mongodb',
      zip_safe=True)
