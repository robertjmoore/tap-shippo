#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-shippo',
      version='0.1.0',
      description='Singer Tap for Shippo',
      author='Robert J. Moore',
      url='https://github.com/robertjmoore/tap-shippo',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_shippo'],
      install_requires=['stitchstream-python>=0.6.0',
                        'requests==2.12.4'],
      entry_points='''
          [console_scripts]
          tap-shippo=tap_shippo:main
      ''',
)

