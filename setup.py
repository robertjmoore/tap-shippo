#!/usr/bin/env python

from setuptools import setup

setup(name='tap-shippo',
      version='0.6.3',
      description='Singer.io tap for extracting data from the Shippo API',
      author='Robert J. Moore',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_shippo'],
      install_requires=[
          'singer-python==1.6.0',
          'backoff==1.3.2',
          'requests==2.12.4',
      ],
      entry_points='''
          [console_scripts]
          tap-shippo=tap_shippo:main
      ''',
      packages=['tap_shippo'],
      package_data = {
          'tap_shippo/schemas': [
              "addresses.json",
              "parcels.json",
              "refunds.json",
              "shipments.json",
              "transactions.json",
          ],
      },
      include_package_data=True,
)

