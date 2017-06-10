import os
import setuptools

setuptools.setup(
  name='dataflow-snippets',
  version='0.0.1',
  install_requires=[
    'pygeoip',
    ],
  packages=setuptools.find_packages(),
  package_data={
   'resources': ['GeoIP.dat'],     # All files from folder A
   },
)