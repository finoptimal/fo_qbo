from setuptools import setup, find_packages
from glob import glob

setup(name='fo_qbo',
      version='1.0',
      description="Wrapper around Intuit's QBO API",
      # Note that the tests folder can only be 1 level deep!!! 
      scripts=glob('tests/*'),
      py_modules=[],
      packages=find_packages())
