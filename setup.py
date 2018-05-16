from setuptools import setup, find_packages


install_requires = (
    'dataclasses==0.5',  # backport from 3.7 stdlib
)

setup(
    name='platform-api',
    version='0.0.1b1',
    url='https://github.com/neuromation/platform-api',
    packages=find_packages(),
    install_requires=install_requires,
)