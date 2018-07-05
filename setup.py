from setuptools import setup, find_packages


install_requires = (
    'aiohttp==3.3.2',
    'async-generator==1.9',
    'dataclasses==0.5',  # backport from 3.7 stdlib
    'trafaret==1.1.1'
)

setup(
    name='platform-api',
    version='0.0.1b1',
    url='https://github.com/neuromation/platform-api',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        'console_scripts': 'platform-api=platform_api.api:main'
    },
)
