from setuptools import setup, find_packages


install_requires = (
    'aiohttp==3.4.3',
    # WARN: aioredis does not support Redis Cluster yet
    'aioredis==1.1.0',
    'async-exit-stack==1.0.1',  # backport from 3.7 stdlib
    'async-generator==1.9',
    'dataclasses==0.5',  # backport from 3.7 stdlib
    'iso8601==0.1.12',
    'trafaret==1.1.1',
    'neuro_auth_client==0.0.1b3',
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
