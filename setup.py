from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.5.0a1",
    # WARN: aioredis does not support Redis Cluster yet
    "aioredis==1.1.0",
    "async-exit-stack==1.0.1",  # backport from 3.7 stdlib
    "async-generator==1.9",
    "dataclasses==0.6",  # backport from 3.7 stdlib
    "iso8601==0.1.12",
    "trafaret==1.1.1",
    "neuro_auth_client==0.0.1b4",
)

setup(
    name="platform-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-api",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": [
            "platform-api=platform_api.api:main",
            "api-ssh-server=platform_api.ssh.server:main",
        ]
    },
    zip_safe=False,
)
