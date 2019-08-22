from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.5.4",
    # WARN: aioredis does not support Redis Cluster yet
    "aioredis==1.2.0",
    "async-exit-stack==1.0.1",  # backport from 3.7 stdlib
    "async-generator==1.10",
    "dataclasses==0.6",  # backport from 3.7 stdlib
    "iso8601==0.1.12",
    "trafaret==1.2.0",
    "neuro_auth_client==1.0.7",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==2.7",
    "asyncssh==1.17.0",
    "aiorwlock==0.6.0",
    "notifications-client==0.6rc1",
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
            "ssh-authorize=platform_api.ssh_auth.authorize:main",
        ]
    },
    zip_safe=False,
)
