from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.6.2",
    # WARN: aioredis does not support Redis Cluster yet
    "aioredis==1.3.1",
    "iso8601==0.1.12",
    "trafaret==2.0.2",
    "neuro_auth_client==19.11.25",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==2.9",
    "aiorwlock==0.6.0",
    "notifications-client==0.8.2",
    "platform-logging==0.3",
    "yarl==1.3.0",
    "multidict==4.7.6",
    "aiohttp-cors==0.7.0",
    "aiozipkin==0.7.0",
)

setup(
    name="platform-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-api",
    packages=find_packages(),
    python_requires=">=3.7.0",
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
