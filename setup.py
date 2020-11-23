from setuptools import find_packages, setup


install_requires = (
    "aiohttp==3.7.3",
    # WARN: aioredis does not support Redis Cluster yet
    "aioredis==1.3.1",
    "iso8601==0.1.13",
    "trafaret==1.2.0",
    "neuro_auth_client==20.11.10",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==3.2.1",
    "aiorwlock==1.0.0",
    "notifications-client==0.8.2",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "aiozipkin==1.0.0",
    "asyncpg==0.21.0",
    "sqlalchemy==1.3.20",
    "asyncpgsa==0.26.3",
    "alembic==1.4.3",
    "psycopg2-binary==2.8.6",
    "sentry-sdk==0.19.4",
)

setup(
    name="platform-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-api",
    packages=find_packages(),
    python_requires=">=3.7.0",
    install_requires=install_requires,
    entry_points={"console_scripts": ["platform-api=platform_api.api:main"]},
    zip_safe=False,
)
