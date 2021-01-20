from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiohttp==3.7.3",
    # WARN: aioredis does not support Redis Cluster yet
    "aioredis==1.3.1",
    "iso8601==0.1.13",
    "trafaret==1.2.0",
    "neuro_auth_client==21.1.6",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==3.2.1",
    "aiorwlock==1.0.0",
    "notifications-client==20.1.4",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "aiozipkin==1.0.0",
    "asyncpg==0.21.0",
    "sqlalchemy==1.3.22",
    "asyncpgsa==0.26.3",
    "alembic==1.5.1",
    "psycopg2-binary==2.8.6",
    "sentry-sdk==0.19.5",
)

setup(
    name="platform-api",
    url="https://github.com/neuromation/platform-api",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    packages=find_packages(),
    python_requires=">=3.7.0",
    setup_requires=setup_requires,
    install_requires=install_requires,
    entry_points={"console_scripts": ["platform-api=platform_api.api:main"]},
    zip_safe=False,
)
