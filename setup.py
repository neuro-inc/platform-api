from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiohttp==3.7.4.post0",
    "iso8601==0.1.14",
    "trafaret==1.2.0",
    "neuro_auth_client==21.4.24",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==3.3.2",
    "aiorwlock==1.0.0",
    "notifications-client==21.4.21",
    "platform-logging==21.5.7",
    "aiohttp-cors==0.7.0",
    "aiozipkin==1.0.0",
    "asyncpg==0.22.0",
    "sqlalchemy==1.3.23",
    "asyncpgsa==0.27.1",
    "alembic==1.6.2",
    "psycopg2-binary==2.8.6",
    "sentry-sdk==1.1.0",
    "typing-extensions==3.7.4.3",
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
    entry_points={
        "console_scripts": [
            "platform-api=platform_api.api:main",
            "platform-api-poller=platform_api.poller_main:main",
        ]
    },
    zip_safe=False,
)
