from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiohttp==3.7.4.post0",
    "iso8601==0.1.14",
    "trafaret==1.2.0",
    "neuro-auth-client==21.9.13.1",
    # Circle CI fails on the latest cryptography version
    # because the server has too old OpenSSL version
    "cryptography==3.3.2",
    "aiorwlock==1.0.0",
    "neuro-notifications-client==21.9.11.1",
    "neuro-logging==21.8.4.1",
    "aiohttp-cors==0.7.0",
    "aiozipkin==1.1.0",
    "asyncpg==0.23.0",
    "sqlalchemy==1.3.23",
    "asyncpgsa==0.27.1",
    "alembic==1.6.2",
    "psycopg2-binary==2.9.1",
    "sentry-sdk==1.1.0",
    "typing-extensions==3.10.0.0",
    "neuro-admin-client==21.9.2.1",
)

setup(
    name="platform-api",
    url="https://github.com/neuro-inc/platform-api",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    packages=find_packages(),
    python_requires=">=3.8.0",
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
