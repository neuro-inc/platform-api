from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.config import PostgresConfig
from platform_api.config_factory import EnvironConfigFactory
from platform_api.postgres import MigrationRunner, make_async_engine


@pytest.fixture
async def postgres_config(k8s_postgres_api_dsn: str) -> AsyncIterator[PostgresConfig]:
    db_config = PostgresConfig(
        postgres_dsn=k8s_postgres_api_dsn,
        alembic=EnvironConfigFactory().create_alembic(k8s_postgres_api_dsn),
    )
    migration_runner = MigrationRunner(db_config)
    await migration_runner.upgrade()

    yield db_config

    await migration_runner.downgrade()


@pytest.fixture
async def sqalchemy_engine(
    postgres_config: PostgresConfig,
) -> AsyncIterator[AsyncEngine]:
    engine = make_async_engine(postgres_config)
    try:
        yield engine
    finally:
        await engine.dispose()
