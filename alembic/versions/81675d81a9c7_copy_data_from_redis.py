"""copy data from redis

Revision ID: 81675d81a9c7
Revises: eaa33ba10d63
Create Date: 2020-09-11 18:19:36.310016

"""
import asyncio
from typing import Tuple

from alembic import context

from platform_api.config import PostgresConfig
from platform_api.orchestrator.jobs_storage import PostgresJobsStorage, RedisJobsStorage
from platform_api.postgres import create_postgres_pool
from platform_api.redis import RedisConfig, create_redis_client


# revision identifiers, used by Alembic.

revision = "81675d81a9c7"
down_revision = "eaa33ba10d63"
branch_labels = None
depends_on = None


async def move_redis_to_postgres(
    redis_config: RedisConfig, postgres_config: PostgresConfig
):
    async with create_redis_client(redis_config) as redis, create_postgres_pool(
        postgres_config
    ) as postgres:
        redis_storage = RedisJobsStorage(redis)
        postgres_storage = PostgresJobsStorage(postgres)
        async for job in redis_storage.iter_all_jobs():
            await postgres_storage.set_job(job)


async def move_postgres_to_redis(
    redis_config: RedisConfig, postgres_config: PostgresConfig
):
    async with create_redis_client(redis_config) as redis, create_postgres_pool(
        postgres_config
    ) as postgres:
        redis_storage = RedisJobsStorage(redis)
        postgres_storage = PostgresJobsStorage(postgres)
        async for job in postgres_storage.iter_all_jobs():
            await redis_storage.set_job(job)


def _make_configs() -> Tuple[RedisConfig, PostgresConfig]:
    # We need this to be able to run tests properly
    redis_config = RedisConfig(uri=context.config.get_main_option("redis_url"))
    postgres_config = PostgresConfig(
        postgres_dsn=context.config.get_main_option("sqlalchemy.url"),
        alembic=None,  # noqa
    )
    assert redis_config.uri, (
        "Data migration from redis to postgres cannot run " "without redis url set"
    )
    return redis_config, postgres_config


def upgrade():
    asyncio.run(move_redis_to_postgres(*_make_configs()))


def downgrade():
    asyncio.run(move_redis_to_postgres(*_make_configs()))
