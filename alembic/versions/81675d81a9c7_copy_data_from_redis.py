"""copy data from redis

Revision ID: 81675d81a9c7
Revises: eaa33ba10d63
Create Date: 2020-09-11 18:19:36.310016

"""
import asyncio

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
from alembic import context, op

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.jobs_storage import RedisJobsStorage
from platform_api.redis import RedisConfig, create_redis_client


# revision identifiers, used by Alembic.

revision = "81675d81a9c7"
down_revision = "eaa33ba10d63"
branch_labels = None
depends_on = None


def get_job_table():
    metadata = sa.MetaData()
    return sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("cluster_name", sa.String(), nullable=False),
        sa.Column("tags", sapg.JSONB(), nullable=True),
        # Denormalized fields for optimized access/unique constrains checks
        sa.Column("status", sa.String(), nullable=False),
        sa.Column(
            "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
        ),
        sa.Column(
            "finished_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=True
        ),
        # All other fields
        sa.Column("payload", sapg.JSONB(), nullable=False),
    )


async def move_redis_to_postgres(redis_config: RedisConfig):
    async with create_redis_client(redis_config) as redis:
        redis_storage = RedisJobsStorage(redis)
        table = get_job_table()

        async for job in redis_storage.iter_all_jobs():
            payload = job.to_primitive()
            values = {
                "id": payload.pop("id"),
                "owner": payload.pop("owner"),
                "name": payload.pop("name", None),
                "cluster_name": payload.pop("cluster_name"),
                "tags": payload.pop("tags", None),
                "status": job.status_history.current.status,
                "created_at": job.status_history.created_at,
                "finished_at": job.status_history.finished_at,
                "payload": payload,
            }
            op.bulk_insert(table, [values])


async def move_postgres_to_redis(redis_config: RedisConfig):
    async with create_redis_client(redis_config) as redis:
        redis_storage = RedisJobsStorage(redis)
        conn = op.get_bind()
        res = conn.execute("select * from jobs")
        results = res.fetchall()
        for result in results:
            record = dict(result.items())
            payload = record["payload"]
            payload["id"] = record["id"]
            payload["owner"] = record["owner"]
            payload["name"] = record["name"]
            payload["cluster_name"] = record["cluster_name"]
            if record["tags"] is not None:
                payload["tags"] = record["tags"]
            job = JobRecord.from_primitive(payload)
            await redis_storage.set_job(job)


def _make_redis_config() -> RedisConfig:
    # We need this to go through context.config be able to run tests properly
    redis_config = RedisConfig(uri=context.config.get_main_option("redis_url"))
    assert redis_config.uri, (
        "Data migration from redis to postgres cannot run " "without redis url set"
    )
    return redis_config


def upgrade():
    asyncio.run(move_redis_to_postgres(_make_redis_config()))


def downgrade():
    asyncio.run(move_postgres_to_redis(_make_redis_config()))
