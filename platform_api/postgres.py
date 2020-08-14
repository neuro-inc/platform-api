import asyncio
from contextlib import asynccontextmanager

import alembic
from asyncpg import create_pool
from asyncpg.pool import Pool

from .config import PostgresConfig


@asynccontextmanager
async def create_postgres_pool(db_config: PostgresConfig) -> Pool:
    async with create_pool(
        dsn=db_config.postgres_dsn,
        min_size=db_config.pool_min_size,
        max_size=db_config.pool_max_size,
        timeout=db_config.connect_timeout_s,
        command_timeout=db_config.command_timeout_s,
    ) as pool:
        yield pool


class MigrationRunner:
    def __init__(self, db_config: PostgresConfig) -> None:
        self._db_config = db_config
        self._loop = asyncio.get_event_loop()

    def _upgrade(self) -> None:
        alembic.command.upgrade(self._db_config.alembic, "head")

    async def upgrade(self) -> None:
        await self._loop.run_in_executor(None, self._upgrade)

    def _downgrade(self) -> None:
        alembic.command.downgrade(self._db_config.alembic, "base")

    async def downgrade(self) -> None:
        await self._loop.run_in_executor(None, self._downgrade)
