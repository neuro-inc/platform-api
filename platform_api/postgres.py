import asyncio

import alembic
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .config import PostgresConfig


def make_async_engine(db_config: PostgresConfig) -> AsyncEngine:
    return create_async_engine(
        db_config.postgres_dsn,
        pool_size=db_config.pool_min_size,
        max_overflow=max(0, db_config.pool_max_size - db_config.pool_min_size),
        pool_timeout=db_config.connect_timeout_s,
    )


class MigrationRunner:
    def __init__(self, db_config: PostgresConfig) -> None:
        self._db_config = db_config
        self._loop = asyncio.get_event_loop()

    def _upgrade(self, version: str) -> None:
        alembic.command.upgrade(self._db_config.alembic, version)

    async def upgrade(self, version: str = "head") -> None:
        await self._loop.run_in_executor(None, self._upgrade, version)

    def _downgrade(self, version: str) -> None:
        alembic.command.downgrade(self._db_config.alembic, version)

    async def downgrade(self, version: str = "base") -> None:
        await self._loop.run_in_executor(None, self._downgrade, version)
