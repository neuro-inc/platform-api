import asyncio
import sys
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import sqlalchemy.sql as sasql
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class BasePostgresStorage:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        async with _safe_connect(self._engine.begin()) as conn:
            yield conn

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[AsyncConnection]:
        async with _safe_connect(self._engine.connect()) as conn:
            yield conn

    async def _execute(
        self, query: sasql.ClauseElement, conn: AsyncConnection | None = None
    ) -> None:
        if conn:
            await conn.execute(query)
            return
        async with self._connect() as conn:
            await conn.execute(query)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: AsyncConnection | None = None
    ) -> Row | None:
        if conn:
            result = await conn.execute(query)
            return result.one_or_none()
        async with self._connect() as conn:
            result = await conn.execute(query)
            return result.one_or_none()

    async def _fetch(
        self, query: sasql.ClauseElement, conn: AsyncConnection | None = None
    ) -> list[Row]:
        if conn:
            result = await conn.execute(query)
            return result.all()
        async with self._connect() as conn:
            result = await conn.execute(query)
            return result.all()

    @asynccontextmanager
    async def _cursor(
        self, query: sasql.ClauseElement
    ) -> AsyncIterator[AsyncIterator[Row]]:
        async with self._transaction() as conn:
            yield await conn.stream(query)


@asynccontextmanager
async def _safe_connect(
    conn_cm: AbstractAsyncContextManager[AsyncConnection],
) -> AsyncConnection:
    # Workaround of the SQLAlchemy bug.
    conn_task = asyncio.create_task(conn_cm.__aenter__())
    try:
        conn = await asyncio.shield(conn_task)
    except asyncio.CancelledError:
        conn = await conn_task
        await conn.close()
        raise
    try:
        yield conn
    except:  # noqa
        if not await conn_cm.__aexit__(*sys.exc_info()):
            raise
    else:
        await conn_cm.__aexit__(None, None, None)
