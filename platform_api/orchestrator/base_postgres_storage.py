from contextlib import asynccontextmanager
from typing import AsyncIterator, List, Optional

import sqlalchemy.sql as sasql
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class BasePostgresStorage:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self._engine.begin() as conn:
            yield conn

    async def _execute(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> None:
        if conn:
            await conn.execute(query)
            return
        async with self._engine.connect() as conn:
            await conn.execute(query)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> Optional[Row]:
        if conn:
            result = await conn.execute(query)
            return result.one_or_none()
        async with self._engine.connect() as conn:
            result = await conn.execute(query)
            return result.one_or_none()

    async def _fetch(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> List[Row]:
        if conn:
            result = await conn.execute(query)
            return result.all()
        async with self._engine.connect() as conn:
            result = await conn.execute(query)
            return result.all()

    @asynccontextmanager
    async def _cursor(
        self, query: sasql.ClauseElement
    ) -> AsyncIterator[AsyncIterator[Row]]:
        async with self._engine.begin() as conn:
            yield await conn.stream(query)
