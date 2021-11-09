from typing import AsyncIterator, List, Optional

import sqlalchemy.row as Row
import sqlalchemy.sql as sasql
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class BasePostgresStorage:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

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

    async def _cursor(
        self, query: sasql.ClauseElement, conn: AsyncConnection
    ) -> AsyncIterator[Row]:
        return await conn.stream(query)
