from typing import Any, AsyncIterator, Dict, List, Optional

import sqlalchemy.sql as sasql
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class BasePostgresStorage:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def _execute(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> str:
        if conn:
            return await conn.execute(query)
        async with self._engine.connect() as conn:
            return await conn.execute(query)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> Optional[Dict[str, Any]]:
        if conn:
            result = await conn.execute(query)
            return result.one_or_none()
        async with self._engine.connect() as conn:
            result = await conn.execute(query)
            return result.one_or_none()

    async def _fetch(
        self, query: sasql.ClauseElement, conn: Optional[AsyncConnection] = None
    ) -> List[Dict[str, Any]]:
        if conn:
            result = await conn.execute(query)
            return result.all()
        async with self._engine.connect() as conn:
            result = await conn.execute(query)
            return result.all()

    async def _cursor(
        self, query: sasql.ClauseElement, conn: AsyncConnection
    ) -> AsyncIterator[Dict[str, Any]]:
        return await conn.stream(query)
