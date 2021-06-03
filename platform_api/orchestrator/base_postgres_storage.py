from typing import List, Optional

import asyncpgsa
import sqlalchemy.sql as sasql
from asyncpg import Connection, Pool
from asyncpg.cursor import CursorFactory
from asyncpg.protocol.protocol import Record


class BasePostgresStorage:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool

    async def _execute(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> str:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.execute(query_string, *params)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> Optional[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.fetchrow(query_string, *params)

    async def _fetch(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> List[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.fetch(query_string, *params)

    def _cursor(self, query: sasql.ClauseElement, conn: Connection) -> CursorFactory:
        query_string, params = asyncpgsa.compile_query(query)
        return conn.cursor(query_string, *params)
