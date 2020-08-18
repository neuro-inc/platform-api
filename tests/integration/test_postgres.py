import pytest
from asyncpg.pool import Pool


@pytest.mark.asyncio
async def test_postgres_available(postgres_pool: Pool) -> None:
    async with postgres_pool.acquire() as connection:
        result = await connection.fetchrow("SELECT 2 + 2;")
        assert result[0] == 4
