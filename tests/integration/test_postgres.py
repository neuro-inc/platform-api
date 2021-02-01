import asyncio

import pytest
from asyncpg.pool import Pool

from platform_api.cluster import ClusterUpdateNotifier


@pytest.mark.asyncio
async def test_postgres_available(postgres_pool: Pool) -> None:
    async with postgres_pool.acquire() as connection:
        result = await connection.fetchrow("SELECT 2 + 2;")
        assert result[0] == 4


@pytest.mark.asyncio
async def test_cluster_update_notifier(postgres_pool: Pool) -> None:
    notifier = ClusterUpdateNotifier(postgres_pool)
    count = 0

    async def assert_count(expected_count: int) -> None:
        async def _loop() -> None:
            while count != expected_count:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(_loop(), timeout=5)
        except asyncio.TimeoutError:
            assert count == expected_count

    def callback() -> None:
        nonlocal count
        count += 1

    async with notifier.listen_to_cluster_update(callback):
        await notifier.notify_cluster_update()
        await assert_count(1)
        await notifier.notify_cluster_update()
        await assert_count(2)
    await notifier.notify_cluster_update()
    await asyncio.sleep(0.2)
    await assert_count(2)
