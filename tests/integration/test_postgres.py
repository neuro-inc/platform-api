import asyncio

import asyncpg
import pytest
from asyncpg.pool import Pool

from platform_api.cluster import ClusterUpdateNotifier


@pytest.mark.asyncio
async def test_postgres_available(postgres_pool: Pool) -> None:
    async with postgres_pool.acquire() as connection:
        result = await connection.fetchrow("SELECT 2 + 2;")
        assert result[0] == 4


class Counter:
    def __init__(self) -> None:
        self.count = 0

    def callback(self) -> None:
        self.count += 1

    async def assert_count(self, expected_count: int) -> None:
        async def _loop() -> None:
            while self.count != expected_count:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(_loop(), timeout=1)
        except asyncio.TimeoutError:
            assert self.count == expected_count


@pytest.mark.asyncio
async def test_cluster_update_notifier(postgres_pool: Pool) -> None:
    notifier = ClusterUpdateNotifier(postgres_pool)
    counter = Counter()

    async with notifier.listen_to_cluster_update(counter.callback):
        await notifier.notify_cluster_update()
        await counter.assert_count(1)
        await notifier.notify_cluster_update()
        await counter.assert_count(2)
    await notifier.notify_cluster_update()
    await asyncio.sleep(0.2)
    await counter.assert_count(2)


@pytest.mark.asyncio
async def test_cluster_update_notifier_connection_lost(postgres_pool: Pool) -> None:
    notifier = ClusterUpdateNotifier(postgres_pool, heartbeat_interval_sec=0.1)
    counter = Counter()

    async with notifier.listen_to_cluster_update(counter.callback):
        await notifier.notify_cluster_update()
        await counter.assert_count(1)

        # Kill 'LISTEN ...' connections
        pid_rows = await postgres_pool.fetch(
            "SELECT pid, query FROM pg_stat_activity where QUERY like 'LISTEN%';"
        )
        for pid_row in pid_rows:
            await postgres_pool.execute(
                f"SELECT pg_terminate_backend({pid_row['pid']});"
            )
        await asyncio.sleep(0.2)  # allow it to reconnect

        await notifier.notify_cluster_update()
        await counter.assert_count(2)


@pytest.mark.asyncio
async def test_cluster_update_notifier_failed_to_subscribe(postgres_pool: Pool) -> None:
    await postgres_pool.close()
    notifier = ClusterUpdateNotifier(postgres_pool)

    with pytest.raises(asyncpg.InterfaceError):
        async with notifier.listen_to_cluster_update(lambda: None):
            pass
