import asyncio

import asyncpg
import pytest
from asyncpg.pool import Pool

from platform_api.utils.update_notifier import (
    Notifier,
    PostgresChannelNotifier,
    ResubscribingNotifier,
)
from tests.unit.test_notifier import Counter


@pytest.mark.asyncio
async def test_postgres_available(postgres_pool: Pool) -> None:
    async with postgres_pool.acquire() as connection:
        result = await connection.fetchrow("SELECT 2 + 2;")
        assert result[0] == 4


@pytest.mark.asyncio
async def test_channel_notifier(postgres_pool: Pool) -> None:
    notifier = PostgresChannelNotifier(postgres_pool, "channel")
    counter = Counter()

    async with notifier.listen_to_updates(counter.callback):
        await notifier.notify()
        await counter.assert_count(1)
        await notifier.notify()
        await counter.assert_count(2)
    await notifier.notify()
    await asyncio.sleep(0.2)
    await counter.assert_count(2)


@pytest.mark.asyncio
async def test_channel_notifier_connection_lost(postgres_pool: Pool) -> None:
    notifier: Notifier = PostgresChannelNotifier(postgres_pool, "channel")
    notifier = ResubscribingNotifier(notifier, check_interval=0.1)
    counter = Counter()

    async with notifier.listen_to_updates(counter.callback):
        await notifier.notify()
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

        await notifier.notify()
        await counter.assert_count(2)


@pytest.mark.asyncio
async def test_cluster_update_notifier_failed_to_subscribe(postgres_pool: Pool) -> None:
    await postgres_pool.close()
    notifier: Notifier = PostgresChannelNotifier(postgres_pool, "channel")
    notifier = ResubscribingNotifier(notifier, check_interval=0.1)

    with pytest.raises(asyncpg.InterfaceError):
        async with notifier.listen_to_updates(lambda: None):
            pass
