import asyncio

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.utils.update_notifier import (
    Notifier,
    PostgresChannelNotifier,
    ResubscribingNotifier,
)
from tests.unit.test_notifier import Counter


@pytest.mark.asyncio
async def test_postgres_available(sqalchemy_engine: AsyncEngine) -> None:
    async with sqalchemy_engine.connect() as connection:
        result = await connection.execute(sa.text("SELECT 2 + 2;"))
        row = result.one()
        assert row == (4,)


@pytest.mark.asyncio
async def test_channel_notifier(sqalchemy_engine: AsyncEngine) -> None:
    notifier = PostgresChannelNotifier(sqalchemy_engine, "channel")
    counter = Counter()

    async with notifier.listen_to_updates(counter.callback) as s:
        assert await s.is_alive()
        await notifier.notify()
        assert await s.is_alive()
        await counter.assert_count(1)
        await notifier.notify()
        await counter.assert_count(2)
    await notifier.notify()
    await asyncio.sleep(0.2)
    await counter.assert_count(2)


@pytest.mark.asyncio
async def test_channel_notifier_connection_lost(sqalchemy_engine: AsyncEngine) -> None:
    notifier: Notifier = PostgresChannelNotifier(sqalchemy_engine, "channel")
    notifier = ResubscribingNotifier(notifier, check_interval=0.1)
    counter = Counter()

    async with notifier.listen_to_updates(counter.callback):
        await notifier.notify()
        await counter.assert_count(1)

        # Kill 'LISTEN ...' connections
        async with sqalchemy_engine.connect() as conn:
            pid_rows = await conn.execute(
                sa.text(
                    "SELECT pid, query FROM pg_stat_activity "
                    "where QUERY like 'LISTEN%';"
                )
            )
        for pid_row in pid_rows.all():
            async with sqalchemy_engine.connect() as conn:
                await conn.execute(
                    sa.text(f"SELECT pg_terminate_backend({pid_row['pid']});")
                )
        await asyncio.sleep(0.2)  # allow it to reconnect

        await notifier.notify()
        await counter.assert_count(2)
