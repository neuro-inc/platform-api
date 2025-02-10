import asyncio

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.utils.update_notifier import (
    Notifier,
    PostgresChannelNotifier,
    ResubscribingNotifier,
)
from tests.unit.test_notifier import Counter


async def test_postgres_available(sqalchemy_engine: AsyncEngine) -> None:
    async with sqalchemy_engine.connect() as connection:
        result = await connection.execute(sa.text("SELECT 2 + 2;"))
        row = result.one()
        assert row == (4,)


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


async def test_channel_notifier_connection_lost(sqalchemy_engine: AsyncEngine) -> None:
    notifier: Notifier = PostgresChannelNotifier(sqalchemy_engine, "channel")
    notifier = ResubscribingNotifier(notifier, check_interval=0.1)
    counter = Counter()

    async with notifier.listen_to_updates(counter.callback) as subscriber:
        await notifier.notify()
        await counter.assert_count(1)

        # Kill 'LISTEN ...' connections
        async with sqalchemy_engine.connect() as conn:
            pid_rows = (
                await conn.execute(
                    sa.text(
                        "SELECT pid, query FROM pg_stat_activity "
                        "WHERE query LIKE 'LISTEN%';"
                    )
                )
            ).all()
        for pid_row in pid_rows:
            async with sqalchemy_engine.connect() as conn:
                await conn.execute(
                    sa.text(f"SELECT pg_terminate_backend({pid_row['pid']});")
                )

        # Instead of a fixed sleep, wait until the subscriber reconnects.
        max_attempts = 50  # maximum total wait time of 5 seconds (50 * 0.1)
        for _ in range(max_attempts):
            if await subscriber.is_alive():
                break
            await asyncio.sleep(0.1)
        else:
            raise Exception(
                "Subscriber did not reconnect in time after killing LISTEN connections"
            )

        # Now that we know the connection is alive again, send a notification.
        await notifier.notify()
        await counter.assert_count(2)
