import asyncio
import logging

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.utils.update_notifier import (
    Notifier,
    PostgresChannelNotifier,
    ResubscribingNotifier,
)
from tests.unit.test_notifier import Counter

logger = logging.getLogger(__name__)


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
    logger.warning("Start test_channel_notifier_connection_lost")
    notifier: Notifier = PostgresChannelNotifier(sqalchemy_engine, "channel")
    notifier = ResubscribingNotifier(notifier, check_interval=0.1)
    counter = Counter()

    logger.warning("Start test_channel_notifier_connection_lost")
    async with notifier.listen_to_updates(counter.callback):
        await notifier.notify()
        await counter.assert_count(1)

        # Kill 'LISTEN ...' connections
        logger.warning(
            "Kill 'LISTEN ...' connections for test_channel_notifier_connection_lost"
        )
        async with sqalchemy_engine.connect() as conn:
            pid_rows = (
                await conn.execute(
                    sa.text(
                        "SELECT pid, query FROM pg_stat_activity "
                        "where QUERY like 'LISTEN%';"
                    )
                )
            ).all()
        logger.warning(
            "Killed 'LISTEN ...' connections for test_channel_notifier_connection_lost"
        )
        for pid_row in pid_rows:
            logger.warning(
                "pid_row %s for test_channel_notifier_connection_lost", pid_row["pid"]
            )
            async with sqalchemy_engine.connect() as conn:
                await conn.execute(
                    sa.text(f"SELECT pg_terminate_backend({pid_row['pid']});")
                )

        logger.warning("sleep for test_channel_notifier_connection_lost")
        await asyncio.sleep(0.2)  # allow it to reconnect
        logger.warning("notify for test_channel_notifier_connection_lost")
        await notifier.notify()
        await counter.assert_count(2)
