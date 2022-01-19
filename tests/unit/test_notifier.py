import asyncio

import pytest

from platform_api.utils.update_notifier import InMemoryNotifier


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


class TestInMemoryNotifier:
    async def test_notifier(self) -> None:
        notifier = InMemoryNotifier()
        counter = Counter()

        async with notifier.listen_to_updates(counter.callback):
            await notifier.notify()
            await counter.assert_count(1)
            await notifier.notify()
            await counter.assert_count(2)

        await notifier.notify()
        await asyncio.sleep(0.2)
        await counter.assert_count(2)
