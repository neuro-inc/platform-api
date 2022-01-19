import asyncio

import pytest

from platform_api.utils.stream import Stream


async def test_stream_ctor() -> None:
    stream = Stream()
    assert not stream.closed


async def test_nonblocking_put_get() -> None:
    stream = Stream()
    await stream.feed(b"data")
    data = await stream.read()
    assert data == b"data"


async def test_put_in_closed_stream() -> None:
    stream = Stream()
    await stream.close()
    with pytest.raises(RuntimeError):
        await stream.feed(b"data")


async def test_get_from_closed_stream() -> None:
    stream = Stream()
    await stream.close()
    with pytest.raises(asyncio.CancelledError):
        await stream.read()


async def test_blocking_get_before_put() -> None:
    loop = asyncio.get_event_loop()
    ready = loop.create_future()

    async def getter() -> None:
        ready.set_result(None)
        data = await stream.read()
        assert data == b"data"

    stream = Stream()
    task = loop.create_task(getter())
    await ready
    await stream.feed(b"data")
    await task


async def test_blocking_get_cancellation() -> None:
    loop = asyncio.get_event_loop()
    ready = loop.create_future()

    async def getter() -> None:
        ready.set_result(None)
        await stream.read()

    stream = Stream()
    task = loop.create_task(getter())
    await ready
    await stream.close()

    with pytest.raises(asyncio.CancelledError):
        await task
