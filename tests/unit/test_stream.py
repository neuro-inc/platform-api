import asyncio

import pytest

from platform_api.utils.stream import Stream


async def test_stream_ctor():
    stream = Stream()
    assert not stream.closed


async def test_nonblocking_put_get():
    stream = Stream()
    await stream.put(b"data")
    data = await stream.get()
    assert data == b"data"


async def test_put_in_closed_stream():
    stream = Stream()
    await stream.close()
    with pytest.raises(RuntimeError):
        await stream.put(b"data")


async def test_get_from_closed_stream():
    stream = Stream()
    await stream.close()
    with pytest.raises(asyncio.CancelledError):
        await stream.get()


async def test_blocking_get_before_put():
    loop = asyncio.get_event_loop()
    ready = loop.create_future()

    async def getter():
        ready.set_result(None)
        data = await stream.get()
        assert data == b"data"

    stream = Stream()
    task = loop.create_task(getter())
    await ready
    await stream.put(b"data")
    await task


async def test_blocking_get_cancellation():
    loop = asyncio.get_event_loop()
    ready = loop.create_future()

    async def getter():
        ready.set_result(None)
        await stream.get()

    stream = Stream()
    task = loop.create_task(getter())
    await ready
    await stream.close()

    with pytest.raises(asyncio.CancelledError):
        await task
