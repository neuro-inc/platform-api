import asyncio
from collections import deque


class Stream:
    """Single writer single reader bytes stream with cancellation.

    Should be used for kube exec channels.
    """

    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._waiter: asyncio.Future[None] | None = None
        self._data: deque[bytes] = deque()
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        # Cancel the waiter if any
        if self._closed:
            return
        waiter = self._waiter
        if waiter is not None:
            waiter.cancel()
            self._waiter = None
        self._closed = True

    async def feed(self, data: bytes) -> None:
        if self._closed:
            raise RuntimeError("The stream is closed")
        self._data.append(data)
        waiter = self._waiter
        if waiter is not None:
            if not waiter.done():
                waiter.set_result(None)
            self._waiter = None

    async def read(self) -> bytes:
        if not self._data:
            if self._closed:
                raise asyncio.CancelledError()
            assert self._waiter is None
            self._waiter = self._loop.create_future()
            await self._waiter
            assert self._waiter is None
        data = self._data.popleft()
        return data
