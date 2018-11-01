import asyncio
from typing import Deque, Optional


class Stream:
    """Single writer single reader bytes stream with cancellation.

    Should be used for kube exec channels.
    """

    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._waiter: Optional[asyncio.Future[None]] = None
        self._data: Deque[bytes] = Deque()
        self._closed = False

    @property
    def closed(self):
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

    async def put(self, line: bytes) -> None:
        if self._closed:
            raise RuntimeError("The stream is closed")
        self._data.append(line)
        waiter = self._waiter
        if waiter is not None:
            if not waiter.done():
                waiter.set_result(None)
            self._waiter = None

    async def get(self) -> bytes:
        if self._closed:
            raise asyncio.CancelledError()
        if not self._data:
            assert self._waiter is None
            self._waiter = self._loop.create_future()
            await self._waiter
            assert self._waiter is None
        line = self._data.popleft()
        return line
