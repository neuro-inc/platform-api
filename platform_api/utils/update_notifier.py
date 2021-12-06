import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncContextManager, Callable, List, Optional

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine
from typing_extensions import AsyncIterator

from platform_api.orchestrator.base_postgres_storage import _safe_connect


logger = logging.getLogger(__name__)


Callback = Callable[[], None]


class Subscription(ABC):
    async def is_alive(self) -> bool:
        pass


class Notifier(ABC):
    @abstractmethod
    async def notify(self) -> None:
        pass

    @abstractmethod
    def listen_to_updates(
        self, listener: Callback
    ) -> AsyncContextManager[Subscription]:
        pass


class InMemoryNotifier(Notifier):
    def __init__(self) -> None:
        self._callbacks: List[Callback] = []

    async def notify(self) -> None:
        for callback in self._callbacks:
            callback()

    class _Subscription(Subscription):
        async def is_alive(self) -> bool:
            return True

    @asynccontextmanager
    async def listen_to_updates(
        self, listener: Callback
    ) -> AsyncIterator[Subscription]:
        self._callbacks.append(listener)
        yield InMemoryNotifier._Subscription()
        self._callbacks.remove(listener)


class PostgresChannelNotifier(Notifier):
    def __init__(self, engine: AsyncEngine, channel: str) -> None:
        self._engine = engine
        self._channel = channel

    @asynccontextmanager
    async def _raw_connect(self) -> AsyncIterator[asyncpg.Connection]:
        async with _safe_connect(self._engine.connect()) as conn:
            connection_fairy = await conn.get_raw_connection()
            connection_fairy.detach()
            raw_connection = connection_fairy.driver_connection
            try:
                yield raw_connection
            finally:
                await raw_connection.close()

    async def notify(self) -> None:
        async with self._raw_connect() as raw_conn:
            logger.info(f"Notifying channel {self._channel!r}")
            await raw_conn.fetch(f"NOTIFY {self._channel}")

    class _Subscription(Subscription):
        def __init__(self, conn: asyncpg.Connection) -> None:
            self._conn = conn

        async def is_alive(self) -> bool:
            try:
                await self._conn.fetchrow("SELECT 42")
            except asyncio.CancelledError:
                raise
            except Exception:
                return False
            return True

    @asynccontextmanager
    async def listen_to_updates(
        self, listener: Callback
    ) -> AsyncIterator[Subscription]:
        def _listener(*args: Any, **kwargs: Any) -> None:
            logger.info(
                f"{type(self).__qualname__}: Notified "
                f"from channel {self._channel!r}"
            )
            listener()

        async with self._raw_connect() as raw_conn:
            logger.info(
                f"{type(self).__qualname__}: Subscribing to channel {self._channel!r}"
            )
            await raw_conn.add_listener(self._channel, _listener)
            try:
                yield PostgresChannelNotifier._Subscription(raw_conn)
            finally:
                logger.info(
                    f"{type(self).__qualname__}: Unsubscribing "
                    f"from channel {self._channel!r}"
                )
                await raw_conn.remove_listener(self._channel, _listener)


class ResubscribingNotifier(Notifier):
    def __init__(self, notifier: Notifier, *, check_interval: float):
        self._inner_notifier = notifier
        self._check_interval = check_interval

    async def notify(self) -> None:
        await self._inner_notifier.notify()

    class _Subscription(Subscription):
        _inner_manager: Optional[AsyncContextManager[Subscription]] = None
        _subscription: Optional[Subscription] = None
        _task: Optional["asyncio.Task[None]"] = None

        def __init__(
            self, notifier: Notifier, callback: Callback, check_interval: float
        ):
            self._notifier = notifier
            self._callback = callback
            self._check_interval = check_interval
            self._lock = asyncio.Lock()

        async def is_alive(self) -> bool:
            if not self._subscription:
                return False
            return await self._subscription.is_alive()

        async def _setup_subscription(self) -> None:
            async with self._lock:
                self._inner_manager = self._notifier.listen_to_updates(self._callback)
                self._subscription = await self._inner_manager.__aenter__()

        async def _teardown_subscription(
            self, aexit_args: Any = (None, None, None)
        ) -> None:
            async with self._lock:
                if self._inner_manager:
                    await self._inner_manager.__aexit__(*aexit_args)
                self._inner_manager = None
                self._subscription = None

        async def _checker_task(self) -> None:
            while True:
                await asyncio.sleep(self._check_interval)
                if self._subscription and not await self._subscription.is_alive():
                    try:
                        await asyncio.shield(self._teardown_subscription())
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            f"{type(self).__qualname__}: Failed to cleanup subscription"
                        )
                    await asyncio.shield(self._setup_subscription())

        async def __aenter__(self) -> "ResubscribingNotifier._Subscription":
            await self._setup_subscription()
            self._task = asyncio.create_task(self._checker_task())
            return self

        async def __aexit__(self, *args: Any) -> None:
            assert self._task
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            await self._teardown_subscription(args)

    @asynccontextmanager
    async def listen_to_updates(
        self, listener: Callback
    ) -> AsyncIterator[Subscription]:
        async with ResubscribingNotifier._Subscription(
            self._inner_notifier, listener, self._check_interval
        ) as subscription:
            yield subscription
