import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
)

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.sql as sasql
from aiorwlock import RWLock
from async_generator import asynccontextmanager
from asyncpg import Connection
from asyncpg.pool import Pool
from typing_extensions import Protocol

from .cluster_config import ClusterConfig
from .config import Config
from .config_client import ConfigClient
from .orchestrator.base import Orchestrator


logger = logging.getLogger(__name__)


class ClusterException(Exception):
    pass


class ClusterNotFound(ClusterException):
    @classmethod
    def create(cls, name: str) -> "ClusterNotFound":
        return cls(f"Cluster '{name}' not found")


class ClusterNotAvailable(ClusterException):
    @classmethod
    def create(cls, name: str) -> "ClusterNotAvailable":
        return cls(f"Cluster '{name}' not available")


class Cluster(ABC):
    @abstractmethod
    async def init(self) -> None:  # pragma: no cover
        pass

    @abstractmethod
    async def close(self) -> None:  # pragma: no cover
        pass

    @property
    @abstractmethod
    def config(self) -> ClusterConfig:  # pragma: no cover
        pass

    @property
    def name(self) -> str:
        return self.config.name

    @property
    @abstractmethod
    def orchestrator(self) -> Orchestrator:  # pragma: no cover
        pass


Callback = Callable[[], None]


class Subscription(Protocol):
    async def is_alive(self) -> bool:
        pass


Subscribe = Callable[[Callback], AsyncContextManager[Subscription]]


class AutoReSubscriber:
    _inner_manager: Optional[AsyncContextManager[Subscription]] = None
    _subscription: Optional[Subscription] = None
    _task: Optional["asyncio.Task[None]"] = None

    def __init__(self, subscribe: Subscribe, callback: Callback, check_interval: float):
        self._subscribe = subscribe
        self._callback = callback
        self._check_interval = check_interval
        self._lock = asyncio.Lock()

    async def _setup_subscription(self) -> None:
        async with self._lock:
            self._inner_manager = self._subscribe(self._callback)
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

    async def __aenter__(self) -> None:
        await self._setup_subscription()
        self._task = asyncio.create_task(self._checker_task())

    async def __aexit__(self, *args: Any) -> None:
        assert self._task
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        await self._teardown_subscription(args)


ClusterFactory = Callable[[ClusterConfig], Cluster]


class ClusterUpdateNotifier:
    def __init__(self, pool: Pool, heartbeat_interval_sec: float = 15) -> None:
        self._pool = pool
        self._channel = "cluster_update_required"
        self._heartbeat_interval_sec = heartbeat_interval_sec

    # Database helpers

    async def _execute(self, query: sasql.ClauseElement) -> str:
        query_string, params = asyncpgsa.compile_query(query)
        return await self._pool.execute(query_string, *params)

    async def notify_cluster_update(self) -> None:
        logger.info(f"Notifying channel {self._channel!r}")
        query = sa.text(f"NOTIFY {self._channel}")
        await self._execute(query)

    class Subscription:
        def __init__(self, conn: Connection):
            self._conn = conn

        async def is_alive(self) -> bool:
            try:
                await self._conn.execute("SELECT 42")
            except asyncio.CancelledError:
                raise
            except Exception:
                return False
            return True

    @asynccontextmanager
    async def _listen_to_cluster_update_once(
        self, listener: Callable[[], None]
    ) -> AsyncIterator[Subscription]:
        def _listener(*args: Any) -> None:
            listener()

        async with self._pool.acquire() as conn:
            logger.info(
                f"{type(self).__qualname__}: Subscribing to channel {self._channel!r}"
            )
            await conn.add_listener(self._channel, _listener)
            try:
                yield ClusterUpdateNotifier.Subscription(conn)
            finally:
                logger.info(
                    f"{type(self).__qualname__}: Unsubscribing "
                    f"from channel {self._channel!r}"
                )
                await conn.remove_listener(self._channel, _listener)

    @asynccontextmanager
    async def listen_to_cluster_update(
        self, listener: Callable[[], None]
    ) -> AsyncIterator[None]:
        async with AutoReSubscriber(
            self._listen_to_cluster_update_once, listener, self._heartbeat_interval_sec
        ):
            yield


class ClusterUpdater:
    def __init__(
        self,
        notifier: ClusterUpdateNotifier,
        cluster_registry: "ClusterConfigRegistry",
        config: Config,
        config_client: ConfigClient,
    ):
        self._loop = asyncio.get_event_loop()
        self._notifier = notifier
        self._cluster_registry = cluster_registry
        self._config = config
        self._config_client = config_client

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None

    async def start(self) -> None:
        logger.info("Starting Cluster Updater")
        await self._init_task()

    async def __aenter__(self) -> "ClusterUpdater":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def _init_task(self) -> None:
        assert not self._is_active
        assert not self._task

        self._is_active = self._loop.create_future()
        self._task = asyncio.ensure_future(self._run())
        # forcing execution of the newly created task
        await asyncio.sleep(0)

    async def stop(self) -> None:
        logger.info("Stopping Cluster Updater")
        assert self._is_active is not None
        self._is_active.set_result(None)

        assert self._task
        await self._task

        self._task = None
        self._is_active = None

    async def _run(self) -> None:
        assert self._is_active is not None

        def _listener() -> None:
            self._loop.create_task(self._do_update())

        async with self._notifier.listen_to_cluster_update(_listener):
            await self._is_active

    async def _do_update(self) -> None:
        cluster_configs = await self._config_client.get_clusters()
        cluster_registry = self._cluster_registry
        [
            await cluster_registry.replace(cluster_config)
            for cluster_config in cluster_configs
        ]
        await cluster_registry.cleanup(cluster_configs)


class SingleClusterUpdater:
    def __init__(
        self,
        cluster_holder: "ClusterHolder",
        config_client: ConfigClient,
        cluster_name: str,
    ):
        self._loop = asyncio.get_event_loop()
        self._cluster_holder = cluster_holder
        self._config_client = config_client
        self._cluster_name = cluster_name

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None

    async def do_update(self) -> None:
        cluster_config = await self._config_client.get_cluster(self._cluster_name)
        if cluster_config:
            await self._cluster_holder.update(cluster_config)
        else:
            logger.warning(
                f"Was unable to fetch config for cluster {self._cluster_name}"
            )
            await self._cluster_holder.clean()


class ClusterHolder:
    def __init__(self, *, factory: ClusterFactory) -> None:
        self._factory = factory
        self._cluster: Optional[Cluster] = None
        self._lock = RWLock()

    async def __aenter__(self) -> "ClusterHolder":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.clean()

    async def update(
        self,
        config: ClusterConfig,
    ) -> None:
        async with self._lock.writer:
            if self._cluster:
                if self._cluster.config == config:
                    return
                await self._close_cluster(self._cluster)
            self._cluster = self._factory(config)
            await self._init_cluster(self._cluster)

    async def _init_cluster(self, cluster: Cluster) -> None:
        logger.info(f"Initializing cluster '{cluster.name}'")
        try:
            await cluster.init()
        except Exception:
            logger.info(f"Failed to initialize cluster '{cluster.name}'")
            raise
        logger.info(f"Initialized cluster '{cluster.name}'")

    async def _close_cluster(self, cluster: Cluster) -> None:
        logger.info(f"Closing cluster '{cluster.name}'")
        try:
            await cluster.close()
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception:
            logger.exception(f"Failed to close cluster '{cluster.name}'")
        logger.info(f"Closed cluster '{cluster.name}'")

    @asynccontextmanager
    async def get(self) -> AsyncIterator[Cluster]:
        async with self._lock.reader:
            if self._cluster is None:
                raise ClusterNotFound("Cluster is not present")
            else:
                yield self._cluster

    async def clean(self) -> None:
        async with self._lock.writer:
            if self._cluster:
                await self._close_cluster(self._cluster)
                self._cluster = None


class ClusterConfigRegistry:
    def __init__(
        self,
    ) -> None:
        self._records: Dict[str, ClusterConfig] = {}

    @property
    def cluster_names(self) -> List[str]:
        return list(self._records)

    def get(self, name: str) -> ClusterConfig:
        try:
            return self._records[name]
        except KeyError:
            raise ClusterNotFound.create(name)

    async def replace(self, config: ClusterConfig) -> None:
        self._records[config.name] = config

    def remove(self, name: str) -> ClusterConfig:
        record = self._records.pop(name, None)
        if not record:
            raise ClusterNotFound.create(name)
        return record

    async def cleanup(self, keep_clusters: Sequence[ClusterConfig]) -> None:
        all_cluster_names = set(self._records.keys())
        keep_clusters_with_names = {
            cluster_config.name for cluster_config in keep_clusters
        }
        for cluster_for_removal in all_cluster_names - keep_clusters_with_names:
            try:
                self.remove(cluster_for_removal)
            except ClusterNotFound:
                pass
