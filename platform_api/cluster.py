import asyncio
import logging
from abc import ABC, abstractmethod
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
from asyncpg.pool import Pool

from .circuit_breaker import CircuitBreaker
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


ClusterFactory = Callable[[ClusterConfig], Cluster]


class ClusterRegistryRecord:
    def __init__(self, cluster: Cluster) -> None:
        self._lock = RWLock()
        self.cluster = cluster

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    @cluster.setter
    def cluster(self, cluster: Cluster) -> None:
        self._is_cluster_initialized = False
        self._is_cluster_closed = False
        self._cluster = cluster
        self._breaker = CircuitBreaker(
            open_threshold=cluster.config.circuit_breaker.open_threshold,
            open_timeout_s=cluster.config.circuit_breaker.open_timeout_s,
        )

    @property
    def lock(self) -> RWLock:
        return self._lock

    @property
    def is_cluster_initialized(self) -> bool:
        return self._is_cluster_initialized

    @property
    def is_cluster_closed(self) -> bool:
        return self._is_cluster_closed

    def mark_cluster_initialized(self) -> None:
        self._is_cluster_initialized = True

    def mark_cluster_closed(self) -> None:
        self._is_cluster_closed = True

    @property
    def name(self) -> str:
        return self.cluster.name

    @property
    def circuit_breaker(self) -> AsyncContextManager[None]:
        return self._circuit_breaker()

    @asynccontextmanager
    async def _circuit_breaker(self) -> AsyncIterator[None]:
        if not self._breaker.is_closed and not self._breaker.is_half_closed:
            raise ClusterNotAvailable.create(self.name)

        try:
            yield
        except asyncio.CancelledError:
            self._breaker.register_failure()
            raise
        except Exception:
            self._breaker.register_failure()
            logger.exception(
                f"Unexpected exception in cluster: '{self.name}'. Suppressing"
            )
        else:
            self._breaker.register_success()


class ClusterUpdateNotifier:
    def __init__(self, pool: Pool) -> None:
        self._pool = pool
        self._channel = "cluster_update_required"

    # Database helpers

    async def _execute(self, query: sasql.ClauseElement) -> str:
        query_string, params = asyncpgsa.compile_query(query)
        return await self._pool.execute(query_string, *params)

    async def notify_cluster_update(self) -> None:
        logger.info(f"Notifying channel {self._channel!r}")
        query = sa.text(f"NOTIFY {self._channel}")
        await self._execute(query)

    @asynccontextmanager
    async def listen_to_cluster_update(
        self, listener: Callable[[], None]
    ) -> AsyncIterator[None]:
        def _listener(*args: Any) -> None:
            listener()

        async with self._pool.acquire() as conn:
            logger.info(f"Subscribing to channel {self._channel!r}")
            await conn.add_listener(self._channel, _listener)
            try:
                yield
            finally:
                logger.info(f"Unsubscribing from channel {self._channel!r}")
                await conn.remove_listener(self._channel, _listener)


async def get_cluster_configs(
    config: Config, config_client: ConfigClient
) -> Sequence[ClusterConfig]:
    return await config_client.get_clusters(
        jobs_ingress_class=config.jobs.jobs_ingress_class,
        jobs_ingress_oauth_url=config.jobs.jobs_ingress_oauth_url,
        registry_username=config.auth.service_name,
        registry_password=config.auth.service_token,
    )


class ClusterUpdater:
    def __init__(
        self,
        notifier: ClusterUpdateNotifier,
        cluster_registry: "BaseClusterRegistry",
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
        cluster_configs = await get_cluster_configs(self._config, self._config_client)
        cluster_registry = self._cluster_registry
        [
            await cluster_registry.replace(cluster_config)
            for cluster_config in cluster_configs
        ]
        await cluster_registry.cleanup(cluster_configs)


class BaseClusterRegistry(ABC):
    @abstractmethod
    async def replace(self, config: ClusterConfig) -> None:
        pass

    @abstractmethod
    async def cleanup(self, keep_clusters: Sequence[ClusterConfig]) -> None:
        pass


class ClusterRegistry(BaseClusterRegistry):
    def __init__(self, *, factory: ClusterFactory) -> None:
        self._factory = factory
        self._records: Dict[str, ClusterRegistryRecord] = {}

    def _remove(self, name: str) -> ClusterRegistryRecord:
        record = self._records.pop(name, None)
        if not record:
            raise ClusterNotFound.create(name)
        return record

    def _get(self, name: str) -> ClusterRegistryRecord:
        record = self._records.get(name)
        if not record:
            raise ClusterNotFound.create(name)
        return record

    @property
    def cluster_names(self) -> List[str]:
        return list(self._records)

    async def __aenter__(self) -> "ClusterRegistry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        for name in self.cluster_names:
            await self.remove(name)

    async def replace(
        self,
        config: ClusterConfig,
        record_assigned: Optional[asyncio.Event] = None,  # used for testing
        record_ready: Optional[asyncio.Event] = None,  # used for testing
    ) -> None:
        record = self._records.get(config.name)

        if record:
            if record_ready:
                # used only for testing
                if record_assigned:
                    record_assigned.set()
                await record_ready.wait()

            async with record.lock.writer:
                if config.name not in self._records:  # pragma: no cover
                    record = self._add_record(config)

                    async with record.lock.writer:
                        await self._init_cluster(record)
                    return

                old_cluster = record.cluster
                if old_cluster.config == config:
                    logger.info(f"Cluster '{config.name}' didn't change")
                    return

                new_cluster = self._factory(config)
                logger.info(f"Initializing cluster '{config.name}'")
                await new_cluster.init()
                logger.info(f"Initialized cluster '{config.name}'")
                record.cluster = new_cluster
                record.mark_cluster_initialized()
                logger.info(f"Registered cluster '{config.name}'")

                await self._close_cluster(old_cluster)
        else:
            record = self._add_record(config)

            if record_ready:
                # used only for testing
                if record_assigned:
                    record_assigned.set()
                await record_ready.wait()

            async with record.lock.writer:
                await self._init_cluster(record)

    def _add_record(self, config: ClusterConfig) -> ClusterRegistryRecord:
        new_cluster = self._factory(config)
        record = ClusterRegistryRecord(new_cluster)
        self._records[config.name] = record
        logger.info(f"Registered new cluster '{config.name}'")
        return record

    async def _init_cluster(self, record: ClusterRegistryRecord) -> None:
        if record is not self._records.get(record.name):
            return

        logger.info(f"Initializing cluster '{record.name}'")
        try:
            await record.cluster.init()
            record.mark_cluster_initialized()
        except Exception:
            record.mark_cluster_closed()
            self._remove(record.name)
            logger.info(f"Failed to initialize cluster '{record.name}'")
            raise
        logger.info(f"Initialized cluster '{record.name}'")

    async def remove(self, name: str) -> None:
        record = self._remove(name)

        logger.info(f"Unregistered cluster '{name}'")

        async with record.lock.writer:
            record.mark_cluster_closed()

            await self._close_cluster(record.cluster)

    async def _close_cluster(self, cluster: Cluster) -> None:
        logger.info(f"Closing cluster '{cluster.name}'")
        try:
            await cluster.close()
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception:
            logger.exception(f"Failed to close cluster '{cluster.name}'")
        logger.info(f"Closed cluster '{cluster.name}'")

    async def cleanup(self, keep_clusters: Sequence[ClusterConfig]) -> None:
        all_cluster_names = set(self._records.keys())
        keep_clusters_with_names = {
            cluster_config.name for cluster_config in keep_clusters
        }
        for cluster_for_removal in all_cluster_names - keep_clusters_with_names:
            try:
                await self.remove(cluster_for_removal)
            except ClusterNotFound:
                pass

    @asynccontextmanager
    async def get(
        self, name: str, skip_circuit_breaker: bool = False
    ) -> AsyncIterator[Cluster]:
        record = self._get(name)

        # by switching an execution context here, we are giving both readers
        # and writers a chance to acquire the lock.
        # if a writer wins, the readers block until the lock is released, but
        # once it is released, the underlying cluster is considered to be
        # closed, therefore we have to check the state explicitly.
        async with record.lock.reader:
            if (
                record.is_cluster_closed or not record.is_cluster_initialized
            ):  # pragma: no cover
                raise ClusterNotFound.create(name)
            if skip_circuit_breaker:
                yield record.cluster
            else:
                async with record.circuit_breaker:
                    yield record.cluster

    def __len__(self) -> int:
        return len(self._records)


class ClusterConfigRegistry(BaseClusterRegistry):
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
