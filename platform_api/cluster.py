import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Dict, Sequence

from aiorwlock import RWLock
from async_generator import asynccontextmanager

from .circuit_breaker import CircuitBreaker
from .cluster_config import ClusterConfig
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


class ClusterRegistry:
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

    async def __aenter__(self) -> "ClusterRegistry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        for name in list(self._records):
            await self.remove(name)

    async def replace(self, config: ClusterConfig) -> None:
        record = self._records.get(config.name)
        if record:
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

            async with record.lock.writer:
                await self._init_cluster(record)

    def _add_record(self, config: ClusterConfig) -> ClusterRegistryRecord:
        new_cluster = self._factory(config)
        record = ClusterRegistryRecord(new_cluster)
        self._records[config.name] = record
        logger.info(f"Registered new cluster '{config.name}'")
        return record

    async def _init_cluster(self, record: ClusterRegistryRecord) -> None:
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
        keep_clusters_with_names = set(
            cluster_config.name for cluster_config in keep_clusters
        )
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
