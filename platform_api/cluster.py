import asyncio
import logging
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, AsyncIterator, Callable, Dict, Optional, Sequence, Type

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


class ClusterCircuitBreaker(CircuitBreaker):
    def __init__(self, *, name: str, **kwargs: Any) -> None:
        self._name = name
        super().__init__(**kwargs)

    async def __aenter__(self) -> "ClusterCircuitBreaker":
        if self.is_closed or self.is_half_closed:
            return self
        raise ClusterNotAvailable.create(self._name)

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        if exc_type is not None:
            self.register_failure()

            if issubclass(exc_type, asyncio.CancelledError):
                return False

            logger.exception(
                f"Unexpected exception in cluster: '{self._name}'. Suppressing"
            )
            return True

        self.register_success()
        return False


class ClusterRegistryRecord:
    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster
        self._lock = RWLock()
        self._is_cluster_closed = False
        self._breaker = ClusterCircuitBreaker(name=cluster.name)

    @property
    def cluster(self) -> Cluster:
        return self._cluster

    @property
    def lock(self) -> RWLock:
        return self._lock

    @property
    def is_cluster_closed(self) -> bool:
        return self._is_cluster_closed

    def mark_cluster_closed(self) -> None:
        self._is_cluster_closed = True

    @property
    def name(self) -> str:
        return self.cluster.name

    @property
    def breaker(self) -> ClusterCircuitBreaker:
        return self._breaker


class ClusterRegistry:
    def __init__(self, *, factory: ClusterFactory) -> None:
        self._factory = factory
        self._records: Dict[str, ClusterRegistryRecord] = {}

    def _add(self, record: ClusterRegistryRecord) -> None:
        assert record.name not in self._records
        self._records[record.name] = record

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

    async def add(self, config: ClusterConfig) -> None:
        try:
            await self.remove(config.name)
        except ClusterNotFound:
            pass

        logger.info(f"Initializing cluster '{config.name}'")
        cluster = self._factory(config)
        await cluster.init()
        logger.info(f"Initialized cluster '{config.name}'")

        record = ClusterRegistryRecord(cluster=cluster)
        self._add(record)
        logger.info(f"Registered cluster '{config.name}'")

    async def remove(self, name: str) -> None:
        record = self._remove(name)

        logger.info(f"Unregistered cluster '{name}'")

        async with record.lock.writer:
            record.mark_cluster_closed()

            logger.info(f"Closing cluster '{name}'")
            try:
                await record.cluster.close()
            except asyncio.CancelledError:  # pragma: no cover
                raise
            except Exception:
                logger.exception(f"Failed to close cluster '{name}'")
            logger.info(f"Closed cluster '{name}'")

    async def cleanup(self, keep_clusters: Sequence[ClusterConfig]) -> None:
        all_cluster_names = set(self._records.keys())
        keep_clusters_with_names = set(
            cluster_config.name for cluster_config in keep_clusters
        )
        for cluster_for_removal in all_cluster_names - keep_clusters_with_names:
            await self.remove(cluster_for_removal)

    @asynccontextmanager
    async def get(self, name: str) -> AsyncIterator[Cluster]:
        record = self._get(name)

        # by switching an execution context here, we are giving both readers
        # and writers a chance to acquire the lock.
        # if a writer wins, the readers block until the lock is released, but
        # once it is released, the underlying cluster is considered to be
        # closed, therefore we have to check the state explicitly.
        async with record.lock.reader:
            if record.is_cluster_closed:  # pragma: no cover
                raise ClusterNotFound.create(name)
            async with record.breaker:
                yield record.cluster

    def __len__(self) -> int:
        return len(self._records)
