import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict

from aiorwlock import RWLock
from async_generator import asynccontextmanager

from .orchestrator import Orchestrator


logger = logging.getLogger(__name__)


class ClusterException(Exception):
    pass


class ClusterNotFound(ClusterException):
    @classmethod
    def create(cls, name: str) -> "ClusterNotFound":
        return cls(f"Cluster '{name}' not found")


@dataclass(frozen=True)
class ClusterConfig:
    name: str


class Cluster(ABC):
    @abstractmethod
    async def init(self) -> None:  # pragma: no cover
        pass

    @abstractmethod
    async def close(self) -> None:  # pragma: no cover
        pass

    @property
    @abstractmethod
    def name(self) -> str:  # pragma: no cover
        pass

    @property
    @abstractmethod
    def orchestrator(self) -> Orchestrator:  # pragma: no cover
        pass


ClusterFactory = Callable[[ClusterConfig], Cluster]


@dataclass
class ClusterRegistryRecord:
    cluster: Cluster
    lock: RWLock = field(default_factory=RWLock)
    is_cluster_closed: bool = False

    @property
    def name(self) -> str:
        return self.cluster.name


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

        async with record.lock.writer_lock:
            record.is_cluster_closed = True

            logger.info(f"Closing cluster '{name}'")
            try:
                await record.cluster.close()
            except Exception:
                logger.exception(f"Failed to close cluster '{name}'")
            logger.info(f"Closed cluster '{name}'")

    @asynccontextmanager
    async def get(self, name: str) -> AsyncIterator[Cluster]:
        record = self._get(name)

        # by switching an execution context here, we are giving both readers
        # and writers a chance to acquire the lock.
        # if a writer wins, the readers block until the lock is released, but
        # once it is released, the underlying cluster is considered to be
        # closed, therefore we have to check the state explicitly.
        async with record.lock.reader_lock:
            if record.is_cluster_closed:  # pragma: no cover
                raise ClusterNotFound.create(name)
            yield record.cluster
