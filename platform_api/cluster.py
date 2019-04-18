import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import AsyncIterator, Callable, Dict, Optional

from aiorwlock import RWLock
from async_generator import asynccontextmanager

from .orchestrator import Orchestrator


logger = logging.getLogger(__name__)


class ClusterException(Exception):
    pass


class ClusterNotFound(ClusterException):
    pass


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


@dataclass(frozen=True)
class ClusterRegistryRecord:
    cluster: Cluster
    lock: RWLock = field(default_factory=RWLock)

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

    def _remove(self, name: str) -> Optional[ClusterRegistryRecord]:
        return self._records.pop(name, None)

    def _get(self, name: str) -> ClusterRegistryRecord:
        record = self._records.get(name)
        if not record:
            raise ClusterNotFound(f"Cluster '{name}' not found")
        return record

    async def add(self, config: ClusterConfig) -> None:
        await self.remove(config.name)

        logger.info(f"Initializing cluster '{config.name}'")
        cluster = self._factory(config)
        await cluster.init()
        logger.info(f"Initialized cluster '{config.name}'")

        record = ClusterRegistryRecord(cluster=cluster)
        self._add(record)
        logger.info(f"Registered cluster '{config.name}'")

    async def remove(self, name: str) -> None:
        record = self._remove(name)
        if not record:
            return
        logger.info(f"Unregistered cluster '{name}'")

        async with record.lock.writer_lock:
            logger.info(f"Closing cluster '{name}'")
            try:
                await record.cluster.close()
            except Exception:
                logger.exception(f"Failed to close cluster '{name}'")
            logger.info(f"Closed cluster '{name}'")

    @asynccontextmanager
    async def get(self, name: str) -> AsyncIterator[Cluster]:
        record = self._get(name)

        async with record.lock.reader_lock:
            yield record.cluster
