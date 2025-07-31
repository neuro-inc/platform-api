import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Self

from aiorwlock import RWLock
from apolo_events_client import (
    AbstractEventsClient,
    EventType,
    FilterItem,
    RecvEvent,
    StreamType,
)

from .cluster_config import ClusterConfig
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


class ClusterUpdater:
    _CONFIG_STREAM = StreamType("platform-config")
    _CLUSTER_ADD_EVENT = EventType("cluster-add")
    _CLUSTER_UPDATE_EVENT = EventType("cluster-update")
    _CLUSTER_REMOVE_EVENT = EventType("cluster-remove")

    def __init__(
        self,
        events_client: AbstractEventsClient,
        cluster_registry: "ClusterConfigRegistry",
        config_client: ConfigClient,
    ):
        self._events_client = events_client
        self._cluster_registry = cluster_registry
        self._config_client = config_client

        self._is_active: asyncio.Future[None] | None = None
        self._task: asyncio.Future[None] | None = None

    async def __aenter__(self) -> Self:
        logger.info("Subscribe for %r", self._CONFIG_STREAM)
        await self._events_client.subscribe_group(
            self._CONFIG_STREAM,
            self._on_event,
            filters=[
                FilterItem(
                    event_types=frozenset(
                        [
                            self._CLUSTER_ADD_EVENT,
                            self._CLUSTER_UPDATE_EVENT,
                            self._CLUSTER_REMOVE_EVENT,
                        ]
                    )
                )
            ],
        )
        logger.info("Subscribed")
        return self

    async def __aexit__(self, exc_typ: object, exc_val: object, exc_tb: object) -> None:
        pass

    async def _on_event(self, ev: RecvEvent) -> None:
        assert ev.cluster, "event cluster is required"

        if (
            ev.event_type == self._CLUSTER_UPDATE_EVENT
            or ev.event_type == self._CLUSTER_ADD_EVENT
        ):
            cluster_config = await self._config_client.get_cluster(ev.cluster)
            if cluster_config:
                await self._cluster_registry.replace(cluster_config)
            else:
                logger.warning("Cluster %r not found", ev.cluster)
        if ev.event_type == self._CLUSTER_REMOVE_EVENT:
            self._cluster_registry.remove(ev.cluster)


class SingleClusterUpdater:
    _CONFIG_STREAM = StreamType("platform-config")
    _CLUSTER_UPDATE_EVENT = EventType("cluster-update")

    def __init__(
        self,
        events_client: AbstractEventsClient,
        config_client: ConfigClient,
        cluster_holder: "ClusterHolder",
        cluster_name: str,
    ):
        self._events_client = events_client
        self._config_client = config_client
        self._cluster_holder = cluster_holder
        self._cluster_name = cluster_name

        self.disable_updates_for_test = False

    async def __aenter__(self) -> Self:
        logger.info("Subscribe for %r", self._CONFIG_STREAM)
        await self._events_client.subscribe_group(
            self._CONFIG_STREAM,
            self._on_event,
            filters=[
                FilterItem(
                    event_types=frozenset([self._CLUSTER_UPDATE_EVENT]),
                    clusters=frozenset([self._cluster_name]),
                )
            ],
        )
        logger.info("Subscribed")
        return self

    async def __aexit__(self, exc_typ: object, exc_val: object, exc_tb: object) -> None:
        pass

    async def _on_event(self, _: RecvEvent) -> None:
        await self.do_update()

    async def do_update(self) -> None:
        if self.disable_updates_for_test:
            return

        cluster_config = await self._config_client.get_cluster(self._cluster_name)
        if cluster_config:
            await self._cluster_holder.update(cluster_config)
        else:
            logger.warning(
                "Was unable to fetch config for cluster %s", self._cluster_name
            )
            await self._cluster_holder.clean()


class ClusterHolder:
    def __init__(self, *, factory: ClusterFactory) -> None:
        self._factory = factory
        self._cluster: Cluster | None = None
        self._lock = RWLock()

    async def __aenter__(self) -> "ClusterHolder":
        return self

    async def __aexit__(self, *args: object) -> None:
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
        logger.info("Initializing cluster '%s'", cluster.name)
        try:
            await cluster.init()
        except Exception:
            logger.info("Failed to initialize cluster '%s'", cluster.name)
            raise
        logger.info("Initialized cluster '%s'", cluster.name)

    async def _close_cluster(self, cluster: Cluster) -> None:
        logger.info("Closing cluster '%s'", cluster.name)
        try:
            await cluster.close()
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception:
            logger.exception("Failed to close cluster '%s'", cluster.name)
        logger.info("Closed cluster '%s'", cluster.name)

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
        self._records: dict[str, ClusterConfig] = {}

    @property
    def cluster_names(self) -> list[str]:
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
