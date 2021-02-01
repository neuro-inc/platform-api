import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Sequence, Union

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.sql as sasql
from aiorwlock import RWLock
from async_generator import asynccontextmanager
from asyncpg.pool import Pool

from .cluster_config import ClusterConfig
from .config import Config, PollerConfig
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
    config: Union[Config, PollerConfig], config_client: ConfigClient
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
        cluster_configs = await get_cluster_configs(self._config, self._config_client)
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
        config: PollerConfig,
        config_client: ConfigClient,
        cluster_name: str,
    ):
        self._loop = asyncio.get_event_loop()
        self._cluster_holder = cluster_holder
        self._config = config
        self._config_client = config_client
        self._cluster_name = cluster_name

        self._is_active: Optional[asyncio.Future[None]] = None
        self._task: Optional[asyncio.Future[None]] = None

    async def do_update(self) -> None:
        cluster_configs = await get_cluster_configs(self._config, self._config_client)
        try:
            cluster_config = next(
                cluster_config
                for cluster_config in cluster_configs
                if cluster_config.name == self._cluster_name
            )
        except StopIteration:
            logger.warning(
                f"Was unable to fetch config for cluster {self._cluster_name}"
            )
            await self._cluster_holder.clean()
        else:
            await self._cluster_holder.update(cluster_config)


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
                logger.info(f"Closing cluster '{self._cluster.name}'")
                await self._close_cluster(self._cluster)
                self._cluster = None
            else:
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
