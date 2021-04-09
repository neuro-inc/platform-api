import logging
from contextlib import AsyncExitStack

from .cluster import Cluster
from .cluster_config import ClusterConfig, RegistryConfig, StorageConfig
from .orchestrator.kube_config import KubeConfig
from .orchestrator.kube_orchestrator import KubeOrchestrator, Orchestrator


logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    _orchestrator: Orchestrator

    def __init__(
        self,
        registry_config: RegistryConfig,
        storage_config: StorageConfig,
        cluster_config: ClusterConfig,
        kube_config: KubeConfig,
    ) -> None:
        self._registry_config = registry_config
        self._storage_config = storage_config
        self._cluster_config = cluster_config
        self._kube_config = kube_config

        self._exit_stack = AsyncExitStack()

    @property
    def config(self) -> ClusterConfig:
        return self._cluster_config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator

    async def init(self) -> None:
        await self._exit_stack.__aenter__()
        await self._init_orchestrator()

    async def _init_orchestrator(self) -> None:
        logger.info(f"Cluster '{self.name}': initializing Orchestrator")
        orchestrator = KubeOrchestrator(
            storage_config=self._storage_config,
            registry_config=self._registry_config,
            orchestrator_config=self._cluster_config.orchestrator,
            kube_config=self._kube_config,
        )
        await self._exit_stack.enter_async_context(orchestrator)
        self._orchestrator = orchestrator

    async def close(self) -> None:
        await self._exit_stack.__aexit__(None, None, None)
