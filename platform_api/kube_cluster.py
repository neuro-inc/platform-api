import logging

from async_exit_stack import AsyncExitStack

from .cluster import Cluster
from .cluster_config import ClusterConfig
from .orchestrator.kube_orchestrator import KubeOrchestrator, Orchestrator


logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    _orchestrator: Orchestrator

    def __init__(self, config: ClusterConfig) -> None:
        super().__init__(failure_threshold=config.cluster_failure_threshold)
        self._config = config

        self._exit_stack = AsyncExitStack()

    @property
    def config(self) -> ClusterConfig:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator

    async def init(self) -> None:
        await self._exit_stack.__aenter__()
        await self._init_orchestrator()

    async def _init_orchestrator(self) -> None:
        logger.info(f"Cluster '{self.name}': initializing Orchestrator")
        orchestrator = KubeOrchestrator(
            storage_config=self._config.storage,
            registry_config=self._config.registry,
            kube_config=self._config.orchestrator,
        )
        await self._exit_stack.enter_async_context(orchestrator)
        self._orchestrator = orchestrator

    async def close(self) -> None:
        await self._exit_stack.__aexit__(None, None, None)
