import logging
from contextlib import AsyncExitStack

from .cluster import Cluster
from .cluster_config import ClusterConfig
from .config import RegistryConfig
from .orchestrator.kube_client import KubeClient, NodeWatcher, PodWatcher
from .orchestrator.kube_config import KubeConfig
from .orchestrator.kube_orchestrator import KubeOrchestrator, Orchestrator

logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    def __init__(
        self,
        kube_client: KubeClient,
        kube_config: KubeConfig,
        registry_config: RegistryConfig,
        cluster_config: ClusterConfig,
    ) -> None:
        self._kube_client = kube_client
        self._kube_config = kube_config
        self._registry_config = registry_config
        self._cluster_config = cluster_config

        self._exit_stack = AsyncExitStack()

    @property
    def config(self) -> ClusterConfig:
        return self._cluster_config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator

    async def init(self) -> None:
        await self._init_orchestrator()

    async def _init_orchestrator(self) -> None:
        logger.info("Cluster '%s': initializing Orchestrator", self.name)
        kube_node_watcher = NodeWatcher(
            self._kube_client, labels=self._get_job_node_labels()
        )
        kube_pod_watcher = PodWatcher(self._kube_client)
        orchestrator = KubeOrchestrator(
            cluster_name=self.name,
            registry_config=self._registry_config,
            orchestrator_config=self._cluster_config.orchestrator,
            kube_config=self._kube_config,
            kube_client=self._kube_client,
        )
        orchestrator.subscribe_to_kube_events(kube_node_watcher, kube_pod_watcher)
        await self._exit_stack.enter_async_context(kube_node_watcher)
        await self._exit_stack.enter_async_context(kube_pod_watcher)
        self._orchestrator = orchestrator

    def _get_job_node_labels(self) -> dict[str, str]:
        labels = {}
        if self._kube_config.node_label_job:
            labels[self._kube_config.node_label_job] = "true"
        return labels

    async def close(self) -> None:
        await self._exit_stack.aclose()
