import logging
from collections.abc import Sequence
from contextlib import AsyncExitStack
from typing import Optional

import aiohttp

from .cluster import Cluster
from .cluster_config import ClusterConfig
from .config import RegistryConfig, StorageConfig
from .orchestrator.kube_client import KubeClient, NodeWatcher, PodWatcher
from .orchestrator.kube_config import KubeConfig
from .orchestrator.kube_orchestrator import KubeOrchestrator, Orchestrator

logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    _orchestrator: Orchestrator

    def __init__(
        self,
        registry_config: RegistryConfig,
        storage_configs: Sequence[StorageConfig],
        cluster_config: ClusterConfig,
        kube_config: KubeConfig,
        trace_configs: Optional[list[aiohttp.TraceConfig]] = None,
    ) -> None:
        self._registry_config = registry_config
        self._storage_configs = storage_configs
        self._cluster_config = cluster_config
        self._kube_config = kube_config
        self._trace_configs = trace_configs

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
        kube_client = KubeClient(
            base_url=self._kube_config.endpoint_url,
            cert_authority_data_pem=self._kube_config.cert_authority_data_pem,
            cert_authority_path=self._kube_config.cert_authority_path,
            auth_type=self._kube_config.auth_type,
            auth_cert_path=self._kube_config.auth_cert_path,
            auth_cert_key_path=self._kube_config.auth_cert_key_path,
            token=self._kube_config.token,
            token_path=self._kube_config.token_path,
            namespace=self._kube_config.namespace,
            conn_timeout_s=self._kube_config.client_conn_timeout_s,
            read_timeout_s=self._kube_config.client_read_timeout_s,
            conn_pool_size=self._kube_config.client_conn_pool_size,
            trace_configs=self._trace_configs,
        )
        node_watcher = NodeWatcher(kube_client)
        pod_watcher = PodWatcher(kube_client)
        orchestrator = KubeOrchestrator(
            cluster_name=self.name,
            storage_configs=self._storage_configs,
            registry_config=self._registry_config,
            orchestrator_config=self._cluster_config.orchestrator,
            kube_config=self._kube_config,
            kube_client=kube_client,
        )
        orchestrator.register(node_watcher, pod_watcher)
        await self._exit_stack.enter_async_context(kube_client)
        await self._exit_stack.enter_async_context(node_watcher)
        await self._exit_stack.enter_async_context(pod_watcher)
        self._orchestrator = orchestrator

    async def close(self) -> None:
        await self._exit_stack.__aexit__(None, None, None)
