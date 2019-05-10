import logging

from aioelasticsearch import Elasticsearch
from async_exit_stack import AsyncExitStack

from .cluster import Cluster
from .cluster_config import ClusterConfig
from .elasticsearch import create_elasticsearch_client
from .orchestrator import KubeConfig, KubeOrchestrator, Orchestrator


logger = logging.getLogger(__name__)


class KubeCluster(Cluster):
    _es_client: Elasticsearch
    _orchestrator: Orchestrator

    def __init__(self, config: ClusterConfig) -> None:
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
        await self._init_es_client()
        await self._init_orchestrator()

    async def _init_es_client(self) -> None:
        logger.info(f"Cluster '{self.name}': initializing Elasticsearch client")
        es_client = await self._exit_stack.enter_async_context(
            create_elasticsearch_client(self._config.logging.elasticsearch)
        )
        self._es_client = es_client

    async def _init_orchestrator(self) -> None:
        assert isinstance(self._config.orchestrator, KubeConfig)
        logger.info(f"Cluster '{self.name}': initializing Orchestrator")
        orchestrator = KubeOrchestrator(
            config=self._config.orchestrator, es_client=self._es_client
        )
        await self._exit_stack.enter_async_context(orchestrator)
        self._orchestrator = orchestrator

    async def close(self) -> None:
        await self._exit_stack.__aexit__(None, None, None)
