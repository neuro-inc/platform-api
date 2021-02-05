from typing import Any

from platform_api.cluster import Cluster, ClusterConfig
from platform_api.orchestrator.base import Orchestrator


class _TestCluster(Cluster):
    def __init__(self, config: ClusterConfig) -> None:
        self._config = config

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def config(self) -> ClusterConfig:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        pass


def create_cluster_config(name: str, **kwargs: Any) -> ClusterConfig:
    return ClusterConfig(
        name=name,
        storage=None,  # type: ignore
        registry=None,  # type: ignore
        orchestrator=None,  # type: ignore
        ingress=None,  # type: ignore
        **kwargs,
    )


class TestClusterRegistry:
    pass
