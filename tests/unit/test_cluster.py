from collections.abc import Callable
from unittest import mock

import neuro_config_client
import pytest

from platform_api.cluster import (
    Cluster,
    ClusterHolder,
    ClusterNotAvailable,
    ClusterNotFound,
)
from platform_api.orchestrator.base import Orchestrator


class _FlakyCluster(Cluster):
    def __init__(self, config: neuro_config_client.Cluster, failures: int) -> None:
        self._config = config
        self._failures = failures
        self.closed = 0

    @property
    def config(self) -> neuro_config_client.Cluster:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        return mock.Mock(spec=Orchestrator)

    async def init(self) -> None:
        if self._failures:
            self._failures -= 1
            raise RuntimeError("kube api is not ready")

    async def close(self) -> None:
        self.closed += 1


class TestClusterHolder:
    @pytest.fixture
    def created(self) -> list[_FlakyCluster]:
        return []

    @pytest.fixture
    def factory(
        self, created: list[_FlakyCluster]
    ) -> Callable[[neuro_config_client.Cluster], Cluster]:
        def _factory(config: neuro_config_client.Cluster) -> Cluster:
            cluster = _FlakyCluster(config, failures=max(0, 1 - len(created)))
            created.append(cluster)
            return cluster

        return _factory

    async def test_get__without_config(
        self, factory: Callable[[neuro_config_client.Cluster], Cluster]
    ) -> None:
        async with ClusterHolder(factory=factory) as holder:
            with pytest.raises(ClusterNotFound):
                async with holder.get():
                    pass

    async def test_update__failed_init_leaves_holder_without_cluster(
        self,
        factory: Callable[[neuro_config_client.Cluster], Cluster],
        created: list[_FlakyCluster],
        cluster_config: neuro_config_client.Cluster,
    ) -> None:
        async with ClusterHolder(factory=factory) as holder:
            with pytest.raises(RuntimeError):
                await holder.update(cluster_config)

            assert created[0].closed == 1
            with pytest.raises(ClusterNotAvailable):
                async with holder.get():
                    pass

    async def test_update__retries_init_after_failure(
        self,
        factory: Callable[[neuro_config_client.Cluster], Cluster],
        created: list[_FlakyCluster],
        cluster_config: neuro_config_client.Cluster,
    ) -> None:
        async with ClusterHolder(factory=factory) as holder:
            with pytest.raises(RuntimeError):
                await holder.update(cluster_config)

            await holder.update(cluster_config)

            async with holder.get() as cluster:
                assert cluster is created[1]
            assert created[1].closed == 0

    async def test_clean__forgets_failed_config(
        self,
        factory: Callable[[neuro_config_client.Cluster], Cluster],
        cluster_config: neuro_config_client.Cluster,
    ) -> None:
        async with ClusterHolder(factory=factory) as holder:
            with pytest.raises(RuntimeError):
                await holder.update(cluster_config)

            await holder.clean()

            with pytest.raises(ClusterNotFound):
                async with holder.get():
                    pass
