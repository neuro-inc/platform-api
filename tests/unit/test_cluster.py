import asyncio

import pytest
from async_timeout import timeout

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterNotFound,
    ClusterRegistry,
)
from platform_api.orchestrator import Orchestrator


class _TestCluster(Cluster):
    def __init__(self, config: ClusterConfig) -> None:
        self._config = config

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def orchestrator(self) -> Orchestrator:
        pass


class TestClusterRegistry:
    @pytest.mark.asyncio
    async def test_get_not_found(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)

        with pytest.raises(ClusterNotFound, match="Cluster 'test' not found"):
            async with registry.get("test"):
                pass

    @pytest.mark.asyncio
    async def test_add(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        config = ClusterConfig(name="test")

        await registry.add(config)

        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name

    @pytest.mark.asyncio
    async def test_add_existing(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = ClusterConfig(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.add(config)

        old_cluster = None
        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name
            old_cluster = cluster

        await registry.add(config)

        new_cluster = None
        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name
            new_cluster = cluster

        assert old_cluster is not new_cluster

    @pytest.mark.asyncio
    async def test_remove_missing(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            await registry.remove(name)

    @pytest.mark.asyncio
    async def test_remove(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = ClusterConfig(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.add(config)

        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name

        await registry.remove(name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

    @pytest.mark.asyncio
    async def test_remove_locked(self, event_loop) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = ClusterConfig(name=name)

        await registry.add(config)

        async with registry.get(name):
            with pytest.raises(asyncio.TimeoutError):
                async with timeout(0.1):
                    await event_loop.create_task(registry.remove(name))

    @pytest.mark.asyncio
    async def test_remove_another_locked(self, event_loop) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        config = ClusterConfig(name="test1")
        anotherconfig = ClusterConfig(name="test2")

        await registry.add(config)
        await registry.add(anotherconfig)

        async with registry.get(config.name):
            async with registry.get(anotherconfig.name):
                pass

            await registry.remove(anotherconfig.name)

            with pytest.raises(
                ClusterNotFound, match=f"Cluster '{anotherconfig.name}' not found"
            ):
                async with registry.get(anotherconfig.name):
                    pass

    @pytest.mark.asyncio
    async def test_remove_close_failure(self) -> None:
        class _NotClosingCluster(_TestCluster):
            async def close(self) -> None:
                raise RuntimeError("Unexpected")

        registry = ClusterRegistry(factory=_NotClosingCluster)
        name = "test"
        config = ClusterConfig(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.add(config)

        with pytest.not_raises(Exception):
            await registry.remove(name)
