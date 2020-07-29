import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from _pytest.logging import LogCaptureFixture
from async_timeout import timeout

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterNotAvailable,
    ClusterNotFound,
    ClusterRegistry,
    ClusterRegistryRecord,
)
from platform_api.cluster_config import CircuitBreakerConfig, StorageConfig
from platform_api.orchestrator.base import Orchestrator
from tests.conftest import not_raises


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
    @pytest.mark.asyncio
    async def test_get_not_found(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)

        with pytest.raises(ClusterNotFound, match="Cluster 'test' not found"):
            async with registry.get("test"):
                pass

    @pytest.mark.asyncio
    async def test_add(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        config = create_cluster_config(name="test")

        await registry.replace(config)

        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name

    @pytest.mark.asyncio
    async def test_add_raises(self) -> None:
        class _NotAvailableCluster(_TestCluster):
            async def init(self) -> None:
                raise RuntimeError("Unexpected")

        registry = ClusterRegistry(factory=_NotAvailableCluster)
        config = create_cluster_config(name="test")

        with pytest.raises(Exception):
            await registry.replace(config)

        with pytest.raises(ClusterNotFound):
            async with registry.get(config.name):
                pass

    @pytest.mark.asyncio
    async def test_get(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        config = create_cluster_config(
            name="test", circuit_breaker=CircuitBreakerConfig(open_threshold=1)
        )

        await registry.replace(config)

        async with registry.get(config.name):
            raise RuntimeError("test")

        with pytest.raises(ClusterNotAvailable):
            async with registry.get(config.name):
                pass

        async with registry.get(config.name, skip_circuit_breaker=True) as cluster:
            assert cluster.name == config.name

    @pytest.mark.asyncio
    async def test_replace_existing_and_remove_at_once(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = create_cluster_config(name=name)

        await registry.replace(config)

        old_cluster = None
        async with registry.get(name) as cluster:
            old_cluster = cluster

        config = create_cluster_config(name=name)
        record_assigned = asyncio.Event()
        record_ready = asyncio.Event()

        async def _replace() -> None:
            await registry.replace(
                config, record_assigned=record_assigned, record_ready=record_ready
            )

        async def _remove() -> None:
            await record_assigned.wait()
            await registry.remove(config.name)

            with pytest.raises(ClusterNotFound):
                async with registry.get(config.name):
                    pass

            record_ready.set()

        await asyncio.wait(
            [asyncio.create_task(_replace()), asyncio.create_task(_remove())]
        )

        new_cluster = None
        async with registry.get(name) as cluster:
            new_cluster = cluster

        assert new_cluster.name == config.name
        assert old_cluster is not new_cluster

    @pytest.mark.asyncio
    async def test_replace_new_and_remove_at_once(self) -> None:
        class _NotInitingCluster(_TestCluster):
            async def init(self) -> None:
                raise RuntimeError("Unexpected")

        registry = ClusterRegistry(factory=_NotInitingCluster)
        name = "test"
        config = create_cluster_config(name=name)
        record_assigned = asyncio.Event()
        record_ready = asyncio.Event()

        async def _replace() -> None:
            await registry.replace(
                config, record_assigned=record_assigned, record_ready=record_ready
            )

        async def _remove() -> None:
            await record_assigned.wait()
            await registry.remove(config.name)

            with pytest.raises(ClusterNotFound):
                async with registry.get(config.name):
                    pass

            record_ready.set()

        await asyncio.wait(
            [asyncio.create_task(_replace()), asyncio.create_task(_remove())]
        )

        with pytest.raises(ClusterNotFound):
            async with registry.get(config.name):
                pass

    @pytest.mark.asyncio
    async def test_replace_if_existing_changed(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = create_cluster_config(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.replace(config)

        old_cluster = None
        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name
            old_cluster = cluster

        new_config = replace(config, storage=StorageConfig(host_mount_path=Path("/")))
        await registry.replace(new_config)

        new_cluster = None
        async with registry.get(new_config.name) as cluster:
            assert cluster.name == new_config.name
            new_cluster = cluster

        assert old_cluster is not new_cluster

    @pytest.mark.asyncio
    async def test_replace_if_existing_not_changed(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = create_cluster_config(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.replace(config)

        old_cluster = None
        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name
            old_cluster = cluster

        await registry.replace(config)

        new_cluster = None
        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name
            new_cluster = cluster

        assert old_cluster is new_cluster

    @pytest.mark.asyncio
    async def test_replace_close_failure(self) -> None:
        class _NotClosingCluster(_TestCluster):
            async def close(self) -> None:
                raise RuntimeError("Unexpected")

        registry = ClusterRegistry(factory=_NotClosingCluster)
        name = "test"
        config = create_cluster_config(name=name)

        await registry.replace(config)

        new_config = replace(config, storage=StorageConfig(host_mount_path=Path("/")))

        with not_raises(Exception):
            await registry.replace(new_config)

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
        config = create_cluster_config(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.replace(config)

        async with registry.get(config.name) as cluster:
            assert cluster.name == config.name

        await registry.remove(name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

    @pytest.mark.asyncio
    async def test_remove_locked(self, event_loop: asyncio.AbstractEventLoop) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = create_cluster_config(name=name)

        await registry.replace(config)

        async with registry.get(name):
            with pytest.raises(asyncio.TimeoutError):
                async with timeout(0.1):
                    await event_loop.create_task(registry.remove(name))

    @pytest.mark.asyncio
    async def test_remove_another_locked(
        self, event_loop: asyncio.AbstractEventLoop
    ) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        config = create_cluster_config(name="test1")
        anotherconfig = create_cluster_config(name="test2")

        await registry.replace(config)
        await registry.replace(anotherconfig)

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
        config = create_cluster_config(name=name)

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

        await registry.replace(config)

        with not_raises(Exception):
            await registry.remove(name)

    @pytest.mark.asyncio
    async def test_cleanup(self) -> None:
        registry = ClusterRegistry(factory=_TestCluster)
        name = "test"
        config = create_cluster_config(name=name)

        async with registry:
            await registry.replace(config)

            async with registry.get(name):
                pass

        with pytest.raises(ClusterNotFound, match=f"Cluster '{name}' not found"):
            async with registry.get(name):
                pass

    @pytest.mark.asyncio
    async def test_cleanup_not_found(self) -> None:
        class _NotFoundCluster(_TestCluster):
            async def close(self) -> None:
                raise ClusterNotFound("test")

        registry = ClusterRegistry(factory=_NotFoundCluster)
        name = "test"
        config = create_cluster_config(name=name)

        await registry.replace(config)

        async with registry.get(name):
            pass

        with not_raises(Exception):
            await registry.cleanup([])


class TestClusterRegistryRecord:
    def create_record(self, **kwargs: Any) -> ClusterRegistryRecord:
        return ClusterRegistryRecord(
            _TestCluster(
                create_cluster_config(
                    "test", circuit_breaker=CircuitBreakerConfig(**kwargs)
                )
            )
        )

    @pytest.mark.asyncio
    async def test_idle_closed(self) -> None:
        record = self.create_record()

        async with record.circuit_breaker:
            pass

    @pytest.mark.asyncio
    async def test_threshold_not_exceeded(self, caplog: LogCaptureFixture) -> None:
        record = self.create_record(open_threshold=2)

        async with record.circuit_breaker:
            raise RuntimeError("testerror")

        assert "Unexpected exception in cluster: 'test'. Suppressing" in caplog.text
        assert "RuntimeError: testerror" in caplog.text

        async with record.circuit_breaker:
            pass

    @pytest.mark.asyncio
    async def test_threshold_exceeded(self) -> None:
        record = self.create_record(open_threshold=1)

        async with record.circuit_breaker:
            raise RuntimeError("testerror")

        with pytest.raises(ClusterNotAvailable, match="Cluster 'test' not available"):
            async with record.circuit_breaker:
                pass

    @pytest.mark.asyncio
    async def test_cancelled_not_suppressed(self) -> None:
        record = self.create_record(open_threshold=1)

        with pytest.raises(asyncio.CancelledError):
            async with record.circuit_breaker:
                raise asyncio.CancelledError()

        with pytest.raises(ClusterNotAvailable, match="Cluster 'test' not available"):
            async with record.circuit_breaker:
                pass
