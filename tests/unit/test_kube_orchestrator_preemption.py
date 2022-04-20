from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from typing import Any, Callable
from unittest import mock

import pytest

from platform_api.orchestrator.kube_client import (
    KubeClient,
    Node,
    NodeResources,
    PodDescriptor,
    PodWatchEvent,
    Resources,
)
from platform_api.orchestrator.kube_orchestrator_preemption import (
    IdlePodsHandler,
    NodeResourcesHandler,
)

RawPodFactory = Callable[..., dict[str, Any]]


@pytest.fixture
async def pod_factory() -> RawPodFactory:
    def _create(
        name: str | None = None,
        node_name: str = "minikube",
        cpu: float = 0.1,
        memory: int = 128,
        gpu: int = 1,
        phase: str = "Running",
        is_terminating: bool = False,
        is_idle: bool = False,
    ) -> dict[str, Any]:
        labels = {}
        if is_idle:
            labels["platform.neuromation.io/idle"] = "true"
        pod = PodDescriptor(
            name=name or f"pod-{uuid.uuid4()}",
            labels=labels,
            image="gcr.io/google_containers/pause:3.1",
            resources=Resources(cpu=cpu, memory=memory, gpu=gpu),
        )
        raw_pod = pod.to_primitive()
        raw_pod["metadata"]["creationTimestamp"] = datetime.now().isoformat()
        raw_pod["status"] = {"phase": phase}
        if is_terminating:
            raw_pod["metadata"]["deletionTimestamp"] = datetime.now().isoformat()
        if phase != "Pending":
            raw_pod["spec"]["nodeName"] = node_name
        return raw_pod

    return _create


class TestNodeResourcesHandler:
    @pytest.fixture
    def kube_client(self) -> KubeClient:
        kube_client = mock.AsyncMock(spec=KubeClient)
        kube_client.get_node.side_effect = [
            Node(
                name="minikube1",
                allocatable_resources=NodeResources(cpu=1, memory=1024, gpu=1),
            ),
            Node(
                name="minikube2",
                allocatable_resources=NodeResources(cpu=2, memory=4096, gpu=2),
            ),
        ]
        return kube_client

    @pytest.fixture
    def handler(self, kube_client: KubeClient) -> NodeResourcesHandler:
        return NodeResourcesHandler(kube_client)

    async def test_init_running(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [
            pod_factory(name="job", node_name="minikube1", gpu=0),
            pod_factory(node_name="minikube1"),
            pod_factory(node_name="minikube2"),
        ]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 2

        assert handler.is_pod_running("job") is True

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(cpu=0.8, memory=768, gpu=0)

        resources = handler.get_node_free_resources("minikube2")
        assert resources == NodeResources(cpu=1.9, memory=3968, gpu=1)

    async def test_init_pending(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory(name="job", phase="Pending")]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 0

        assert handler.is_pod_running("job") is False

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)

    async def test_init_succeeded(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory(name="job", phase="Succeeded")]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 0

        assert handler.is_pod_running("job") is False

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)

    async def test_handle_added_running(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(name="job", node_name="minikube1"))
        )

        assert len(handler.get_nodes()) == 1

        assert handler.is_pod_running("job") is True

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=0)

    async def test_handle_added_pending(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(name="job", phase="Pending"))
        )

        assert len(handler.get_nodes()) == 0

        assert handler.is_pod_running("job") is False

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)

    async def test_handle_modified_succeeded(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(name="job", node_name="minikube1"))
        )
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(node_name="minikube1", gpu=0))
        )
        await handler.handle(
            PodWatchEvent.create_modified(
                pod_factory(name="job", node_name="minikube1", phase="Succeeded")
            )
        )

        assert len(handler.get_nodes()) == 1

        assert handler.is_pod_running("job") is False

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=1)

    async def test_handle_deleted(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(name="job", node_name="minikube1"))
        )
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(node_name="minikube1", gpu=0))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(pod_factory(name="job", node_name="minikube1"))
        )

        assert len(handler.get_nodes()) == 1

        assert handler.is_pod_running("job") is False

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=1)

    async def test_handle_deleted_last(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(pod_factory(name="job", node_name="minikube1"))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(pod_factory(name="job", node_name="minikube1"))
        )

        assert len(handler.get_nodes()) == 0

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)

    async def test_handle_deleted_not_existing(
        self, handler: NodeResourcesHandler, pod_factory: RawPodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_deleted(pod_factory(node_name="minikube1"))
        )

        assert len(handler.get_nodes()) == 0

        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)

    async def test_get_node_free_resources_unknown_node(
        self, handler: NodeResourcesHandler
    ) -> None:
        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources(0.0, 0)


class TestIdlePodsHandler:
    @pytest.fixture
    def handler(self) -> IdlePodsHandler:
        return IdlePodsHandler()

    async def test_init_running(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory(is_idle=True)]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 1

    async def test_init_non_idle(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory()]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 0

    async def test_init_pending(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory(is_idle=True, phase="Pending")]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 0

    async def test_init_succeeded(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        pods = [pod_factory(is_idle=True, phase="Succeeded")]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 0

    async def test_handle_added_running(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(pod_factory(is_idle=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")

        assert len(pods) == 1

    async def test_handle_added_pending(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(pod_factory(is_idle=True, phase="Pending"))
        await handler.handle(event)

        pods = handler.get_pods("minikube")

        assert len(pods) == 0

    async def test_handle_added_non_idle(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(pod_factory())
        await handler.handle(event)

        pods = handler.get_pods("minikube")

        assert len(pods) == 0

    async def test_handle_modified_succeeded(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

        event = PodWatchEvent.create_modified(
            pod_factory(name="idle-job", is_idle=True, phase="Succeeded"),
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_deleted(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

        event = PodWatchEvent.create_deleted(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_deleted_not_existing(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_deleted(
            pod_factory(is_idle=True),
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_get_pods_unknown_node(self, handler: IdlePodsHandler) -> None:
        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 0

    async def test_wait_for_pod_terminating(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        async def _handle_modified() -> None:
            await asyncio.sleep(0.1)
            event = PodWatchEvent.create_modified(
                pod_factory(name="idle-job", is_idle=True, is_terminating=True),
            )
            await handler.handle(event)

        asyncio.create_task(_handle_modified())

        await handler.wait_for_pod_terminating("idle-job", 1)

    async def test_wait_for_pod_terminating_deleted(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        async def _handle_deleted() -> None:
            await asyncio.sleep(0.1)
            event = PodWatchEvent.create_deleted(
                pod_factory(name="idle-job", is_idle=True, is_terminating=True),
            )
            await handler.handle(event)

        asyncio.create_task(_handle_deleted())

        await handler.wait_for_pod_terminating("idle-job", 1)

    async def test_wait_for_pod_terminating_already(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        event = PodWatchEvent.create_modified(
            pod_factory(name="idle-job", is_idle=True, is_terminating=True),
        )
        await handler.handle(event)

        await handler.wait_for_pod_terminating("idle-job", 1)

    async def test_wait_for_pod_terminating_fail(
        self, handler: IdlePodsHandler, pod_factory: RawPodFactory
    ) -> None:
        event = PodWatchEvent.create_added(
            pod_factory(name="idle-job", is_idle=True),
        )
        await handler.handle(event)

        with pytest.raises(asyncio.TimeoutError):
            await handler.wait_for_pod_terminating("idle-job", 1)
