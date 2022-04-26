from __future__ import annotations

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

PodFactory = Callable[..., dict[str, Any]]


@pytest.fixture
def create_pod() -> PodFactory:
    def _create(
        name: str | None = None,
        *,
        cpu: float = 0.1,
        memory: int = 128,
        gpu: int = 1,
        labels: dict[str, str] | None = None,
        node_name: str | None = "minikube",
        is_scheduled: bool = False,
        is_running: bool = False,
        is_terminating: bool = False,
        is_terminated: bool = False,
    ) -> dict[str, Any]:
        pod = PodDescriptor(
            name or f"pod-{uuid.uuid4()}",
            labels=labels or {},
            image="gcr.io/google_containers/pause:3.1",
            resources=Resources(cpu=cpu, memory=memory, gpu=gpu),
        )
        raw_pod = pod.to_primitive()
        raw_pod["metadata"]["creationTimestamp"] = datetime.now().isoformat()
        raw_pod["status"] = {"phase": "Pending"}
        scheduled_condition = {
            "lastProbeTime": None,
            "lastTransitionTime": datetime.now().isoformat(),
            "status": "True",
            "type": "PodScheduled",
        }
        if is_scheduled:
            raw_pod["status"] = {
                "phase": "Pending",
                "containerStatuses": [{"state": {"waiting": {}}}],
                "conditions": [scheduled_condition],
            }
            raw_pod["spec"]["nodeName"] = node_name
        if is_running or is_terminating:
            raw_pod["status"] = {
                "phase": "Running",
                "containerStatuses": [{"state": {"running": {}}}],
                "conditions": [scheduled_condition],
            }
            raw_pod["spec"]["nodeName"] = node_name
        if is_terminating:
            raw_pod["metadata"]["deletionTimestamp"] = datetime.now().isoformat()
        if is_terminated:
            raw_pod["status"] = {
                "phase": "Succeeded",
                "containerStatuses": [{"state": {"terminated": {}}}],
                "conditions": [scheduled_condition],
            }
            raw_pod["spec"]["nodeName"] = node_name
        return raw_pod

    return _create


class TestNodeResourcesHandler:
    @pytest.fixture
    def kube_client(self) -> KubeClient:
        kube_client = mock.AsyncMock(spec=KubeClient)
        kube_client.get_node.side_effect = [
            Node(
                "minikube1",
                allocatable_resources=NodeResources(cpu=1, memory=1024, gpu=1),
            ),
            Node(
                "minikube2",
                allocatable_resources=NodeResources(cpu=2, memory=4096, gpu=2),
            ),
        ]
        return kube_client

    @pytest.fixture
    def handler(self, kube_client: KubeClient) -> NodeResourcesHandler:
        return NodeResourcesHandler(kube_client)

    async def test_init_pending(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job")]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 0
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources()

    async def test_init_scheduled(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job", is_scheduled=True)]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is True
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(0.9, 896)

    async def test_init_running(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [
            create_pod("job", gpu=0, is_running=True),
            create_pod(is_running=True),
            create_pod(node_name="minikube2", is_running=True),
        ]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 2
        assert handler.is_pod_bound_to_node("job") is True
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.8, memory=768)
        resources = handler.get_node_free_resources("minikube2")
        assert resources == NodeResources(cpu=1.9, memory=3968, gpu=1)

    async def test_init_succeeded(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job", is_terminated=True)]
        await handler.init(pods)

        assert len(handler.get_nodes()) == 0
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube1")
        assert resources == NodeResources()

    async def test_handle_added_pending(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(PodWatchEvent.create_added(create_pod("job")))

        assert len(handler.get_nodes()) == 0
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources()

    async def test_handle_added_running(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is True
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.9, memory=896)

    async def test_handle_added_running_multiple_times(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is True
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.9, memory=896)

    async def test_handle_modified_succeeded(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod(gpu=0, is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_modified(create_pod("job", is_terminated=True))
        )

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=1)

    async def test_handle_deleted(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod(gpu=0, is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod("job", is_terminated=True))
        )

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=1)

    async def test_handle_deleted_multiple_times(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod(gpu=0, is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod("job", is_terminated=True))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod("job", is_terminated=True))
        )

        assert len(handler.get_nodes()) == 1
        assert handler.is_pod_bound_to_node("job") is False
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources(cpu=0.9, memory=896, gpu=1)

    async def test_handle_deleted_last(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod("job", is_terminated=True))
        )

        assert len(handler.get_nodes()) == 0
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources()

    async def test_handle_deleted_not_existing(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod(is_terminated=True))
        )

        assert len(handler.get_nodes()) == 0
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources()

    async def test_get_node_free_resources_unknown_node(
        self, handler: NodeResourcesHandler
    ) -> None:
        resources = handler.get_node_free_resources("minikube")
        assert resources == NodeResources()


class TestIdlePodsHandler:
    @pytest.fixture
    def create_pod(self, create_pod: PodFactory) -> PodFactory:
        def _create(*args: Any, is_idle: bool = True, **kwargs: Any) -> dict[str, Any]:
            if is_idle:
                kwargs["labels"] = {"platform.neuromation.io/idle": "true"}
            return create_pod(*args, **kwargs)

        return _create

    @pytest.fixture
    def handler(self) -> IdlePodsHandler:
        return IdlePodsHandler()

    async def test_init_non_idle(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod(is_idle=False)]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")
        assert len(idle_pods) == 0

    async def test_init_pending(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod()]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")
        assert len(idle_pods) == 0

    async def test_init_scheduled(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod(is_scheduled=True)]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")
        assert len(idle_pods) == 1

    async def test_init_running(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod(is_running=True)]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")
        assert len(idle_pods) == 1

    async def test_init_succeeded(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod(is_terminated=True)]
        await handler.init(pods)

        idle_pods = handler.get_pods("minikube")
        assert len(idle_pods) == 0

    async def test_handle_added_non_idle(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod(is_idle=False, is_running=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_added_pending(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod())
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_added_scheduled(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod(is_scheduled=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

    async def test_handle_added_running(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod(is_running=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

    async def test_handle_modified_succeeded(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod("idle-job", is_running=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

        event = PodWatchEvent.create_modified(
            create_pod("idle-job", is_terminated=True)
        )
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_deleted(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_added(create_pod("idle-job", is_running=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 1

        event = PodWatchEvent.create_deleted(create_pod("idle-job", is_terminated=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_handle_deleted_not_existing(
        self, handler: IdlePodsHandler, create_pod: PodFactory
    ) -> None:
        event = PodWatchEvent.create_deleted(create_pod(is_terminated=True))
        await handler.handle(event)

        pods = handler.get_pods("minikube")
        assert len(pods) == 0

    async def test_get_pods_unknown_node(self, handler: IdlePodsHandler) -> None:
        idle_pods = handler.get_pods("minikube")

        assert len(idle_pods) == 0
