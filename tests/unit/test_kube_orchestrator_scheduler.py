from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest

from platform_api.orchestrator.kube_client import (
    Node,
    NodeResources,
    PodDescriptor,
    Resources,
    WatchEvent,
)
from platform_api.orchestrator.kube_orchestrator_scheduler import (
    KubeOrchestratorScheduler,
    NodeResourcesHandler,
    NodesHandler,
)

NodeFactory = Callable[..., dict[str, Any]]


@pytest.fixture
def create_node() -> NodeFactory:
    def _create(
        cpu: float = 1, memory: int = 1024, gpu: int = 1, ready: bool = True
    ) -> dict[str, Any]:
        return {
            "metadata": {"name": "minikube"},
            "status": {
                "allocatable": {
                    "cpu": cpu,
                    "memory": f"{memory}Mi",
                    "nvidia.com/gpu": gpu,
                },
                "conditions": [
                    {
                        "type": "Ready",
                        "status": str(ready),
                        "lastTransitionTime": datetime.now().isoformat(),
                    }
                ],
            },
        }

    return _create


PodFactory = Callable[..., dict[str, Any]]


@pytest.fixture
def create_pod() -> PodFactory:
    def _create(
        name: str | None = None,
        *,
        cpu: float = 0.1,
        memory: int = 128 * 10**6,
        gpu: int = 1,
        labels: dict[str, str] | None = None,
        node_name: str | None = "minikube",
        is_scheduled: bool = False,
        is_running: bool = False,
        is_terminated: bool = False,
    ) -> dict[str, Any]:
        pod = PodDescriptor(
            name or f"pod-{uuid.uuid4()}",
            labels=labels or {},
            image="gcr.io/google_containers/pause:3.1",
            resources=Resources(cpu=cpu, memory=memory, nvidia_gpu=gpu),
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
        if is_running:
            raw_pod["status"] = {
                "phase": "Running",
                "containerStatuses": [{"state": {"running": {}}}],
                "conditions": [scheduled_condition],
            }
            raw_pod["spec"]["nodeName"] = node_name
        if is_terminated:
            raw_pod["status"] = {
                "phase": "Succeeded",
                "containerStatuses": [{"state": {"terminated": {}}}],
                "conditions": [scheduled_condition],
            }
            raw_pod["spec"]["nodeName"] = node_name
        return raw_pod

    return _create


class TestNodesHandler:
    @pytest.fixture
    def handler(self) -> NodesHandler:
        return NodesHandler()

    async def test_init_ready(
        self, handler: NodesHandler, create_node: NodeFactory
    ) -> None:
        raw_node = create_node()
        node = Node.from_primitive(raw_node)
        await handler.init([raw_node])

        nodes = list(handler.get_ready_nodes())
        assert nodes == [node]

    async def test_init_not_ready(
        self, handler: NodesHandler, create_node: NodeFactory
    ) -> None:
        raw_node = create_node(ready=False)
        await handler.init([raw_node])

        nodes = list(handler.get_ready_nodes())
        assert nodes == []

    async def test_added_ready(
        self, handler: NodesHandler, create_node: NodeFactory
    ) -> None:
        raw_node = create_node()
        node = Node.from_primitive(raw_node)
        await handler.handle(WatchEvent.create_added(raw_node))

        nodes = list(handler.get_ready_nodes())
        assert nodes == [node]

    async def test_added_not_ready(
        self, handler: NodesHandler, create_node: NodeFactory
    ) -> None:
        raw_node = create_node(ready=False)
        await handler.handle(WatchEvent.create_added(raw_node))

        nodes = list(handler.get_ready_nodes())
        assert nodes == []


PodWatchEvent = WatchEvent


class TestNodeResourcesHandler:
    @pytest.fixture
    def handler(self) -> NodeResourcesHandler:
        return NodeResourcesHandler()

    async def test_init_pending(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job")]
        await handler.init(pods)

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_init_idle(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [
            create_pod(
                "job", labels={"platform.neuromation.io/idle": "true"}, is_running=True
            )
        ]
        await handler.init(pods)

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_init_scheduled(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job", is_scheduled=True)]
        await handler.init(pods)

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

    async def test_init_running(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [
            create_pod("job", gpu=0, is_running=True),
            create_pod(is_running=True),
            create_pod(node_name="minikube2", is_running=True),
        ]
        await handler.init(pods)

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.2, memory=256 * 10**6, nvidia_gpu=1)
        resources = handler.get_resource_requests("minikube2")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

    async def test_init_succeeded(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        pods = [create_pod("job", is_terminated=True)]
        await handler.init(pods)

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube1")
        assert resources == NodeResources()

    async def test_handle_added_pending(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(PodWatchEvent.create_added(create_pod("job")))

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_added_idle(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(
                create_pod(
                    "job",
                    labels={"platform.neuromation.io/idle": "true"},
                    is_running=True,
                )
            )
        )

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_added_running(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

    async def test_handle_added_running_multiple_times(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

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

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6)

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

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6)

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

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=0)

    async def test_handle_deleted_last(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_added(create_pod("job", is_running=True))
        )
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod("job", is_terminated=True))
        )

        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_deleted_unknown(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            PodWatchEvent.create_deleted(create_pod(is_terminated=True))
        )

        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_get_resource_requests_unknown_node(
        self, handler: NodeResourcesHandler
    ) -> None:
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()


class TestKubeOrchestratorScheduler:
    @pytest.fixture
    async def nodes_handler(self, create_node: NodeFactory) -> NodesHandler:
        handler = NodesHandler()
        await handler.init([create_node()])
        return handler

    @pytest.fixture
    def node_resources_handler(self) -> NodeResourcesHandler:
        return NodeResourcesHandler()

    @pytest.fixture
    def scheduler(
        self, nodes_handler: NodesHandler, node_resources_handler: NodeResourcesHandler
    ) -> KubeOrchestratorScheduler:
        return KubeOrchestratorScheduler(nodes_handler, node_resources_handler)

    async def test_is_pod_scheduled(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        await node_resources_handler.handle(WatchEvent.create_added(create_pod("job")))

        assert scheduler.is_pod_scheduled("job") is False

        await node_resources_handler.handle(
            WatchEvent.create_modified(create_pod("job", is_running=True))
        )

        assert scheduler.is_pod_scheduled("job") is True

        await node_resources_handler.handle(
            WatchEvent.create_deleted(create_pod("job", is_running=True))
        )

        assert scheduler.is_pod_scheduled("job") is False

    def test_is_pod_scheduled_unknown(
        self, scheduler: KubeOrchestratorScheduler
    ) -> None:
        assert scheduler.is_pod_scheduled("unknown") is False

    async def test_get_schedulable_pods(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        pod = PodDescriptor.from_primitive(create_pod())

        assert scheduler.get_schedulable_pods([pod]) == [pod]

        await node_resources_handler.handle(
            WatchEvent.create_added(create_pod(cpu=1, is_running=True))
        )

        assert scheduler.get_schedulable_pods([pod]) == []

    async def test_get_schedulable_pods_already_scheduled(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        pod1 = PodDescriptor.from_primitive(create_pod(name="job", cpu=0.1))
        pod2 = PodDescriptor.from_primitive(create_pod(cpu=0.9, gpu=0))
        pod3 = PodDescriptor.from_primitive(create_pod(cpu=0.1, gpu=0))

        await node_resources_handler.handle(
            WatchEvent.create_added(create_pod(name="job", cpu=0.1, is_running=True))
        )

        assert scheduler.get_schedulable_pods([pod1, pod2, pod3]) == [pod1, pod2]

    async def test_get_schedulable_pods_without_resources(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        await node_resources_handler.handle(
            WatchEvent.create_added(create_pod(name="job", cpu=1, is_running=True))
        )

        pod = PodDescriptor(name="job", image="job")
        assert scheduler.get_schedulable_pods([pod]) == [pod]

    def test_get_schedulable_pods_cannot_schedule_on_node(
        self, scheduler: KubeOrchestratorScheduler
    ) -> None:
        pod = PodDescriptor(
            name="job", image="job", node_selector={"unknown": "unknown"}
        )
        assert scheduler.get_schedulable_pods([pod]) == []

    async def test_get_schedulable_pods_node_not_ready(
        self,
        scheduler: KubeOrchestratorScheduler,
        nodes_handler: NodesHandler,
        create_node: NodeFactory,
        create_pod: PodFactory,
    ) -> None:
        pod = PodDescriptor.from_primitive(create_pod(is_running=True))
        assert scheduler.get_schedulable_pods([pod]) == [pod]

        await nodes_handler.handle(WatchEvent.create_modified(create_node(ready=False)))

        assert scheduler.get_schedulable_pods([pod]) == []
