from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime

import pytest
from apolo_kube_client import (
    V1ContainerState,
    V1ContainerStateRunning,
    V1ContainerStateTerminated,
    V1ContainerStateWaiting,
    V1ContainerStatus,
    V1Node,
    V1NodeCondition,
    V1NodeStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodStatus,
)

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

NodeFactory = Callable[..., V1Node]


@pytest.fixture
def create_node() -> NodeFactory:
    def _create(
        cpu: float = 1, memory: int = 1024, gpu: int = 1, ready: bool = True
    ) -> V1Node:
        return V1Node(
            metadata=V1ObjectMeta(name="minikube"),
            status=V1NodeStatus(
                allocatable={
                    "cpu": str(cpu),
                    "memory": f"{memory}Mi",
                    "nvidia.com/gpu": str(gpu),
                },
                conditions=[
                    V1NodeCondition(
                        type="Ready",
                        status=str(ready),
                        last_transition_time=datetime.now(),
                    )
                ],
            ),
        )

    return _create


PodFactory = Callable[..., V1Pod]


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
        is_failed: bool = False,
    ) -> V1Pod:
        pod = PodDescriptor(
            name or f"pod-{uuid.uuid4()}",
            labels=labels or {},
            image="gcr.io/google_containers/pause:3.1",
            resources=Resources(cpu=cpu, memory=memory, nvidia_gpu=gpu),
        )
        raw_pod = pod.to_model()
        assert raw_pod.spec is not None
        assert raw_pod.spec.containers[0].image is not None
        raw_pod.metadata.creation_timestamp = datetime.now()
        raw_pod.status = V1PodStatus(phase="Pending")
        scheduled_condition = V1PodCondition(
            last_probe_time=None,
            last_transition_time=datetime.now(),
            status="True",
            type="PodScheduled",
        )
        if is_scheduled:
            raw_pod.status = V1PodStatus(
                phase="Pending",
                container_statuses=[
                    V1ContainerStatus(
                        image=raw_pod.spec.containers[0].image,
                        image_id="test-image-id",
                        name="test-name",
                        ready=False,
                        restart_count=0,
                        state=V1ContainerState(waiting=V1ContainerStateWaiting()),
                    )
                ],
                conditions=[scheduled_condition],
            )
            raw_pod.spec.node_name = node_name
        if is_running:
            raw_pod.status = V1PodStatus(
                phase="Running",
                container_statuses=[
                    V1ContainerStatus(
                        image=raw_pod.spec.containers[0].image,
                        image_id="test-image-id",
                        name="test-name",
                        ready=False,
                        restart_count=0,
                        state=V1ContainerState(running=V1ContainerStateRunning()),
                    )
                ],
                conditions=[scheduled_condition],
            )
            raw_pod.spec.node_name = node_name
        if is_terminated:
            raw_pod.status = V1PodStatus(
                phase="Succeeded",
                container_statuses=[
                    V1ContainerStatus(
                        image=raw_pod.spec.containers[0].image,
                        image_id="test-image-id",
                        name="test-name",
                        ready=False,
                        restart_count=0,
                        state=V1ContainerState(
                            terminated=V1ContainerStateTerminated(exit_code=1)
                        ),
                    )
                ],
                conditions=[scheduled_condition],
            )
            raw_pod.spec.node_name = node_name
        if is_failed:
            raw_pod.status = V1PodStatus(
                phase="Failed",
                reason="OutOfcpu",
            )
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
        node = Node.from_model(raw_node)
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
        node = Node.from_model(raw_node)
        await handler.handle(WatchEvent("ADDED", raw_node))

        nodes = list(handler.get_ready_nodes())
        assert nodes == [node]

    async def test_added_not_ready(
        self, handler: NodesHandler, create_node: NodeFactory
    ) -> None:
        raw_node = create_node(ready=False)
        await handler.handle(WatchEvent("ADDED", raw_node))

        nodes = list(handler.get_ready_nodes())
        assert nodes == []


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
        await handler.handle(WatchEvent("ADDED", create_pod("job")))

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_added_idle(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(
            WatchEvent(
                "ADDED",
                create_pod(
                    "job",
                    labels={"platform.neuromation.io/idle": "true"},
                    is_running=True,
                ),
            )
        )

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_added_running(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

    async def test_handle_added_running_multiple_times(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))

        assert handler.get_pod_node_name("job") == "minikube"
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=1)

    async def test_handle_modified_succeeded(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod(gpu=0, is_running=True)))
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))
        await handler.handle(
            WatchEvent("MODIFIED", create_pod("job", is_terminated=True))
        )

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6)

    async def test_handle_deleted(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod(gpu=0, is_running=True)))
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))
        await handler.handle(
            WatchEvent("DELETED", create_pod("job", is_terminated=True))
        )

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6)

    async def test_handle_deleted_multiple_times(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod(gpu=0, is_running=True)))
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))
        await handler.handle(
            WatchEvent("DELETED", create_pod("job", is_terminated=True))
        )
        await handler.handle(
            WatchEvent("DELETED", create_pod("job", is_terminated=True))
        )

        assert handler.get_pod_node_name("job") is None
        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources(cpu=0.1, memory=128 * 10**6, nvidia_gpu=0)

    async def test_handle_deleted_last(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_running=True)))
        await handler.handle(
            WatchEvent("DELETED", create_pod("job", is_terminated=True))
        )

        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_deleted_unknown(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("DELETED", create_pod(is_terminated=True)))

        resources = handler.get_resource_requests("minikube")
        assert resources == NodeResources()

    async def test_handle_failed(
        self, handler: NodeResourcesHandler, create_pod: PodFactory
    ) -> None:
        await handler.handle(WatchEvent("ADDED", create_pod("job", is_scheduled=True)))

        await handler.handle(
            WatchEvent("MODIFIED", create_pod("job", is_scheduled=True, is_failed=True))
        )

        assert handler.get_pod_node_name("job") is None
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
        await node_resources_handler.handle(WatchEvent("ADDED", create_pod("job")))

        assert scheduler.is_pod_scheduled("job") is False

        await node_resources_handler.handle(
            WatchEvent("MODIFIED", create_pod("job", is_running=True))
        )

        assert scheduler.is_pod_scheduled("job") is True

        await node_resources_handler.handle(
            WatchEvent("DELETED", create_pod("job", is_running=True))
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
        pod = PodDescriptor.from_model(create_pod())

        assert scheduler.get_schedulable_pods([pod]) == [pod]

        await node_resources_handler.handle(
            WatchEvent("ADDED", create_pod(cpu=1, is_running=True))
        )

        assert scheduler.get_schedulable_pods([pod]) == []

    async def test_get_schedulable_pods_already_scheduled(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        pod1 = PodDescriptor.from_model(create_pod(name="job", cpu=0.1))
        pod2 = PodDescriptor.from_model(create_pod(cpu=0.9, gpu=0))
        pod3 = PodDescriptor.from_model(create_pod(cpu=0.1, gpu=0))

        await node_resources_handler.handle(
            WatchEvent("ADDED", create_pod(name="job", cpu=0.1, is_running=True))
        )

        assert scheduler.get_schedulable_pods([pod1, pod2, pod3]) == [pod1, pod2]

    async def test_get_schedulable_pods_without_resources(
        self,
        scheduler: KubeOrchestratorScheduler,
        node_resources_handler: NodeResourcesHandler,
        create_pod: PodFactory,
    ) -> None:
        await node_resources_handler.handle(
            WatchEvent("ADDED", create_pod(name="job", cpu=1, is_running=True))
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
        pod = PodDescriptor.from_model(create_pod(is_running=True))
        assert scheduler.get_schedulable_pods([pod]) == [pod]

        await nodes_handler.handle(WatchEvent("MODIFIED", create_node(ready=False)))

        assert scheduler.get_schedulable_pods([pod]) == []
