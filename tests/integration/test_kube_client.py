from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from apolo_kube_client import (
    KubeClientSelector,
    V1Node,
    V1NodeCondition,
    V1NodeSpec,
    V1NodeStatus,
    V1ObjectMeta,
)

from platform_api.config import KubeConfig
from platform_api.old_kube_client.errors import KubeClientException
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobRequest,
)
from platform_api.orchestrator.kube_client import (
    DockerRegistrySecret,
    EventHandler,
    KubeClient,
    NodeWatcher,
    PodDescriptor,
    WatchEvent,
    create_pod,
    get_pod_status,
    wait_pod_is_terminated,
)

PodFactory = Callable[..., Awaitable[PodDescriptor]]


class TestKubeClient:
    async def test_create_docker_secret_non_existent_namespace(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.create_docker_secret(docker_secret, namespace=name)

    async def test_create_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret, kube_config.namespace)

            with pytest.raises(KubeClientException, match="AlreadyExists"):
                await kube_client.create_docker_secret(
                    docker_secret, kube_config.namespace
                )
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    async def test_update_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret, kube_config.namespace)
            await kube_client.update_docker_secret(docker_secret, kube_config.namespace)
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    async def test_update_docker_secret_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.update_docker_secret(docker_secret, kube_config.namespace)

    async def test_update_docker_secret_create_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        await kube_client.update_docker_secret(
            docker_secret, kube_config.namespace, create_non_existent=True
        )
        await kube_client.update_docker_secret(docker_secret, kube_config.namespace)

    async def test_service_account_not_available(
        self,
        kube_client_selector: KubeClientSelector,
        delete_pod_later: Callable[[PodDescriptor, str, str], Awaitable[None]],
    ) -> None:
        container = Container(
            image="lachlanevenson/k8s-kubectl:v1.10.3",
            command="get pods",
            resources=ContainerResources(cpu=0.2, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod, "org", "proj")
        async with kube_client_selector.get_client(
            org_name="org", project_name="proj"
        ) as client_proxy:
            await create_pod(client_proxy, pod)
            await wait_pod_is_terminated(
                client_proxy, pod_name=pod.name, timeout_s=60.0
            )
            pod_status = await get_pod_status(client_proxy, pod.name)

            assert pod_status.container_status
            assert pod_status.container_status.exit_code != 0

    async def test_get_raw_nodes(self, kube_client: KubeClient, kube_node: str) -> None:
        result = await kube_client.get_raw_nodes()

        assert kube_node in [n["metadata"]["name"] for n in result.items]


class MyNodeEventHandler(EventHandler):
    def __init__(self) -> None:
        self.node_names: list[str] = []
        self._events: dict[str, asyncio.Event] = {}

    async def init(self, raw_nodes: list[dict[str, Any]]) -> None:
        self.node_names.extend([p["metadata"]["name"] for p in raw_nodes])

    async def handle(self, event: WatchEvent) -> None:
        pod_name = event.resource["metadata"]["name"]
        self.node_names.append(pod_name)
        waiter = self._events.get(pod_name)
        if waiter:
            del self._events[pod_name]
            waiter.set()

    async def wait_for_node(self, name: str) -> None:
        if name in self.node_names:
            return
        event = asyncio.Event()
        self._events[name] = event
        await event.wait()


class TestNodeWatcher:
    @pytest.fixture
    def handler(self) -> MyNodeEventHandler:
        return MyNodeEventHandler()

    @pytest.fixture
    async def node_watcher(
        self, kube_client: KubeClient, handler: MyNodeEventHandler
    ) -> AsyncIterator[NodeWatcher]:
        watcher = NodeWatcher(kube_client)
        watcher.subscribe(handler)
        async with watcher:
            yield watcher

    @pytest.mark.usefixtures("node_watcher")
    async def test_handle(
        self, kube_client_selector: KubeClientSelector, handler: MyNodeEventHandler
    ) -> None:
        assert len(handler.node_names) > 0

        node_name = str(uuid.uuid4())
        try:
            await kube_client_selector.host_client.core_v1.node.create(
                V1Node(
                    metadata=V1ObjectMeta(name=node_name),
                    spec=V1NodeSpec(),
                    status=V1NodeStatus(
                        capacity={"pods": "110", "cpu": "1", "memory": "1024Mi"},
                        conditions=[V1NodeCondition(status="True", type="Ready")],
                    ),
                )
            )

            await asyncio.wait_for(handler.wait_for_node(node_name), 5)

            assert node_name in handler.node_names
        finally:
            await kube_client_selector.host_client.core_v1.node.delete(node_name)

    async def test_subscribe_after_start(
        self, node_watcher: NodeWatcher, handler: MyNodeEventHandler
    ) -> None:
        with pytest.raises(
            Exception, match="Subscription is not possible after watcher start"
        ):
            node_watcher.subscribe(handler)
