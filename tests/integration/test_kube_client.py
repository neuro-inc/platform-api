from __future__ import annotations

import asyncio
import shlex
import tempfile
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any

import aiohttp
import aiohttp.web
import pytest
from apolo_kube_client.errors import KubeClientException, ResourceExists

from platform_api.config import KubeConfig
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobNotFoundException,
    JobRequest,
)
from platform_api.orchestrator.kube_client import (
    DockerRegistrySecret,
    EventHandler,
    Ingress,
    KubeClient,
    NodeWatcher,
    PodDescriptor,
    PodWatcher,
    Service,
    WatchEvent,
)
from platform_api.orchestrator.kube_config import KubeClientAuthType

from .api import ApiRunner
from .conftest import MyKubeClient

PodFactory = Callable[..., Awaitable[PodDescriptor]]


@pytest.fixture
async def delete_ingress_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[Ingress], Awaitable[None]]]:
    ingresses = []

    async def _add(ingress: Ingress) -> None:
        ingresses.append(ingress)

    yield _add

    for ingress in ingresses:
        try:
            await kube_client.delete_ingress(kube_client.namespace, ingress.name)
        except Exception:
            pass


@pytest.fixture
async def delete_service_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[Service], Awaitable[None]]]:
    services = []

    async def _add(service: Service) -> None:
        services.append(service)

    yield _add

    for service in services:
        try:
            await kube_client.delete_service(kube_client.namespace, service.name)
        except Exception:
            pass


class TestKubeClientTokenUpdater:
    @pytest.fixture
    async def kube_app(self) -> aiohttp.web.Application:
        async def _get_nodes(request: aiohttp.web.Request) -> aiohttp.web.Response:
            auth = request.headers["Authorization"]
            token = auth.split()[-1]
            app["token"]["value"] = token
            return aiohttp.web.json_response({"kind": "NodeList", "items": []})

        app = aiohttp.web.Application()
        app["token"] = {"value": ""}
        app.router.add_routes([aiohttp.web.get("/api/v1/nodes", _get_nodes)])
        return app

    @pytest.fixture
    async def kube_server(
        self, kube_app: aiohttp.web.Application, unused_tcp_port_factory: Any
    ) -> AsyncIterator[str]:
        runner = ApiRunner(kube_app, port=unused_tcp_port_factory())
        api_address = await runner.run()
        yield f"http://{api_address.host}:{api_address.port}"
        await runner.close()

    @pytest.fixture
    def kube_token_path(self) -> Iterator[str]:
        _, path = tempfile.mkstemp()
        Path(path).write_text("token-1")
        yield path
        Path(path).unlink()

    @pytest.fixture
    async def kube_client(
        self, kube_server: str, kube_token_path: str
    ) -> AsyncIterator[KubeClient]:
        async with KubeClient(
            base_url=kube_server,
            namespace="default",
            auth_type=KubeClientAuthType.TOKEN,
            token_path=kube_token_path,
            token_update_interval_s=1,
        ) as client:
            yield client

    async def test_token_periodically_updated(
        self,
        kube_app: aiohttp.web.Application,
        kube_client: KubeClient,
        kube_token_path: str,
    ) -> None:
        await kube_client.get_nodes()
        assert kube_app["token"]["value"] == "token-1"

        Path(kube_token_path).write_text("token-2")
        await asyncio.sleep(2)

        await kube_client.get_nodes()
        assert kube_app["token"]["value"] == "token-2"


class TestKubeClient:
    async def test_wait_pod_is_running_not_found(self, kube_client: KubeClient) -> None:
        with pytest.raises(JobNotFoundException):
            await kube_client.wait_pod_is_running(
                kube_client.namespace, pod_name="unknown"
            )

    async def test_wait_pod_is_running_timed_out(
        self,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=32 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(kube_client.namespace, pod)
        with pytest.raises(asyncio.TimeoutError):
            await kube_client.wait_pod_is_running(
                kube_client.namespace, pod_name=pod.name, timeout_s=0.1
            )

    async def test_wait_pod_is_running(
        self,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(kube_client.namespace, pod)
        await kube_client.wait_pod_is_running(
            kube_client.namespace, pod_name=pod.name, timeout_s=60.0
        )
        pod_status = await kube_client.get_pod_status(kube_client.namespace, pod.name)
        assert pod_status.phase in ("Running", "Succeeded")

    @pytest.mark.parametrize(
        "entrypoint,command",
        [(None, "/bin/echo false"), ("/bin/echo false", None), ("/bin/echo", "false")],
    )
    async def test_run_check_entrypoint_and_command(
        self,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
        entrypoint: str,
        command: str,
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            entrypoint=entrypoint,
            command=command,
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(kube_client.namespace, pod)
        await kube_client.wait_pod_is_terminated(
            kube_client.namespace, pod_name=pod.name, timeout_s=60.0
        )

        pod_finished = await kube_client.get_pod(kube_client.namespace, pod.name)

        # check that "/bin/echo" was not lost anywhere (and "false" was not executed):
        assert pod_finished.status
        assert pod_finished.status.phase == "Succeeded"

        if entrypoint is None:
            assert pod_finished.command is None
        else:
            assert pod_finished.command == shlex.split(entrypoint)

        if command is None:
            assert pod_finished.args is None
        else:
            assert pod_finished.args == shlex.split(command)

    async def test_create_docker_secret_non_existent_namespace(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=name,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.create_docker_secret(docker_secret)

    async def test_create_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret)

            with pytest.raises(KubeClientException, match="AlreadyExists"):
                await kube_client.create_docker_secret(docker_secret)
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    async def test_update_docker_secret_already_exists(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        try:
            await kube_client.create_docker_secret(docker_secret)
            await kube_client.update_docker_secret(docker_secret)
        finally:
            await kube_client.delete_secret(name, kube_config.namespace)

    async def test_update_docker_secret_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.update_docker_secret(docker_secret)

    async def test_update_docker_secret_create_non_existent(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )

        await kube_client.update_docker_secret(docker_secret, create_non_existent=True)
        await kube_client.update_docker_secret(docker_secret)

    async def test_get_raw_secret(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        docker_secret = DockerRegistrySecret(
            name=name,
            namespace=kube_config.namespace,
            username="testuser",
            password="testpassword",
            email="testuser@example.com",
            registry_server="registry.example.com",
        )
        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.get_raw_secret(name, kube_config.namespace)

        await kube_client.update_docker_secret(docker_secret, create_non_existent=True)
        raw = await kube_client.get_raw_secret(name, kube_config.namespace)
        assert raw["metadata"]["name"] == name
        assert raw["metadata"]["namespace"] == kube_config.namespace
        assert raw["data"] == docker_secret.to_primitive()["data"]

    @pytest.fixture
    async def delete_network_policy_later(
        self, kube_client: KubeClient
    ) -> AsyncIterator[Callable[[str], Awaitable[None]]]:
        names = []

        async def _add_name(name: str) -> None:
            names.append(name)

        yield _add_name

        for name in names:
            try:
                await kube_client.delete_network_policy(kube_client.namespace, name)
            except Exception:
                pass

    async def test_create_default_network_policy(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> None:
        name = str(uuid.uuid4())
        await delete_network_policy_later(name)
        payload = await kube_client.create_default_network_policy(
            kube_client.namespace,
            name,
            pod_labels={"testlabel": name},
            org_project_labels={"testlabel": name},
        )

        assert payload["metadata"]["name"] == name
        assert len(payload["spec"]["egress"]) == 3
        egress_via_labels = payload["spec"]["egress"][2]["to"]
        egress_pod_selectors = egress_via_labels[0]["podSelector"]
        egress_ns_selectors = egress_via_labels[1]["namespaceSelector"]
        assert egress_pod_selectors == {"matchLabels": {"testlabel": name}}
        assert egress_ns_selectors == {"matchLabels": {"testlabel": name}}

    async def test_create_default_network_policy_twice(
        self,
        kube_config: KubeConfig,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> None:
        name = str(uuid.uuid4())
        await delete_network_policy_later(name)
        payload = await kube_client.create_default_network_policy(
            kube_client.namespace,
            name,
            pod_labels={"testlabel": name},
            org_project_labels={"testlabel": name},
        )
        assert payload["metadata"]["name"] == name
        with pytest.raises(ResourceExists):
            await kube_client.create_default_network_policy(
                kube_client.namespace,
                name,
                {"testlabel": name},
                {"testlabel": name},
            )

    async def test_get_network_policy_not_found(self, kube_client: KubeClient) -> None:
        name = str(uuid.uuid4())
        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.get_network_policy(kube_client.namespace, name)

    async def test_delete_network_policy_not_found(
        self, kube_client: KubeClient
    ) -> None:
        name = str(uuid.uuid4())
        with pytest.raises(KubeClientException, match="NotFound"):
            await kube_client.delete_network_policy(kube_client.namespace, name)

    async def test_get_pod_events(
        self,
        kube_client: MyKubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="ubuntu:20.10",
            command="true",
            resources=ContainerResources(cpu=0.1, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(kube_client.namespace, pod)
        await kube_client.wait_pod_is_terminated(kube_client.namespace, pod.name)

        events = await kube_client.get_pod_events(pod.name, kube_client.namespace)

        assert events
        for event in events:
            involved_object = event.involved_object
            assert involved_object["kind"] == "Pod"
            assert involved_object["namespace"] == kube_client.namespace
            assert involved_object["name"] == pod.name

    async def test_get_pod_events_empty(
        self, kube_config: KubeConfig, kube_client: KubeClient
    ) -> None:
        pod_name = str(uuid.uuid4())
        events = await kube_client.get_pod_events(pod_name, kube_config.namespace)

        assert not events

    async def test_service_account_not_available(
        self,
        kube_client: KubeClient,
        delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    ) -> None:
        container = Container(
            image="lachlanevenson/k8s-kubectl:v1.10.3",
            command="get pods",
            resources=ContainerResources(cpu=0.2, memory=128 * 10**6),
        )
        job_request = JobRequest.create(container)
        pod = PodDescriptor.from_job_request(job_request)
        await delete_pod_later(pod)
        await kube_client.create_pod(kube_client.namespace, pod)
        await kube_client.wait_pod_is_terminated(
            kube_client.namespace, pod_name=pod.name, timeout_s=60.0
        )
        pod_status = await kube_client.get_pod_status(kube_client.namespace, pod.name)

        assert pod_status.container_status.exit_code != 0

    async def test_get_node(self, kube_client: KubeClient, kube_node: str) -> None:
        node = await kube_client.get_node(kube_node)

        assert node

    async def test_get_nodes(self, kube_client: KubeClient, kube_node: str) -> None:
        nodes = await kube_client.get_nodes()

        assert kube_node in [n.name for n in nodes]

    async def test_get_raw_nodes(self, kube_client: KubeClient, kube_node: str) -> None:
        result = await kube_client.get_raw_nodes()

        assert kube_node in [n["metadata"]["name"] for n in result.items]

    async def test_get_raw_pods(
        self, kube_client: KubeClient, create_pod: PodFactory
    ) -> None:
        pod = await create_pod()
        pod_list = await kube_client.get_raw_pods()
        pods = pod_list.items

        assert pod.name in [p["metadata"]["name"] for p in pods]

    async def test_get_raw_pods_all_namespaces(
        self, kube_client: KubeClient, create_pod: PodFactory
    ) -> None:
        pod = await create_pod()
        pod_list = await kube_client.get_raw_pods(all_namespaces=True)
        pods = pod_list.items

        assert pod.name in [p["metadata"]["name"] for p in pods]
        assert any(
            p["metadata"]["name"].startswith("kube-") for p in pods
        )  # kube-system pods

    @pytest.fixture
    async def create_pod(
        self, pod_factory: Callable[..., Awaitable[PodDescriptor]]
    ) -> Callable[..., Awaitable[PodDescriptor]]:
        async def _create(
            cpu: float = 0.1,
            memory: int = 128 * 10**6,
            labels: dict[str, str] | None = None,
        ) -> PodDescriptor:
            return await pod_factory(
                image="gcr.io/google_containers/pause:3.1",
                cpu=cpu,
                memory=memory,
                labels=labels,
                wait=True,
            )

        return _create

    @pytest.fixture
    async def create_ingress(
        self,
        kube_client: KubeClient,
        delete_ingress_later: Callable[[Ingress], Awaitable[None]],
    ) -> Callable[[str], Awaitable[Ingress]]:
        async def _f(job_id: str) -> Ingress:
            ingress_name = f"ingress-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}
            ingress = await kube_client.create_ingress(
                ingress_name, kube_client.namespace, labels=labels
            )
            await delete_ingress_later(ingress)
            return ingress

        return _f

    @pytest.fixture
    async def create_service(
        self,
        kube_client: KubeClient,
        delete_service_later: Callable[[Service], Awaitable[None]],
    ) -> Callable[[str], Awaitable[Service]]:
        async def _f(job_id: str) -> Service:
            service_name = f"service-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}
            service = Service(
                namespace=kube_client.namespace,
                name=service_name,
                target_port=8080,
                labels=labels,
            )
            service = await kube_client.create_service(kube_client.namespace, service)
            await delete_service_later(service)
            return service

        return _f

    @pytest.fixture
    async def create_network_policy(
        self,
        kube_client: KubeClient,
        delete_network_policy_later: Callable[[str], Awaitable[None]],
    ) -> Callable[[str], Awaitable[dict[str, Any]]]:
        async def _f(job_id: str) -> dict[str, Any]:
            np_name = f"networkpolicy-{uuid.uuid4().hex[:6]}"
            labels = {"platform.neuromation.io/job": job_id}

            await delete_network_policy_later(np_name)
            return await kube_client.create_egress_network_policy(
                kube_client.namespace,
                np_name,
                pod_labels=labels,
                labels=labels,
                rules=[{}],
            )

        return _f


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
        self, kube_client: KubeClient, handler: MyNodeEventHandler
    ) -> None:
        assert len(handler.node_names) > 0

        node_name = str(uuid.uuid4())
        try:
            await kube_client.create_node(
                node_name,
                {"pods": "110", "cpu": 1, "memory": "1024Mi"},
            )

            await asyncio.wait_for(handler.wait_for_node(node_name), 5)

            assert node_name in handler.node_names
        finally:
            await kube_client.delete_node(node_name)

    async def test_subscribe_after_start(
        self, node_watcher: NodeWatcher, handler: MyNodeEventHandler
    ) -> None:
        with pytest.raises(
            Exception, match="Subscription is not possible after watcher start"
        ):
            node_watcher.subscribe(handler)


class MyPodEventHandler(EventHandler):
    def __init__(self) -> None:
        self.pod_names: list[str] = []
        self._events: dict[str, asyncio.Event] = {}

    async def init(self, raw_pods: list[dict[str, Any]]) -> None:
        self.pod_names.extend([p["metadata"]["name"] for p in raw_pods])

    async def handle(self, event: WatchEvent) -> None:
        pod_name = event.resource["metadata"]["name"]
        self.pod_names.append(pod_name)
        waiter = self._events.get(pod_name)
        if waiter:
            del self._events[pod_name]
            waiter.set()

    async def wait_for_pod(self, name: str) -> None:
        if name in self.pod_names:
            return
        event = asyncio.Event()
        self._events[name] = event
        await event.wait()


class TestPodWatcher:
    @pytest.fixture
    def handler(self) -> MyPodEventHandler:
        return MyPodEventHandler()

    @pytest.fixture
    async def pod_watcher(
        self, kube_client: KubeClient, handler: MyPodEventHandler
    ) -> AsyncIterator[PodWatcher]:
        watcher = PodWatcher(kube_client)
        watcher.subscribe(handler)
        async with watcher:
            yield watcher

    @pytest.mark.usefixtures("pod_watcher")
    async def test_handle(
        self, handler: MyPodEventHandler, pod_factory: PodFactory
    ) -> None:
        assert len(handler.pod_names) > 0

        pod = await pod_factory(image="gcr.io/google_containers/pause:3.1")

        await asyncio.wait_for(handler.wait_for_pod(pod.name), 5)

        assert pod.name in handler.pod_names

    async def test_subscribe_after_start(
        self, pod_watcher: PodWatcher, handler: MyPodEventHandler
    ) -> None:
        with pytest.raises(
            Exception, match="Subscription is not possible after watcher start"
        ):
            pod_watcher.subscribe(handler)
