import asyncio
import json
import uuid
from dataclasses import dataclass
from pathlib import PurePath
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    Optional,
    Sequence,
)
from urllib.parse import urlsplit

import pytest
from aioelasticsearch import Elasticsearch
from async_timeout import timeout
from yarl import URL

from platform_api.config import (
    AuthConfig,
    ClusterConfig,
    Config,
    DatabaseConfig,
    IngressConfig,
    LoggingConfig,
    OAuthConfig,
    RegistryConfig,
    ServerConfig,
    StorageConfig,
)
from platform_api.elasticsearch import ElasticsearchConfig
from platform_api.orchestrator.kube_client import JobNotFoundException
from platform_api.orchestrator.kube_orchestrator import (
    KubeClient,
    KubeConfig,
    KubeOrchestrator,
)
from platform_api.redis import RedisConfig
from platform_api.resource import GPUModel, ResourcePoolType


pytest_plugins = [
    "tests.integration.docker",
    "tests.integration.redis",
    "tests.integration.auth",
    "tests.integration.elasticsearch",
]


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()  # type: ignore
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def kube_config_payload() -> Dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        "kubectl", "config", "view", "-o", "json", stdout=asyncio.subprocess.PIPE
    )
    output, _ = await process.communicate()
    payload_str = output.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
async def kube_config_cluster_payload(kube_config_payload: Dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
async def kube_config_user_payload(kube_config_payload: Dict[str, Any]) -> Any:
    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    return users[user_name]


@pytest.fixture(scope="session")
async def kube_config(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    storage_config = StorageConfig.create_host(host_mount_path=PurePath("/tmp"))
    registry_config = RegistryConfig()
    return KubeConfig(
        storage=storage_config,
        registry=registry_config,
        jobs_ingress_name="platformjobsingress",
        jobs_ingress_auth_name="platformjobsingressauth",
        jobs_domain_name_template="{job_id}.jobs.neu.ro",
        named_jobs_domain_name_template="{job_name}-{job_owner}.jobs.neu.ro",
        ssh_domain_name="ssh.platform.neuromation.io",
        ssh_auth_domain_name="ssh-auth.platform.neuromation.io",
        endpoint_url=cluster["server"],
        cert_authority_path=cluster["certificate-authority"],
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        job_deletion_delay_s=0,
        node_label_gpu="gpu",
        resource_pool_types=[
            ResourcePoolType(),
            ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel")),
        ],
        orphaned_job_owner="compute",
        node_label_preemptible="preemptible",
        namespace="platformapi-tests",
    )


@pytest.fixture(scope="session")
async def kube_ingress_ip(kube_config_cluster_payload: Dict[str, Any]) -> str:
    cluster = kube_config_cluster_payload
    return urlsplit(cluster["server"]).hostname


@dataclass(frozen=True)
class NodeTaint:
    key: str
    value: str
    effect: str = "NoSchedule"

    def to_primitive(self) -> Dict[str, Any]:
        return {"key": self.key, "value": self.value, "effect": self.effect}


class TestKubeClient(KubeClient):

    def _generate_endpoint_url(self, name: str, namespace: str) -> str:
        return f"{namespace}/endpoints/{name}"

    @property
    def _nodes_url(self) -> str:
        return f"{self._api_v1_url}/nodes"

    def _generate_node_url(self, name: str) -> str:
        return f"{self._nodes_url}/{name}"

    async def get_endpoint(self, name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        url = self._generate_endpoint_url(name, namespace or self._namespace_url)
        return await self._request(method="GET", url=url)

    async def request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await self._request(*args, **kwargs)

    async def get_raw_pod(self, name: str) -> Dict[str, Any]:
        url = self._generate_pod_url(name)
        return await self._request(method="GET", url=url)

    async def set_raw_pod_status(
        self, name: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = self._generate_pod_url(name) + "/status"
        return await self._request(method="PUT", url=url, json=payload)

    async def wait_pod_scheduled(
        self,
        pod_name: str,
        node_name: str,
        timeout_s: float = 5.0,
        interval_s: float = 1.0,
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    raw_pod = await self.get_raw_pod(pod_name)
                    pod_has_node = raw_pod["spec"].get("nodeName") == node_name
                    pod_is_scheduled = "PodScheduled" in [
                        cond["type"]
                        for cond in raw_pod["status"].get("conditions", [])
                        if cond["status"]
                    ]
                    if pod_has_node and pod_is_scheduled:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail("Pod unscheduled")

    async def wait_pod_non_existent(
        self, pod_name: str, timeout_s: float = 5.0, interval_s: float = 1.0
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    try:
                        await self.get_pod(pod_name)
                    except JobNotFoundException:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail("Pod still exists")

    async def wait_pod_is_terminated(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        try:
            async with timeout(timeout_s):
                while True:
                    pod_status = await self.get_pod_status(pod_name)
                    if pod_status.container_status.is_terminated:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            pytest.fail("Pod has not terminated yet")

    async def create_node(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None,
        taints: Optional[Sequence[NodeTaint]] = None,
    ) -> None:
        taints = taints or []
        payload = {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name, "labels": labels or {}},
            "spec": {"taints": [taint.to_primitive() for taint in taints]},
            "status": {
                "capacity": {
                    "pods": "110",
                    "memory": "1Gi",
                    "cpu": 2,
                    "nvidia.com/gpu": 1,
                },
                "conditions": [{"status": "True", "type": "Ready"}],
            },
        }
        url = self._nodes_url
        result = await self._request(method="POST", url=url, json=payload)
        self._check_status_payload(result)

    async def delete_node(self, name: str) -> None:
        url = self._generate_node_url(name)
        result = await self.request(method="DELETE", url=url)
        self._check_status_payload(result)


@pytest.fixture(scope="session")
async def kube_client(kube_config: KubeConfig) -> AsyncIterator[KubeClient]:
    config = kube_config
    # TODO (A Danshyn 06/06/18): create a factory method
    client = TestKubeClient(
        base_url=config.endpoint_url,
        cert_authority_path=config.cert_authority_path,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        namespace=config.namespace,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
    )
    async with client:
        yield client


@pytest.fixture(scope="session")
async def nfs_volume_server(kube_client: TestKubeClient) -> Any:
    payload = await kube_client.get_endpoint("platformstoragenfs", namespace="default")
    return payload["subsets"][0]["addresses"][0]["ip"]


@pytest.fixture(scope="session")
async def kube_config_nfs(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
    nfs_volume_server: Optional[str],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    storage_config = StorageConfig.create_nfs(
        host_mount_path=PurePath("/var/storage"),
        nfs_server=nfs_volume_server,
        nfs_export_path=PurePath("/var/storage"),
    )
    registry_config = RegistryConfig()
    return KubeConfig(
        storage=storage_config,
        registry=registry_config,
        endpoint_url=cluster["server"],
        cert_authority_path=cluster["certificate-authority"],
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        jobs_ingress_name="platformjobsingress",
        jobs_ingress_auth_name="platformjobsingressauth",
        jobs_domain_name_template="{job_id}.jobs.neu.ro",
        named_jobs_domain_name_template="{job_name}-{job_owner}.jobs.neu.ro",
        ssh_domain_name="ssh.platform.neuromation.io",
        ssh_auth_domain_name="ssh-auth.platform.neuromation.io",
        node_label_gpu="gpu",
        resource_pool_types=[ResourcePoolType()],
        orphaned_job_owner="compute",
        namespace="platformapi-tests",
    )


@pytest.fixture
async def kube_orchestrator(
    kube_config: KubeConfig, es_client: Optional[Elasticsearch], event_loop: Any
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(config=kube_config, es_client=es_client)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def kube_orchestrator_nfs(
    kube_config_nfs: KubeConfig, es_client: Optional[Elasticsearch], event_loop: Any
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(config=kube_config_nfs, es_client=es_client)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def delete_node_later(
    kube_client: TestKubeClient
) -> AsyncIterator[Callable[[str], Awaitable[None]]]:
    nodes = []

    async def _add_node(node: str) -> None:
        nodes.append(node)

    yield _add_node

    for node in nodes:
        try:
            await kube_client.delete_node(node)
        except Exception:
            pass


@pytest.fixture
def kube_node() -> str:
    return "minikube"


@pytest.fixture
async def kube_node_gpu(
    kube_config: KubeConfig,
    kube_client: TestKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    assert kube_config.node_label_gpu is not None
    labels = {kube_config.node_label_gpu: "gpumodel"}
    await kube_client.create_node(node_name, labels=labels)

    yield node_name


@pytest.fixture
async def kube_node_preemptible(
    kube_config: KubeConfig,
    kube_client: TestKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    assert kube_config.node_label_preemptible is not None
    labels = {kube_config.node_label_preemptible: "true"}
    taints = [NodeTaint(key=kube_config.node_label_preemptible, value="true")]
    await kube_client.create_node(node_name, labels=labels, taints=taints)

    yield node_name


@pytest.fixture
def config_factory(
    kube_config: KubeConfig,
    redis_config: RedisConfig,
    auth_config: AuthConfig,
    es_config: ElasticsearchConfig,
) -> Callable[..., Config]:
    def _factory(**kwargs: Any) -> Config:
        server_config = ServerConfig()
        database_config = DatabaseConfig(redis=redis_config)
        logging_config = LoggingConfig(elasticsearch=es_config)
        ingress_config = IngressConfig(
            storage_url=URL("https://neu.ro/api/v1/storage"),
            users_url=URL("https://neu.ro/api/v1/users"),
            monitoring_url=URL("https://neu.ro/api/v1/monitoring"),
        )
        cluster_config = ClusterConfig(
            name="default",
            orchestrator=kube_config,
            logging=logging_config,
            ingress=ingress_config,
        )
        return Config(
            server=server_config,
            cluster=cluster_config,
            database=database_config,
            auth=auth_config,
            **kwargs,
        )

    return _factory


@pytest.fixture
def config(config_factory: Callable[..., Config]) -> Config:
    return config_factory()


@pytest.fixture
def config_with_oauth(
    config_factory: Callable[..., Config], oauth_config_dev: Optional[OAuthConfig]
) -> Config:
    return config_factory(oauth=oauth_config_dev)
