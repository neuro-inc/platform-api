import asyncio
import json
import uuid
from pathlib import PurePath
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, Optional
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
    JobsConfig,
    LoggingConfig,
    OAuthConfig,
    RegistryConfig,
    ServerConfig,
    StorageConfig,
)
from platform_api.elasticsearch import ElasticsearchConfig
from platform_api.orchestrator.job_request import JobNotFoundException
from platform_api.orchestrator.kube_client import KubeClient, NodeTaint
from platform_api.orchestrator.kube_orchestrator import KubeConfig, KubeOrchestrator
from platform_api.redis import RedisConfig
from platform_api.resource import GPUModel, ResourcePoolType
from tests.conftest import read_file_binary


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
def storage_config_host() -> StorageConfig:
    return StorageConfig.create_host(host_mount_path=PurePath("/tmp"))


@pytest.fixture(scope="session")
def registry_config() -> RegistryConfig:
    return RegistryConfig()


@pytest.fixture(scope="session")
async def kube_config(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    storage_config = StorageConfig.create_host(host_mount_path=PurePath("/tmp"))
    registry_config = RegistryConfig()
    ca_data = read_file_binary(cluster["certificate-authority"].encode("utf-8"))
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
        cert_authority_data=ca_data,
        cert_authority_path=None,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
        node_label_gpu="gpu",
        resource_pool_types=[
            ResourcePoolType(),
            ResourcePoolType(gpu=1, gpu_model=GPUModel(id="gpumodel")),
        ],
        node_label_preemptible="preemptible",
        namespace="platformapi-tests",
    )
    return kube_config


@pytest.fixture(scope="session")
async def kube_ingress_ip(kube_config_cluster_payload: Dict[str, Any]) -> str:
    cluster = kube_config_cluster_payload
    return urlsplit(cluster["server"]).hostname


class MyKubeClient(KubeClient):
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


@pytest.fixture(scope="session")
async def kube_client(kube_config: KubeConfig) -> AsyncIterator[MyKubeClient]:
    config = kube_config
    # TODO (A Danshyn 06/06/18): create a factory method
    client = MyKubeClient(
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
async def nfs_volume_server(kube_client: MyKubeClient) -> Any:
    payload = await kube_client.get_endpoint("platformstoragenfs", namespace="default")
    return payload["subsets"][0]["addresses"][0]["ip"]


@pytest.fixture(scope="session")
def storage_config_nfs(nfs_volume_server: Optional[str]) -> StorageConfig:
    assert nfs_volume_server
    return StorageConfig.create_nfs(
        host_mount_path=PurePath("/var/storage"),
        nfs_server=nfs_volume_server,
        nfs_export_path=PurePath("/var/storage"),
    )


@pytest.fixture(scope="session")
async def kube_config_nfs(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
) -> KubeConfig:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    kube_config = KubeConfig(
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
        namespace="platformapi-tests",
    )
    return kube_config


@pytest.fixture
async def kube_orchestrator(
    storage_config_host: StorageConfig,
    registry_config: RegistryConfig,
    kube_config: KubeConfig,
    es_client: Optional[Elasticsearch],
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_host,
        registry_config=registry_config,
        kube_config=kube_config,
        es_client=es_client,
    )
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def kube_orchestrator_nfs(
    storage_config_nfs: StorageConfig,
    registry_config: RegistryConfig,
    kube_config_nfs: KubeConfig,
    es_client: Optional[Elasticsearch],
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_nfs,
        registry_config=registry_config,
        kube_config=kube_config_nfs,
        es_client=es_client,
    )
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def delete_node_later(
    kube_client: MyKubeClient
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
def default_node_capacity() -> Dict[str, Any]:
    return {"pods": "110", "memory": "1Gi", "cpu": 2, "nvidia.com/gpu": 1}


@pytest.fixture
async def kube_node_gpu(
    kube_config: KubeConfig,
    kube_client: MyKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
    default_node_capacity: Dict[str, Any],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    assert kube_config.node_label_gpu is not None
    labels = {kube_config.node_label_gpu: "gpumodel"}
    await kube_client.create_node(
        node_name, capacity=default_node_capacity, labels=labels
    )

    yield node_name


@pytest.fixture
async def kube_node_preemptible(
    kube_config: KubeConfig,
    kube_client: MyKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
    default_node_capacity: Dict[str, Any],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    assert kube_config.node_label_preemptible is not None
    labels = {kube_config.node_label_preemptible: "true"}
    taints = [NodeTaint(key=kube_config.node_label_preemptible, value="true")]
    await kube_client.create_node(
        node_name, capacity=default_node_capacity, labels=labels, taints=taints
    )

    yield node_name


@pytest.fixture
def jobs_config() -> JobsConfig:
    return JobsConfig(orphaned_job_owner="compute", deletion_delay_s=0)


@pytest.fixture
def config_factory(
    storage_config_host: StorageConfig,
    registry_config: RegistryConfig,
    kube_config: KubeConfig,
    redis_config: RedisConfig,
    auth_config: AuthConfig,
    es_config: ElasticsearchConfig,
    jobs_config: JobsConfig,
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
            storage=storage_config_host,
            registry=registry_config,
            orchestrator=kube_config,
            logging=logging_config,
            ingress=ingress_config,
        )
        return Config(
            server=server_config,
            cluster=cluster_config,
            database=database_config,
            auth=auth_config,
            jobs=jobs_config,
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
