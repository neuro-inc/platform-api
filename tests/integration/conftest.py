import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePath
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator, Optional
from urllib.parse import urlsplit

import aiohttp
import aiohttp.pytest_plugin
import aiohttp.web
import pytest
from async_generator import asynccontextmanager
from async_timeout import timeout
from yarl import URL

from platform_api.cluster_config import (
    ClusterConfig,
    IngressConfig,
    RegistryConfig,
    StorageConfig,
)
from platform_api.config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    JobsConfig,
    NotificationsConfig,
    OAuthConfig,
    ServerConfig,
)
from platform_api.config_client import ConfigClient
from platform_api.orchestrator.job_request import JobNotFoundException
from platform_api.orchestrator.kube_client import KubeClient, NodeTaint, Resources
from platform_api.orchestrator.kube_orchestrator import KubeConfig, KubeOrchestrator
from platform_api.redis import RedisConfig
from platform_api.resource import (
    GKEGPUModels,
    Preset,
    ResourcePoolType,
    TPUPreset,
    TPUResource,
)


pytest_plugins = [
    "tests.integration.api",
    "tests.integration.docker",
    "tests.integration.redis",
    "tests.integration.auth",
    "tests.integration.notifications",
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
def cert_authority_data_pem(
    kube_config_cluster_payload: Dict[str, Any]
) -> Optional[str]:
    ca_path = kube_config_cluster_payload["certificate-authority"]
    if ca_path:
        return Path(ca_path).read_text()
    return None


@pytest.fixture(scope="session")
async def kube_config_user_payload(kube_config_payload: Dict[str, Any]) -> Any:
    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    return users[user_name]


@pytest.fixture(scope="session")
def storage_config_host() -> StorageConfig:
    return StorageConfig.create_host(host_mount_path=PurePath("/tmp"))


@pytest.fixture(scope="session")
def storage_config_pvc() -> StorageConfig:
    return StorageConfig.create_pvc(pvc_name="platformstorage")


@pytest.fixture(scope="session")
def registry_config() -> RegistryConfig:
    return RegistryConfig()


@pytest.fixture(scope="session")
def kube_config_factory(
    kube_config_cluster_payload: Dict[str, Any],
    kube_config_user_payload: Dict[str, Any],
    cert_authority_data_pem: Optional[str],
) -> Iterator[Callable[..., KubeConfig]]:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload

    def _f(**kwargs: Any) -> KubeConfig:
        defaults = dict(
            jobs_ingress_class="nginx",
            jobs_domain_name_template="{job_id}.jobs.neu.ro",
            ssh_auth_domain_name="ssh-auth.platform.neuromation.io",
            endpoint_url=cluster["server"],
            cert_authority_data_pem=cert_authority_data_pem,
            cert_authority_path=None,  # disable, only `cert_authority_data_pem` works
            auth_cert_path=user["client-certificate"],
            auth_cert_key_path=user["client-key"],
            node_label_gpu="gpu",
            resource_pool_types=[
                ResourcePoolType(
                    presets=[
                        Preset(
                            name="gpu-small",
                            gpu=1,
                            cpu=7,
                            memory_mb=30720,
                            gpu_model=GKEGPUModels.K80.value.id,
                        ),
                        Preset(
                            name="gpu-large",
                            gpu=1,
                            cpu=7,
                            memory_mb=61440,
                            gpu_model=GKEGPUModels.V100.value.id,
                        ),
                    ]
                ),
                ResourcePoolType(
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                    presets=[
                        Preset(name="cpu-small", cpu=2, memory_mb=2048),
                        Preset(name="cpu-large", cpu=3, memory_mb=14336),
                        Preset(
                            name="tpu",
                            cpu=3,
                            memory_mb=14336,
                            tpu=TPUPreset(type="v2-8", software_version="1.14"),
                        ),
                    ],
                ),
                ResourcePoolType(gpu=1, gpu_model="gpumodel"),
            ],
            node_label_preemptible="preemptible",
            namespace="platformapi-tests",
            job_schedule_scaleup_timeout=5,
        )
        kwargs = {**defaults, **kwargs}
        return KubeConfig(**kwargs)

    yield _f


@pytest.fixture(scope="session")
async def kube_config(kube_config_factory: Callable[..., KubeConfig]) -> KubeConfig:
    return kube_config_factory()


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

    async def create_triggered_scaleup_event(self, pod_id: str) -> None:
        url = f"{self._namespace_url}/events"
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        data = {
            "apiVersion": "v1",
            "count": 1,
            "eventTime": None,
            "firstTimestamp": now_str,
            "involvedObject": {
                "apiVersion": "v1",
                "kind": "Pod",
                "name": pod_id,
                "namespace": self._namespace,
                "resourceVersion": "48102193",
                "uid": "eddfe678-86e9-11e9-9d65-42010a800018",
            },
            "kind": "Event",
            "lastTimestamp": now_str,
            "message": "TriggeredScaleUp",
            "metadata": {
                "creationTimestamp": now_str,
                "name": "job-cd109c3b-c36e-47d4-b3d6-8bb05a5e63ab.15a870d7e2bb228b",
                "namespace": self._namespace,
                "selfLink": (
                    f"/api/v1/namespaces/{self._namespace}"
                    "/events/{pod_id}.15a870d7e2bb228b"
                ),
                "uid": "cb886f64-8f96-11e9-9251-42010a800038",
            },
            "reason": "TriggeredScaleUp",
            "reportingComponent": "",
            "reportingInstance": "",
            "source": {"component": "cluster-autoscaler"},
            "type": "Normal",
        }

        await self._request(method="POST", url=url, json=data)


@pytest.fixture(scope="session")
async def kube_client_factory(kube_config: KubeConfig) -> Callable[..., MyKubeClient]:
    def _f(custom_kube_config: Optional[KubeConfig] = None) -> MyKubeClient:

        config = custom_kube_config or kube_config
        kube_client = MyKubeClient(
            base_url=config.endpoint_url,
            auth_type=config.auth_type,
            cert_authority_data_pem=config.cert_authority_data_pem,
            cert_authority_path=config.cert_authority_path,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,
            namespace=config.namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size,
        )
        return kube_client

    return _f


@pytest.fixture(scope="session")
async def kube_client(
    kube_client_factory: Callable[..., MyKubeClient]
) -> AsyncIterator[KubeClient]:
    async with kube_client_factory() as kube_client:
        yield kube_client


@pytest.fixture(scope="session")
async def nfs_volume_server(kube_client: MyKubeClient) -> Any:
    payload = await kube_client.get_endpoint("platformstoragenfs", namespace="default")
    return payload["subsets"][0]["addresses"][0]["ip"]


@pytest.fixture(scope="session")
def storage_config_nfs(nfs_volume_server: Optional[str]) -> StorageConfig:
    assert nfs_volume_server
    return StorageConfig.create_nfs(
        nfs_server=nfs_volume_server, nfs_export_path=PurePath("/var/storage")
    )


@pytest.fixture
async def kube_orchestrator_factory(
    storage_config_host: StorageConfig,
    registry_config: RegistryConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> Callable[..., KubeOrchestrator]:
    def _f(**kwargs: Any) -> KubeOrchestrator:
        defaults = dict(
            storage_config=storage_config_host,
            registry_config=registry_config,
            kube_config=kube_config,
        )
        kwargs = {**defaults, **kwargs}
        return KubeOrchestrator(**kwargs)

    return _f


@pytest.fixture
async def kube_orchestrator(
    kube_orchestrator_factory: Callable[..., KubeOrchestrator],
) -> AsyncIterator[KubeOrchestrator]:
    async with kube_orchestrator_factory() as kube_orchestrator:
        yield kube_orchestrator


@pytest.fixture
async def kube_orchestrator_nfs(
    storage_config_nfs: StorageConfig,
    registry_config: RegistryConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_nfs,
        registry_config=registry_config,
        kube_config=kube_config,
    )
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def kube_orchestrator_pvc(
    storage_config_pvc: StorageConfig,
    registry_config: RegistryConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_pvc,
        registry_config=registry_config,
        kube_config=kube_config,
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
    taints = [NodeTaint(key=Resources.gpu_key, value="present")]
    await kube_client.create_node(
        node_name, capacity=default_node_capacity, labels=labels, taints=taints
    )

    yield node_name


@pytest.fixture
async def kube_node_tpu(
    kube_config: KubeConfig,
    kube_client: MyKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    await kube_client.create_node(
        node_name,
        capacity={
            "pods": "110",
            "memory": "1Gi",
            "cpu": 2,
            "cloud-tpus.google.com/v2": 8,
        },
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
    kube_config: KubeConfig,
    redis_config: RedisConfig,
    auth_config: AuthConfig,
    jobs_config: JobsConfig,
    notifications_config: NotificationsConfig,
) -> Callable[..., Config]:
    def _factory(**kwargs: Any) -> Config:
        server_config = ServerConfig()
        database_config = DatabaseConfig(redis=redis_config)
        config_client = ConfigClient(base_url=URL("http://localhost:8082/api/v1"))
        return Config(
            server=server_config,
            database=database_config,
            auth=auth_config,
            jobs=jobs_config,
            notifications=notifications_config,
            config_client=config_client,
            **kwargs,
        )

    return _factory


@pytest.fixture
def cluster_config(
    kube_config: KubeConfig,
    storage_config_host: StorageConfig,
    registry_config: RegistryConfig,
) -> ClusterConfig:
    ingress_config = IngressConfig(
        storage_url=URL("https://neu.ro/api/v1/storage"),
        monitoring_url=URL("https://neu.ro/api/v1/monitoring"),
    )
    return ClusterConfig(
        name="default",
        orchestrator=kube_config,
        ingress=ingress_config,
        storage=storage_config_host,
        registry=registry_config,
    )


@pytest.fixture
def config(config_factory: Callable[..., Config]) -> Config:
    return config_factory()


@pytest.fixture
def config_with_oauth(
    config_factory: Callable[..., Config], oauth_config_dev: Optional[OAuthConfig]
) -> Config:
    return config_factory(oauth=oauth_config_dev)


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    app: aiohttp.web.Application, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    runner = aiohttp.web.AppRunner(app)
    try:
        await runner.setup()
        api_address = ApiAddress("0.0.0.0", port)
        site = aiohttp.web.TCPSite(runner, api_address.host, api_address.port)
        await site.start()
        yield api_address
    finally:
        await runner.shutdown()
        await runner.cleanup()


class ApiRunner:
    def __init__(self, app: aiohttp.web.Application, port: int) -> None:
        self._app = app
        self._port = port

        self._api_address_future: asyncio.Future[ApiAddress] = asyncio.Future()
        self._cleanup_future: asyncio.Future[None] = asyncio.Future()
        self._task: Optional[asyncio.Task[None]] = None

    async def _run(self) -> None:
        async with create_local_app_server(self._app, port=self._port) as api_address:
            self._api_address_future.set_result(api_address)
            await self._cleanup_future

    async def run(self) -> ApiAddress:
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._run())
        return await self._api_address_future

    async def close(self) -> None:
        if self._task:
            task = self._task
            self._task = None
            self._cleanup_future.set_result(None)
            await task

    @property
    def closed(self) -> bool:
        return not bool(self._task)
