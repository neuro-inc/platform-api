import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path, PurePath
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
)
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
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from platform_api.config import (
    AuthConfig,
    Config,
    CORSConfig,
    DatabaseConfig,
    JobPolicyEnforcerConfig,
    JobsConfig,
    NotificationsConfig,
    OAuthConfig,
    PostgresConfig,
    ServerConfig,
    ZipkinConfig,
)
from platform_api.orchestrator.job_request import JobNotFoundException
from platform_api.orchestrator.kube_client import (
    AlreadyExistsException,
    KubeClient,
    NodeTaint,
    Resources,
)
from platform_api.orchestrator.kube_config import KubeClientAuthType
from platform_api.orchestrator.kube_orchestrator import KubeConfig, KubeOrchestrator
from platform_api.resource import (
    GKEGPUModels,
    Preset,
    ResourcePoolType,
    TPUPreset,
    TPUResource,
)


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()
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
def registry_config(token_factory: Callable[[str], str]) -> RegistryConfig:
    return RegistryConfig(username="compute", password=token_factory("compute"))


@pytest.fixture(scope="session")
def orchestrator_config_factory() -> Iterator[Callable[..., OrchestratorConfig]]:
    def _f(**kwargs: Any) -> OrchestratorConfig:
        defaults = dict(
            jobs_domain_name_template="{job_id}.jobs.neu.ro",
            jobs_internal_domain_name_template="{job_id}.platformapi-tests",
            resource_pool_types=[
                ResourcePoolType(
                    cpu=1.0,
                    available_cpu=1.0,
                    memory_mb=2048,
                    available_memory_mb=2048,
                ),
                ResourcePoolType(
                    cpu=1.0,
                    available_cpu=1.0,
                    memory_mb=2048,
                    available_memory_mb=2048,
                    is_preemptible=True,
                ),
                ResourcePoolType(
                    cpu=100,
                    available_cpu=100,
                    memory_mb=500_000,
                    available_memory_mb=500_000,
                ),
                ResourcePoolType(
                    cpu=1.0,
                    available_cpu=1.0,
                    memory_mb=2048,
                    available_memory_mb=2048,
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                ),
                ResourcePoolType(
                    cpu=1.0,
                    available_cpu=1.0,
                    memory_mb=2048,
                    available_memory_mb=2048,
                    gpu=1,
                    gpu_model="gpumodel",
                ),
            ],
            presets=[
                Preset(
                    name="gpu-small",
                    credits_per_hour=Decimal("10"),
                    gpu=1,
                    cpu=7,
                    memory_mb=30720,
                    gpu_model=GKEGPUModels.K80.value.id,
                ),
                Preset(
                    name="gpu-large",
                    credits_per_hour=Decimal("10"),
                    gpu=1,
                    cpu=7,
                    memory_mb=61440,
                    gpu_model=GKEGPUModels.V100.value.id,
                ),
                Preset(
                    name="gpu-large-p",
                    credits_per_hour=Decimal("10"),
                    gpu=1,
                    cpu=7,
                    memory_mb=61440,
                    gpu_model=GKEGPUModels.V100.value.id,
                    scheduler_enabled=True,
                    preemptible_node=True,
                ),
                Preset(
                    name="cpu-micro",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory_mb=100,
                ),
                Preset(
                    name="cpu-small",
                    credits_per_hour=Decimal("10"),
                    cpu=2,
                    memory_mb=2048,
                ),
                Preset(
                    name="cpu-large",
                    credits_per_hour=Decimal("10"),
                    cpu=3,
                    memory_mb=14336,
                ),
                Preset(
                    name="tpu",
                    credits_per_hour=Decimal("10"),
                    cpu=3,
                    memory_mb=14336,
                    tpu=TPUPreset(type="v2-8", software_version="1.14"),
                ),
            ],
            job_schedule_scaleup_timeout=5,
            allow_privileged_mode=True,
        )
        kwargs = {**defaults, **kwargs}
        return OrchestratorConfig(**kwargs)

    yield _f


@pytest.fixture(scope="session")
async def orchestrator_config(
    orchestrator_config_factory: Callable[..., OrchestratorConfig]
) -> OrchestratorConfig:
    return orchestrator_config_factory()


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
            endpoint_url=cluster["server"],
            auth_type=KubeClientAuthType.CERTIFICATE,
            cert_authority_data_pem=cert_authority_data_pem,
            cert_authority_path=None,  # disable, only `cert_authority_data_pem` works
            auth_cert_path=user["client-certificate"],
            auth_cert_key_path=user["client-key"],
            node_label_gpu="gpu",
            namespace="platformapi-tests",
        )
        kwargs = {**defaults, **kwargs}
        return KubeConfig(**kwargs)

    yield _f


@pytest.fixture(scope="session")
async def kube_config(kube_config_factory: Callable[..., KubeConfig]) -> KubeConfig:
    return kube_config_factory()


@pytest.fixture
def kube_job_nodes_factory(
    kube_client: KubeClient, delete_node_later: Callable[[str], Awaitable[None]]
) -> Callable[[OrchestratorConfig, KubeConfig], Awaitable[None]]:
    async def _create(
        orchestrator_config: OrchestratorConfig, kube_config: KubeConfig
    ) -> None:
        assert kube_config.node_label_job
        assert kube_config.node_label_node_pool
        assert kube_config.node_label_preemptible

        for pool_type in orchestrator_config.resource_pool_types:
            assert pool_type.name

            await delete_node_later(pool_type.name)
            labels = {
                kube_config.node_label_job: "true",
                kube_config.node_label_node_pool: pool_type.name,
            }
            if pool_type.is_preemptible:
                labels[kube_config.node_label_preemptible] = "true"
            capacity = {
                "pods": "110",
                "cpu": int(pool_type.available_cpu or 0),
                "memory": f"{pool_type.available_memory_mb}Mi",
                "nvidia.com/gpu": pool_type.gpu or 0,
            }
            taints = [
                NodeTaint(key=kube_config.jobs_pod_job_toleration_key, value="true")
            ]
            if pool_type.gpu:
                taints.append(NodeTaint(key=Resources.gpu_key, value="present"))
            try:
                await kube_client.create_node(
                    pool_type.name, capacity=capacity, labels=labels, taints=taints
                )
            except AlreadyExistsException:
                # there can be multiple kube_orchestrator created in tests (for tests
                # and for tests cleanup)
                pass

    return _create


@pytest.fixture(scope="session")
async def kube_ingress_ip(kube_config_cluster_payload: Dict[str, Any]) -> str:
    cluster = kube_config_cluster_payload
    return urlsplit(cluster["server"]).hostname


class MyKubeClient(KubeClient):
    _created_pvcs: List[str]

    async def init(self) -> None:
        await super().init()
        if not hasattr(self, "_created_pvcs"):
            self._created_pvcs = []

    async def close(self) -> None:
        for pvc_name in self._created_pvcs:
            await self.delete_pvc(pvc_name)
        await super().close()

    async def create_pvc(
        self,
        pvc_name: str,
        namespace: str,
        storage: Optional[int] = None,
        labels: Mapping[str, str] = None,
    ) -> None:
        url = self._generate_all_pvcs_url(namespace)
        storage = storage or 1024 * 1024
        primitive = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": pvc_name, "labels": labels},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "volumeMode": "Filesystem",
                "resources": {"requests": {"storage": storage}},
                # From `tests/k8s/storageclass.yml`:
                "storageClassName": "test-storage-class",
            },
        }
        payload = await self._request(method="POST", url=url, json=primitive)
        self._check_status_payload(payload)
        self._created_pvcs.append(pvc_name)

    async def delete_pvc(
        self,
        pvc_name: str,
    ) -> None:
        url = self._generate_pvc_url(pvc_name)
        payload = await self._request(method="DELETE", url=url)
        self._check_status_payload(payload)

    async def update_or_create_secret(
        self, secret_name: str, namespace: str, data: Optional[Dict[str, str]] = None
    ) -> None:
        url = self._generate_all_secrets_url(namespace)
        data = data or {}
        primitive = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": secret_name},
            "data": data,
            "type": "Opaque",
        }
        payload = await self._request(method="POST", url=url, json=primitive)
        self._check_status_payload(payload)

    async def wait_pod_scheduled(
        self,
        pod_name: str,
        node_name: str = "",
        timeout_s: float = 5.0,
        interval_s: float = 1.0,
    ) -> None:
        raw_pod: Optional[Dict[str, Any]] = None
        try:
            async with timeout(timeout_s):
                while True:
                    raw_pod = await self.get_raw_pod(pod_name)
                    if node_name:
                        pod_has_node = raw_pod["spec"].get("nodeName") == node_name
                    else:
                        pod_has_node = bool(raw_pod["spec"].get("nodeName"))
                    pod_is_scheduled = "PodScheduled" in [
                        cond["type"]
                        for cond in raw_pod["status"].get("conditions", [])
                        if cond["status"] == "True"
                    ]
                    if pod_has_node and pod_is_scheduled:
                        return
                    await asyncio.sleep(interval_s)
        except asyncio.TimeoutError:
            if raw_pod:
                print("Node:", raw_pod["spec"].get("nodeName"))
                print("Phase:", raw_pod["status"]["phase"])
                print("Status conditions:", raw_pod["status"].get("conditions", []))
                print("Pods:")
                pods = await self.get_raw_pods()
                pods = sorted(pods, key=lambda p: p["spec"].get("nodeName", ""))
                print(f"  {'Name':40s} {'CPU':5s} {'Memory':10s} {'Phase':9s} Node")
                for pod in pods:
                    container = pod["spec"]["containers"][0]
                    resource_requests = container.get("resources", {}).get(
                        "requests", {}
                    )
                    cpu = resource_requests.get("cpu")
                    memory = resource_requests.get("memory")
                    print(
                        f'  {pod["metadata"]["name"]:40s}',
                        f"{str(cpu):5s}",
                        f"{str(memory):10s}",
                        f'{pod["status"]["phase"]:9s}',
                        f'{str(pod["spec"].get("nodeName"))}',
                    )
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
                "name": f"{pod_id}.{uuid.uuid4()}",
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

    async def create_failed_attach_volume_event(self, pod_id: str) -> None:
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
            "message": "FailedAttachVolume",
            "metadata": {
                "creationTimestamp": now_str,
                "name": f"{pod_id}.{uuid.uuid4()}",
                "namespace": self._namespace,
                "selfLink": (
                    f"/api/v1/namespaces/{self._namespace}"
                    f"/events/{pod_id}.15a870d7e2bb228b"
                ),
                "uid": "cb886f64-8f96-11e9-9251-42010a800038",
            },
            "reason": "FailedAttachVolume",
            "reportingComponent": "",
            "reportingInstance": "",
            "source": {"component": "attachdetach-controller"},
            "type": "Warning",
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
    orchestrator_config: OrchestratorConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> Callable[..., KubeOrchestrator]:
    def _f(**kwargs: Any) -> KubeOrchestrator:
        defaults = dict(
            storage_config=storage_config_host,
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
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
    orchestrator_config: OrchestratorConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_nfs,
        registry_config=registry_config,
        orchestrator_config=orchestrator_config,
        kube_config=kube_config,
    )
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def kube_orchestrator_pvc(
    storage_config_pvc: StorageConfig,
    registry_config: RegistryConfig,
    orchestrator_config: OrchestratorConfig,
    kube_config: KubeConfig,
    event_loop: Any,
) -> AsyncIterator[KubeOrchestrator]:
    orchestrator = KubeOrchestrator(
        storage_config=storage_config_pvc,
        registry_config=registry_config,
        orchestrator_config=orchestrator_config,
        kube_config=kube_config,
    )
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def delete_node_later(
    kube_client: MyKubeClient,
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
def kube_config_node_preemptible(
    kube_config_factory: Callable[..., KubeConfig]
) -> KubeConfig:
    return kube_config_factory(
        node_label_preemptible="preemptible",
        jobs_pod_preemptible_toleration_key="preemptible-taint",
    )


@pytest.fixture
async def kube_node_preemptible(
    kube_config_node_preemptible: KubeConfig,
    kube_client: MyKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
    default_node_capacity: Dict[str, Any],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    kube_config = kube_config_node_preemptible
    assert kube_config.node_label_preemptible is not None
    assert kube_config.jobs_pod_preemptible_toleration_key is not None
    labels = {kube_config.node_label_preemptible: "true"}
    taints = [
        NodeTaint(key=kube_config.jobs_pod_preemptible_toleration_key, value="present")
    ]
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
    postgres_config: PostgresConfig,
    auth_config: AuthConfig,
    jobs_config: JobsConfig,
    notifications_config: NotificationsConfig,
    token_factory: Callable[[str], str],
) -> Callable[..., Config]:
    def _factory(**kwargs: Any) -> Config:
        server_config = ServerConfig()
        job_policy_enforcer = JobPolicyEnforcerConfig(
            platform_api_url=URL("http://localhost:8080/api/v1"),
            token=token_factory("compute"),
            interval_sec=1,
            quota_notification_threshold=0.1,
        )
        database_config = DatabaseConfig(postgres=postgres_config)
        config_url = URL("http://localhost:8082/api/v1")
        admin_url = URL("http://localhost:8080/apis/admin/v1")
        api_base_url = URL("http://localhost:8080/api/v1")
        return Config(
            server=server_config,
            database=database_config,
            auth=auth_config,
            jobs=jobs_config,
            job_policy_enforcer=job_policy_enforcer,
            notifications=notifications_config,
            cors=CORSConfig(allowed_origins=["https://neu.ro"]),
            config_url=config_url,
            admin_url=admin_url,
            api_base_url=api_base_url,
            zipkin=ZipkinConfig(URL("https://zipkin:9411"), 1.0),
            **kwargs,
        )

    return _factory


@pytest.fixture
def cluster_config_factory(
    orchestrator_config: OrchestratorConfig,
) -> Callable[..., ClusterConfig]:
    def _f(cluster_name: str = "test-cluster") -> ClusterConfig:
        ingress_config = IngressConfig(
            registry_url=URL("https://registry.dev.neuromation.io"),
            storage_url=URL("https://neu.ro/api/v1/storage"),
            blob_storage_url=URL("https://neu.ro/api/v1/blob"),
            monitoring_url=URL("https://neu.ro/api/v1/monitoring"),
            secrets_url=URL("https://neu.ro/api/v1/secrets"),
            metrics_url=URL("https://neu.ro/api/v1/metrics"),
            disks_url=URL("https://neu.ro/api/v1/disk"),
        )
        return ClusterConfig(
            name=cluster_name, orchestrator=orchestrator_config, ingress=ingress_config
        )

    return _f


@pytest.fixture
def cluster_config(
    cluster_config_factory: Callable[..., ClusterConfig]
) -> ClusterConfig:
    return cluster_config_factory()


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
