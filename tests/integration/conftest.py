import asyncio
import base64
import json
import uuid
from asyncio import timeout
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import aiohttp.web
import neuro_config_client
import pytest
from apolo_events_client import EventsClientConfig
from apolo_kube_client import (
    KubeClientAuthType as ApoloKubeClientAuthType,
    KubeClientSelector,
    KubeConfig as ApoloKubeConfig,
)
from neuro_config_client import (
    AMDGPU,
    ACMEEnvironment,
    AMDGPUPreset,
    AppsConfig,
    BucketsConfig,
    Cluster,
    DisksConfig,
    DNSConfig,
    EnergyConfig,
    EnergySchedule,
    IngressConfig,
    IntelGPU,
    MetricsConfig,
    MonitoringConfig,
    NvidiaGPU,
    NvidiaGPUPreset,
    OrchestratorConfig,
    ResourcePoolType,
    ResourcePreset,
    SecretsConfig,
    StorageConfig as ClusterStorageConfig,
    TPUPreset,
    TPUResource,
    VolumeConfig,
)
from yarl import URL

from platform_api.config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    JobPolicyEnforcerConfig,
    JobsConfig,
    NotificationsConfig,
    OAuthConfig,
    PostgresConfig,
    RegistryConfig,
    ServerConfig,
)
from platform_api.old_kube_client.errors import ResourceExists
from platform_api.orchestrator.kube_client import (
    KubeClient,
    NodeTaint,
    PodDescriptor,
    Resources,
)
from platform_api.orchestrator.kube_config import KubeClientAuthType, KubeConfig
from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator


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
async def kube_config_payload() -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        "kubectl",
        "config",
        "view",
        "--raw",
        "-o",
        "json",
        stdout=asyncio.subprocess.PIPE,
    )
    output, _ = await process.communicate()
    payload_str = output.decode().rstrip()
    return json.loads(payload_str)


@pytest.fixture(scope="session")
async def kube_config_cluster_payload(kube_config_payload: dict[str, Any]) -> Any:
    cluster_name = "minikube"
    clusters = {
        cluster["name"]: cluster["cluster"]
        for cluster in kube_config_payload["clusters"]
    }
    return clusters[cluster_name]


@pytest.fixture(scope="session")
def cert_authority_data_pem(
    kube_config_cluster_payload: dict[str, Any],
) -> str | None:
    if "certificate-authority" in kube_config_cluster_payload:
        ca_path = kube_config_cluster_payload["certificate-authority"]
        if ca_path:
            return Path(ca_path).read_text()
    elif "certificate-authority-data" in kube_config_cluster_payload:
        return base64.b64decode(
            kube_config_cluster_payload["certificate-authority-data"]
        ).decode()
    return None


@pytest.fixture(scope="session")
async def kube_config_user_payload(kube_config_payload: dict[str, Any]) -> Any:
    import tempfile

    user_name = "minikube"
    users = {user["name"]: user["user"] for user in kube_config_payload["users"]}
    user = users[user_name]
    if "client-certificate-data" in user:
        with tempfile.NamedTemporaryFile(delete=False) as cert_file:
            cert_file.write(base64.b64decode(user["client-certificate-data"]))
            cert_file.flush()
            user["client-certificate"] = cert_file.name
    if "client-key-data" in user:
        with tempfile.NamedTemporaryFile(delete=False) as key_file:
            key_file.write(base64.b64decode(user["client-key-data"]))
            key_file.flush()
            user["client-key"] = key_file.name
    return user


@pytest.fixture(scope="session")
def registry_config(token_factory: Callable[[str], str]) -> RegistryConfig:
    return RegistryConfig(username="compute", password=token_factory("compute"))


@pytest.fixture(scope="session")
def orchestrator_config_factory() -> Iterator[Callable[..., OrchestratorConfig]]:
    def _f(**kwargs: Any) -> OrchestratorConfig:
        defaults = {
            "job_hostname_template": "{job_id}.jobs.neu.ro",
            "resource_pool_types": [
                ResourcePoolType(
                    name="cpu",
                    min_size=1,
                    max_size=2,
                    idle_size=1,
                    cpu=1.0,
                    available_cpu=1.0,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                    disk_size=150 * 10**9,
                    available_disk_size=150 * 10**9,
                    cpu_min_watts=1,
                    cpu_max_watts=2,
                ),
                ResourcePoolType(
                    name="cpu-p",
                    min_size=1,
                    max_size=2,
                    cpu=1.0,
                    available_cpu=1.0,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                    disk_size=150 * 10**9,
                    available_disk_size=150 * 10**9,
                    is_preemptible=True,
                ),
                ResourcePoolType(
                    name="cpu-large",
                    min_size=1,
                    max_size=2,
                    cpu=100,
                    available_cpu=100,
                    memory=500_000 * 10**6,
                    available_memory=500_000 * 10**6,
                    disk_size=150 * 10**9,
                    available_disk_size=150 * 10**9,
                ),
                ResourcePoolType(
                    name="tpu",
                    min_size=1,
                    max_size=2,
                    cpu=1.0,
                    available_cpu=1.0,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                    disk_size=150 * 10**9,
                    available_disk_size=150 * 10**9,
                    tpu=TPUResource(
                        ipv4_cidr_block="1.1.1.1/32",
                        types=("v2-8",),
                        software_versions=("1.14",),
                    ),
                ),
                ResourcePoolType(
                    name="gpu",
                    min_size=1,
                    max_size=2,
                    cpu=1.0,
                    available_cpu=1.0,
                    memory=2048 * 10**6,
                    available_memory=2048 * 10**6,
                    disk_size=150 * 10**9,
                    available_disk_size=150 * 10**9,
                    nvidia_gpu=NvidiaGPU(
                        count=1, model="nvidia-gpu", memory=40 * 2**30
                    ),
                    nvidia_migs={
                        "1g.5gb": NvidiaGPU(
                            count=7, model="nvidia-mig", memory=5 * 2**30
                        )
                    },
                    amd_gpu=AMDGPU(count=2, model="amd-gpu"),
                    intel_gpu=IntelGPU(count=3, model="intel-gpu"),
                ),
            ],
            "resource_presets": [
                ResourcePreset(
                    name="gpu-small",
                    credits_per_hour=Decimal("10"),
                    cpu=7,
                    memory=30720 * 10**6,
                    nvidia_gpu=NvidiaGPUPreset(
                        count=1, model="nvidia-tesla-k80", memory=40 * 2**30
                    ),
                    available_resource_pool_names=["gpu"],
                ),
                ResourcePreset(
                    name="nvidia-mig-small",
                    credits_per_hour=Decimal("10"),
                    cpu=7,
                    memory=30720 * 10**6,
                    nvidia_migs={
                        "1g.5gb": NvidiaGPUPreset(
                            count=1, model="nvidia-mig", memory=5 * 2**30
                        )
                    },
                    available_resource_pool_names=["gpu"],
                ),
                ResourcePreset(
                    name="amd-gpu-small",
                    credits_per_hour=Decimal("10"),
                    cpu=7,
                    memory=30720 * 10**6,
                    amd_gpu=AMDGPUPreset(count=1),
                    available_resource_pool_names=["gpu"],
                ),
                ResourcePreset(
                    name="gpu-large",
                    credits_per_hour=Decimal("10"),
                    cpu=7,
                    memory=61440 * 10**6,
                    nvidia_gpu=NvidiaGPUPreset(count=1, model="nvidia-tesla-v100"),
                    available_resource_pool_names=["gpu"],
                ),
                ResourcePreset(
                    name="gpu-large-p",
                    credits_per_hour=Decimal("10"),
                    cpu=7,
                    memory=61440 * 10**6,
                    nvidia_gpu=NvidiaGPUPreset(count=1, model="nvidia-tesla-v100"),
                    scheduler_enabled=True,
                    preemptible_node=True,
                    available_resource_pool_names=["gpu"],
                ),
                ResourcePreset(
                    name="cpu-micro",
                    credits_per_hour=Decimal("10"),
                    cpu=0.1,
                    memory=100 * 10**6,
                    available_resource_pool_names=["cpu"],
                ),
                ResourcePreset(
                    name="cpu-small",
                    credits_per_hour=Decimal("10"),
                    cpu=2,
                    memory=2048 * 10**6,
                    available_resource_pool_names=["cpu"],
                ),
                ResourcePreset(
                    name="cpu-large",
                    credits_per_hour=Decimal("10"),
                    cpu=3,
                    memory=14336 * 10**6,
                    available_resource_pool_names=["cpu"],
                ),
                ResourcePreset(
                    name="tpu",
                    credits_per_hour=Decimal("10"),
                    cpu=3,
                    memory=14336 * 10**6,
                    tpu=TPUPreset(type="v2-8", software_version="1.14"),
                    available_resource_pool_names=["tpu"],
                ),
            ],
            "job_fallback_hostname": "default.jobs.apolo.us",
            "is_http_ingress_secure": False,
            "job_schedule_timeout_s": 30,
            "job_schedule_scale_up_timeout_s": 5,
            "allow_privileged_mode": True,
            "allow_job_priority": True,
        }
        kwargs = {**defaults, **kwargs}
        return OrchestratorConfig(**kwargs)

    yield _f


@pytest.fixture(scope="session")
async def orchestrator_config(
    orchestrator_config_factory: Callable[..., OrchestratorConfig],
) -> OrchestratorConfig:
    return orchestrator_config_factory()


@pytest.fixture(scope="session")
def kube_config_factory(
    kube_config_cluster_payload: dict[str, Any],
    kube_config_user_payload: dict[str, Any],
    cert_authority_data_pem: str | None,
) -> Iterator[Callable[..., KubeConfig]]:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload

    def _f(**kwargs: Any) -> KubeConfig:
        defaults = {
            "jobs_ingress_class": "nginx",
            "endpoint_url": cluster["server"],
            "auth_type": KubeClientAuthType.CERTIFICATE,
            "cert_authority_data_pem": cert_authority_data_pem,
            # disable, only `cert_authority_data_pem` works
            "cert_authority_path": None,
            "auth_cert_path": user["client-certificate"],
            "auth_cert_key_path": user["client-key"],
            "namespace": "platformapi-tests",
        }
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
                "cpu": int(pool_type.cpu or 0),
                "memory": f"{pool_type.memory}",
                "nvidia.com/gpu": (
                    pool_type.nvidia_gpu.count if pool_type.nvidia_gpu else 0
                ),
            }
            taints = [
                NodeTaint(key=kube_config.jobs_pod_job_toleration_key, value="true")
            ]
            if pool_type.nvidia_gpu:
                taints.append(NodeTaint(key=Resources.nvidia_gpu_key, value="present"))
            if pool_type.amd_gpu:
                taints.append(NodeTaint(key=Resources.amd_gpu_key, value="present"))
            if pool_type.intel_gpu:
                taints.append(NodeTaint(key=Resources.intel_gpu_key, value="present"))
            try:
                await kube_client.create_node(
                    pool_type.name, capacity=capacity, labels=labels, taints=taints
                )
            except ResourceExists:
                # there can be multiple kube_orchestrator created in tests (for tests
                # and for tests cleanup)
                pass

    return _create


@pytest.fixture(scope="session")
async def kube_ingress_ip(kube_config_cluster_payload: dict[str, Any]) -> str:
    cluster = kube_config_cluster_payload
    return urlsplit(cluster["server"]).hostname


class MyKubeClient(KubeClient):
    """
    Extended kube client that has methods used for tests only
    """

    async def wait_pod_scheduled(
        self,
        namespace: str,
        pod_name: str,
        node_name: str = "",
        timeout_s: float = 5.0,
        interval_s: float = 1.0,
    ) -> None:
        raw_pod: dict[str, Any] | None = None
        try:
            async with timeout(timeout_s):
                while True:
                    raw_pod = await self.get_raw_pod(namespace, pod_name)
                    if node_name:
                        pod_at_node = raw_pod["spec"].get("nodeName")
                        if pod_at_node == node_name:
                            pod_has_node = True
                        else:
                            pod_has_node = False
                            print(
                                f"Pod was scheudled to wrong node: {pod_at_node}, "
                                f"expected: {node_name}"
                            )
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
        except TimeoutError:
            if raw_pod:
                print("Node:", raw_pod["spec"].get("nodeName"))
                print("Phase:", raw_pod["status"]["phase"])
                print("Status conditions:", raw_pod["status"].get("conditions", []))
                print("Pods:")
                pod_list = await self.get_raw_pods()
                pods = sorted(
                    pod_list.items, key=lambda p: p["spec"].get("nodeName", "")
                )
                print(f"  {'Name':40s} {'CPU':5s} {'Memory':10s} {'Phase':9s} Node")
                for pod in pods:
                    container = pod["spec"]["containers"][0]
                    resource_requests = container.get("resources", {}).get(
                        "requests", {}
                    )
                    cpu = resource_requests.get("cpu")
                    memory = resource_requests.get("memory")
                    print(
                        f"  {pod['metadata']['name']:40s}",
                        f"{str(cpu):5s}",
                        f"{str(memory):10s}",
                        f"{pod['status']['phase']:9s}",
                        f"{str(pod['spec'].get('nodeName'))}",
                    )
            pytest.fail("Pod unscheduled")

    async def add_node_labels(self, node_name: str, labels: dict[str, Any]) -> None:
        node = await self.get_node(node_name)

        new_labels = node.labels.copy()
        new_labels.update(labels)

        await self.patch(
            url=self._generate_node_url(node_name),
            headers={"content-type": "application/merge-patch+json"},
            json={"metadata": {"labels": new_labels}},
        )

    async def remove_node_labels(self, node_name: str, label_keys: list[str]) -> None:
        node = await self.get_node(node_name)

        new_labels = {
            label: value
            for label, value in node.labels.items()
            if label not in label_keys
        }

        await self.patch(
            url=self._generate_node_url(node_name),
            headers={"content-type": "application/merge-patch+json"},
            json={"metadata": {"labels": new_labels}},
        )


@pytest.fixture(scope="session")
async def kube_client_factory(kube_config: KubeConfig) -> Callable[..., MyKubeClient]:
    def _f(custom_kube_config: KubeConfig | None = None) -> MyKubeClient:
        config = custom_kube_config or kube_config
        return MyKubeClient(
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

    return _f


@pytest.fixture(scope="session")
async def kube_client(
    kube_client_factory: Callable[..., MyKubeClient],
) -> AsyncIterator[KubeClient]:
    async with kube_client_factory() as kube_client:
        yield kube_client


@pytest.fixture(scope="session")
async def kube_client_selector(
    kube_config_cluster_payload: dict[str, Any],
    kube_config_user_payload: dict[str, Any],
    cert_authority_data_pem: str | None,
) -> AsyncIterator[KubeClientSelector]:
    cluster = kube_config_cluster_payload
    user = kube_config_user_payload
    config = ApoloKubeConfig(
        endpoint_url=cluster["server"],
        cert_authority_data_pem=cert_authority_data_pem,
        cert_authority_path=None,
        auth_type=ApoloKubeClientAuthType.CERTIFICATE,
        auth_cert_path=user["client-certificate"],
        auth_cert_key_path=user["client-key"],
    )
    async with KubeClientSelector(config=config) as selector:
        yield selector


@pytest.fixture
async def kube_orchestrator_factory(
    registry_config: RegistryConfig,
    orchestrator_config: OrchestratorConfig,
    kube_config: KubeConfig,
    kube_client_selector: KubeClientSelector,
) -> Callable[..., KubeOrchestrator]:
    def _f(**kwargs: Any) -> KubeOrchestrator:
        defaults = {
            "cluster_name": "default",
            "registry_config": registry_config,
            "orchestrator_config": orchestrator_config,
            "kube_config": kube_config,
            "kube_client_selector": kube_client_selector,
        }
        kwargs = {**defaults, **kwargs}
        return KubeOrchestrator(**kwargs)

    return _f


@pytest.fixture
async def kube_orchestrator(
    kube_orchestrator_factory: Callable[..., KubeOrchestrator],
) -> KubeOrchestrator:
    return kube_orchestrator_factory()


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
async def kube_node(kube_client: KubeClient) -> str:
    nodes = await kube_client.get_nodes()
    return nodes[0].name


@pytest.fixture
def default_node_capacity() -> dict[str, str]:
    return {"pods": "110", "memory": "1Gi", "cpu": "2", "nvidia.com/gpu": "1"}


@pytest.fixture
async def kube_node_gpu(
    kube_client: MyKubeClient,
    delete_node_later: Callable[[str], Awaitable[None]],
    default_node_capacity: dict[str, str],
) -> AsyncIterator[str]:
    node_name = str(uuid.uuid4())
    await delete_node_later(node_name)

    taints = [NodeTaint(key=Resources.nvidia_gpu_key, value="present")]
    await kube_client.create_node(
        node_name, capacity=default_node_capacity, taints=taints
    )

    yield node_name


@pytest.fixture
async def kube_node_tpu(
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
    kube_config_factory: Callable[..., KubeConfig],
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
    default_node_capacity: dict[str, str],
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
async def delete_pod_later(
    kube_client: KubeClient,
) -> AsyncIterator[Callable[[PodDescriptor], Awaitable[None]]]:
    pods = []

    async def _add_pod(pod: PodDescriptor) -> None:
        pods.append(pod)

    yield _add_pod

    for pod in pods:
        try:
            await kube_client.delete_pod(kube_client.namespace, pod.name)
        except Exception:
            pass


@pytest.fixture
async def pod_factory(
    kube_client: KubeClient,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> Callable[..., Awaitable[PodDescriptor]]:
    name_prefix = f"pod-{uuid.uuid4()}"
    name_index = 1

    async def _create(
        image: str,
        command: list[str] | None = None,
        cpu: float = 0.1,
        memory: int = 128 * 10**6,
        labels: dict[str, str] | None = None,
        wait: bool = True,
        wait_timeout_s: float = 60,
        idle: bool = False,
    ) -> PodDescriptor:
        nonlocal name_index
        name = f"{name_prefix}-{name_index}"
        name_index += 1
        labels = labels or {}
        if idle:
            labels["platform.neuromation.io/idle"] = "true"
        pod = PodDescriptor(
            name=name,
            labels=labels,
            image=image,
            command=command or [],
            resources=Resources(cpu=cpu, memory=memory),
        )
        pod = await kube_client.create_pod(kube_client.namespace, pod)
        await delete_pod_later(pod)
        if wait:
            await kube_client.wait_pod_is_running(
                kube_client.namespace, pod.name, timeout_s=wait_timeout_s
            )
        return pod

    return _create


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
    admin_url: URL,
    token_factory: Callable[[str], str],
    events_config: EventsClientConfig,
) -> Callable[..., Config]:
    def _factory(**kwargs: Any) -> Config:
        server_config = ServerConfig()
        job_policy_enforcer = JobPolicyEnforcerConfig(
            platform_api_url=URL("http://localhost:8080/api/v1"),
            interval_sec=1,
            credit_notification_threshold=Decimal("0.1"),
            retention_delay_days=20 / (24 * 60 * 60),  # 20 seconds
        )
        database_config = DatabaseConfig(postgres=postgres_config)
        config_url = URL("http://localhost:8082/api/v1")
        api_base_url = URL("http://localhost:8080/api/v1")
        return Config(
            server=server_config,
            database=database_config,
            auth=auth_config,
            jobs=jobs_config,
            job_policy_enforcer=job_policy_enforcer,
            notifications=notifications_config,
            events=events_config,
            config_url=config_url,
            admin_url=admin_url,
            admin_public_url=admin_url,
            api_base_url=api_base_url,
            **kwargs,
        )

    return _factory


@pytest.fixture
def cluster_config_factory(
    orchestrator_config: OrchestratorConfig,
) -> Callable[..., Cluster]:
    def _f(cluster_name: str = "test-cluster") -> Cluster:
        return Cluster(
            name=cluster_name,
            created_at=datetime.now(UTC),
            location="eu-west-4",
            logo_url=URL("https://logo.url"),
            orchestrator=orchestrator_config,
            ingress=IngressConfig(acme_environment=ACMEEnvironment.PRODUCTION),
            timezone=UTC,
            energy=EnergyConfig(
                schedules=[
                    EnergySchedule.create_default(timezone=UTC),
                    EnergySchedule.create_default(timezone=UTC, name="green"),
                ]
            ),
            storage=ClusterStorageConfig(
                url=URL("https://neu.ro/api/v1/storage"),
                volumes=[
                    VolumeConfig(
                        name="default",
                        path=None,
                        credits_per_hour_per_gb=Decimal("100"),
                    )
                ],
            ),
            registry=neuro_config_client.RegistryConfig(
                url=URL("https://registry.dev.neuromation.io")
            ),
            monitoring=MonitoringConfig(url=URL("https://neu.ro/api/v1/monitoring")),
            secrets=SecretsConfig(url=URL("https://neu.ro/api/v1/secrets")),
            metrics=MetricsConfig(url=URL("https://neu.ro/api/v1/metrics")),
            disks=DisksConfig(
                url=URL("https://neu.ro/api/v1/disk"),
                storage_limit_per_user=100 * 2**30,
            ),
            buckets=BucketsConfig(url=URL("https://neu.ro/api/v1/buckets")),
            apps=AppsConfig(
                apps_hostname_templates=["{app_name}.apps.dev.neu.ro"],
                app_proxy_url=URL("https://proxy.apps.dev.neu.ro"),
            ),
            dns=DNSConfig(name="neu.ro"),
        )

    return _f


@pytest.fixture
def cluster_config(
    cluster_config_factory: Callable[..., Cluster],
) -> Cluster:
    return cluster_config_factory()


@pytest.fixture
def config(config_factory: Callable[..., Config]) -> Config:
    return config_factory()


@pytest.fixture
def config_with_oauth(
    config_factory: Callable[..., Config], oauth_config_dev: OAuthConfig | None
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
    except Exception:
        pass
    finally:
        await runner.shutdown()
        await runner.cleanup()


class ApiRunner:
    def __init__(self, app: aiohttp.web.Application, port: int) -> None:
        self._app = app
        self._port = port

        self._api_address_future: asyncio.Future[ApiAddress] = asyncio.Future()
        self._cleanup_future: asyncio.Future[None] = asyncio.Future()
        self._task: asyncio.Task[None] | None = None

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
        return not self._task
