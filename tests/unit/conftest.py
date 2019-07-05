import asyncio
from datetime import timedelta
from pathlib import Path, PurePath
from typing import AsyncIterator, Callable, Iterator, List, Optional

import pytest
from notifications_client import Client as NotificationsClient
from notifications_client.notification import AbstractNotification
from yarl import URL

from platform_api.cluster import Cluster, ClusterConfig, ClusterRegistry
from platform_api.cluster_config import (
    ElasticsearchConfig,
    IngressConfig,
    LoggingConfig,
    OrchestratorConfig,
)
from platform_api.config import JobsConfig, RegistryConfig, StorageConfig
from platform_api.orchestrator.base import LogReader, Orchestrator, Telemetry
from platform_api.orchestrator.job import (
    AggregatedRunTime,
    Job,
    JobRecord,
    JobStatusItem,
    JobStatusReason,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import (
    InMemoryJobsStorage,
    JobStorageTransactionError,
)
from platform_api.orchestrator.kube_orchestrator import KubeConfig
from platform_api.resource import ResourcePoolType


CA_DATA_PEM = "this-is-certificate-authority-public-key"


class MockOrchestrator(Orchestrator):
    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING
        self._mock_reason_to_return: Optional[JobStatusReason]
        self._mock_reason_to_return = JobStatusReason.CONTAINER_CREATING
        self._mock_exit_code_to_return: Optional[int] = None
        self.raise_on_get_job_status = False
        self.raise_on_delete = False
        self._successfully_deleted_jobs: List[Job] = []

    @property
    def config(self) -> OrchestratorConfig:
        return self._config.orchestrator

    @property
    def storage_config(self) -> StorageConfig:
        return self._config.storage

    async def start_job(self, job: Job, token: str) -> JobStatus:
        job.status = JobStatus.PENDING
        return JobStatus.PENDING

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if self.raise_on_get_job_status:
            raise JobNotFoundException(f"job {job.id} was not found")
        return JobStatusItem.create(
            self._mock_status_to_return,
            reason=self._mock_reason_to_return,
            exit_code=self._mock_exit_code_to_return,
        )

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise JobError()
        self._successfully_deleted_jobs.append(job)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus) -> None:
        self._mock_status_to_return = new_status

    def update_reason_to_return(self, new_reason: Optional[JobStatusReason]) -> None:
        self._mock_reason_to_return = new_reason

    def update_exit_code_to_return(self, new_exit_code: Optional[int]) -> None:
        self._mock_exit_code_to_return = new_exit_code

    def get_successfully_deleted_jobs(self) -> List[Job]:
        return self._successfully_deleted_jobs

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass

    async def get_job_telemetry(self, job: Job) -> Telemetry:
        pass


class MockJobsStorage(InMemoryJobsStorage):
    def __init__(self) -> None:
        super().__init__()
        self.fail_set_job_transaction = False

    async def set_job(self, job: JobRecord) -> None:
        if self.fail_set_job_transaction:
            raise JobStorageTransactionError("transaction failed")
        await super().set_job(job)


class MockNotificationsClient(NotificationsClient):
    def __init__(self) -> None:
        self._sent_notifications: List[AbstractNotification] = []
        pass

    async def notify(self, notification: AbstractNotification) -> None:
        self._sent_notifications.append(notification)

    def init(self) -> None:
        pass

    def close(self) -> None:
        pass

    @property
    def sent_notifications(self) -> List[AbstractNotification]:
        return self._sent_notifications


@pytest.fixture
def job_request_factory() -> Callable[[], JobRequest]:
    def factory() -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    return factory


@pytest.fixture()
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture
def mock_job_request(job_request_factory: Callable[[], JobRequest]) -> JobRequest:
    return job_request_factory()


class MockCluster(Cluster):
    def __init__(self, config: ClusterConfig, orchestrator: Orchestrator) -> None:
        self._config = config
        self._orchestrator = orchestrator

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def config(self) -> ClusterConfig:
        return self._config

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orchestrator


@pytest.fixture
def cluster_config() -> ClusterConfig:
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
    registry_config = RegistryConfig()
    orchestrator_config = KubeConfig(
        jobs_ingress_name="platformjobsingress",
        jobs_domain_name_template="{job_id}.jobs",
        ssh_auth_domain_name="ssh-auth",
        endpoint_url="http://k8s:1234",
        resource_pool_types=[ResourcePoolType()],
    )
    return ClusterConfig(
        name="default",
        storage=storage_config,
        registry=registry_config,
        orchestrator=orchestrator_config,
        logging=LoggingConfig(elasticsearch=ElasticsearchConfig(hosts=[])),
        ingress=IngressConfig(storage_url=URL(), users_url=URL(), monitoring_url=URL()),
    )


@pytest.fixture
def mock_orchestrator(cluster_config: ClusterConfig) -> MockOrchestrator:
    return MockOrchestrator(config=cluster_config)


@pytest.fixture
async def cluster_registry(
    cluster_config: ClusterConfig, mock_orchestrator: MockOrchestrator
) -> AsyncIterator[ClusterRegistry]:
    def _cluster_factory(config: ClusterConfig) -> Cluster:
        return MockCluster(config, mock_orchestrator)

    async with ClusterRegistry(factory=_cluster_factory) as registry:
        await registry.add(cluster_config)
        yield registry


@pytest.fixture
def mock_jobs_storage() -> MockJobsStorage:
    return MockJobsStorage()


@pytest.fixture
def mock_notifications_client() -> NotificationsClient:
    return MockNotificationsClient()


@pytest.fixture
def jobs_config() -> JobsConfig:
    return JobsConfig(orphaned_job_owner="compute")


@pytest.fixture
def jobs_service(
    cluster_registry: ClusterRegistry,
    mock_jobs_storage: MockJobsStorage,
    jobs_config: JobsConfig,
    mock_notifications_client: NotificationsClient,
) -> JobsService:
    return JobsService(
        cluster_registry=cluster_registry,
        jobs_storage=mock_jobs_storage,
        jobs_config=jobs_config,
        notifications_client=mock_notifications_client,
    )


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


def create_quota(
    time_gpu_minutes: Optional[int] = None, time_non_gpu_minutes: Optional[int] = None
) -> AggregatedRunTime:

    if time_gpu_minutes is not None:
        gpu_delta = timedelta(minutes=time_gpu_minutes)
    else:
        gpu_delta = timedelta.max

    if time_non_gpu_minutes is not None:
        non_gpu_delta = timedelta(minutes=time_non_gpu_minutes)
    else:
        non_gpu_delta = timedelta.max

    return AggregatedRunTime(
        total_gpu_run_time_delta=gpu_delta, total_non_gpu_run_time_delta=non_gpu_delta
    )
