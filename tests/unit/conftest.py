import asyncio
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path, PurePath
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional

import pytest
from neuro_auth_client import AuthClient
from notifications_client import Client as NotificationsClient
from notifications_client.notification import AbstractNotification
from yarl import URL

from platform_api.cluster import Cluster, ClusterConfig, ClusterRegistry
from platform_api.cluster_config import (
    IngressConfig,
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from platform_api.config import JobsConfig, JobsSchedulerConfig
from platform_api.orchestrator.base import Orchestrator
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
from platform_api.orchestrator.jobs_service import JobsScheduler, JobsService
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
        self._mock_reason_to_return: Optional[str] = JobStatusReason.CONTAINER_CREATING
        self._mock_exit_code_to_return: Optional[int] = None
        self._mock_statuses: Dict[str, JobStatus] = {}
        self._mock_reasons: Dict[str, Optional[str]] = {}
        self._mock_exit_codes: Dict[str, Optional[int]] = {}
        self.raise_on_get_job_status = False
        self.raise_on_start_job_status = False
        self.get_job_status_exc_factory = self._create_get_job_status_exc
        self.raise_on_delete = False
        self.delete_job_exc_factory = self._create_delete_job_exc
        self.current_datetime_factory: Callable[[], datetime] = partial(
            datetime.now, timezone.utc
        )
        self._successfully_deleted_jobs: List[Job] = []

    @property
    def config(self) -> OrchestratorConfig:
        return self._config.orchestrator

    @property
    def storage_config(self) -> StorageConfig:
        return self._config.storage

    def _get_status(self, job_id: str) -> JobStatus:
        return self._mock_statuses.get(job_id, self._mock_status_to_return)

    def _get_exit_code(self, job_id: str) -> Optional[int]:
        return self._mock_exit_codes.get(job_id, self._mock_exit_code_to_return)

    def _get_reason(self, job_id: str) -> Optional[str]:
        return self._mock_reasons.get(job_id, self._mock_reason_to_return)

    async def prepare_job(self, job: Job) -> None:
        pass

    async def start_job(self, job: Job) -> JobStatus:
        if self.raise_on_start_job_status:
            raise self.get_job_status_exc_factory(job)
        job.status_history.current = JobStatusItem.create(
            self._get_status(job.id),
            reason=self._get_reason(job.id),
            exit_code=self._get_exit_code(job.id),
            current_datetime_factory=self.current_datetime_factory,
        )
        return job.status

    def _create_get_job_status_exc(self, job: Job) -> Exception:
        return JobNotFoundException(f"job {job.id} was not found")

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if self.raise_on_get_job_status:
            raise self.get_job_status_exc_factory(job)
        return JobStatusItem.create(
            self._get_status(job.id),
            reason=self._get_reason(job.id),
            exit_code=self._get_exit_code(job.id),
            current_datetime_factory=self.current_datetime_factory,
        )

    def _create_delete_job_exc(self, job: Job) -> Exception:
        return JobError()

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise self.delete_job_exc_factory(job)
        self._successfully_deleted_jobs.append(job)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus) -> None:
        self._mock_status_to_return = new_status

    def update_reason_to_return(self, new_reason: Optional[str]) -> None:
        self._mock_reason_to_return = new_reason

    def update_exit_code_to_return(self, new_exit_code: Optional[int]) -> None:
        self._mock_exit_code_to_return = new_exit_code

    def update_status_to_return_single(
        self, job_id: str, new_status: JobStatus
    ) -> None:
        self._mock_statuses[job_id] = new_status

    def update_reason_to_return_single(
        self, job_id: str, new_reason: Optional[str]
    ) -> None:
        self._mock_reasons[job_id] = new_reason

    def update_exit_code_to_return_single(
        self, job_id: str, new_exit_code: Optional[int]
    ) -> None:
        self._mock_exit_codes[job_id] = new_exit_code

    def get_successfully_deleted_jobs(self) -> List[Job]:
        return self._successfully_deleted_jobs


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

    async def init(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @property
    def sent_notifications(self) -> List[AbstractNotification]:
        return self._sent_notifications


class MockAuthClient(AuthClient):
    def __init__(self) -> None:
        pass

    async def get_user_token(self, name: str, token: Optional[str] = None) -> str:
        return f"token-{name}"


@pytest.fixture
def job_request_factory() -> Callable[[], JobRequest]:
    def factory(with_gpu: bool = False) -> JobRequest:
        cont_kwargs: Dict[str, Any] = {"cpu": 1, "memory_mb": 128}
        if with_gpu:
            cont_kwargs["gpu"] = 1
            cont_kwargs["gpu_model_id"] = "nvidia-tesla-k80"

        return JobRequest.create(
            Container(image="testimage", resources=ContainerResources(**cont_kwargs))
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
def registry_config() -> RegistryConfig:
    return RegistryConfig(username="compute", password="compute_token")


@pytest.fixture
def cluster_config(registry_config: RegistryConfig) -> ClusterConfig:
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
    orchestrator_config = KubeConfig(
        jobs_domain_name_template="{job_id}.jobs",
        ssh_auth_server="ssh-auth:22",
        endpoint_url="http://k8s:1234",
        resource_pool_types=[ResourcePoolType()],
    )
    return ClusterConfig(
        name="test-cluster",
        storage=storage_config,
        registry=registry_config,
        orchestrator=orchestrator_config,
        ingress=IngressConfig(
            storage_url=URL(),
            monitoring_url=URL(),
            secrets_url=URL(),
            metrics_url=URL(),
        ),
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
        await registry.replace(cluster_config)
        yield registry


@pytest.fixture
def mock_api_base() -> URL:
    return URL("https://testing.neu.ro/api/v1")


@pytest.fixture
def mock_jobs_storage() -> MockJobsStorage:
    return MockJobsStorage()


@pytest.fixture
def mock_notifications_client() -> NotificationsClient:
    return MockNotificationsClient()


@pytest.fixture
def mock_auth_client() -> AuthClient:
    return MockAuthClient()


@pytest.fixture
def jobs_config() -> JobsConfig:
    return JobsConfig(orphaned_job_owner="compute")


@pytest.fixture
def scheduler_config() -> JobsSchedulerConfig:
    return JobsSchedulerConfig()


@pytest.fixture
def jobs_service(
    cluster_registry: ClusterRegistry,
    mock_jobs_storage: MockJobsStorage,
    jobs_config: JobsConfig,
    mock_notifications_client: NotificationsClient,
    scheduler_config: JobsSchedulerConfig,
    mock_auth_client: AuthClient,
    mock_api_base: URL,
) -> JobsService:
    return JobsService(
        cluster_registry=cluster_registry,
        jobs_storage=mock_jobs_storage,
        jobs_config=jobs_config,
        notifications_client=mock_notifications_client,
        scheduler=JobsScheduler(scheduler_config),
        auth_client=mock_auth_client,
        api_base_url=mock_api_base,
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
