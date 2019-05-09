import asyncio
from datetime import timedelta
from typing import Callable, Iterator, List, Optional

import pytest

from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import JobsConfig
from platform_api.orchestrator import (
    Job,
    JobError,
    JobNotFoundException,
    JobRequest,
    JobsService,
    JobStatus,
    KubeConfig,
    LogReader,
    Orchestrator,
    Telemetry,
)
from platform_api.orchestrator.job import AggregatedRunTime, JobRecord, JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.jobs_storage import (
    InMemoryJobsStorage,
    JobStorageTransactionError,
)
from platform_api.resource import ResourcePoolType


class MockOrchestrator(Orchestrator):
    def __init__(self, config: KubeConfig) -> None:
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING
        self._mock_reason_to_return = "Initializing"
        self.raise_on_get_job_status = False
        self.raise_on_delete = False
        self._successfully_deleted_jobs: List[Job] = []

    @property
    def config(self) -> OrchestratorConfig:
        return self._config

    async def start_job(self, job: Job, token: str) -> JobStatus:
        job.status = JobStatus.PENDING
        return JobStatus.PENDING

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if self.raise_on_get_job_status:
            raise JobNotFoundException(f"job {job.id} was not found")
        return JobStatusItem.create(
            self._mock_status_to_return, reason=self._mock_reason_to_return
        )

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise JobError()
        self._successfully_deleted_jobs.append(job)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus) -> None:
        self._mock_status_to_return = new_status

    def update_reason_to_return(self, new_reason: str) -> None:
        self._mock_reason_to_return = new_reason

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


@pytest.fixture
def job_request_factory() -> Callable[[], JobRequest]:
    def factory() -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    return factory


@pytest.fixture(scope="function")
def mock_job_request(job_request_factory: Callable[[], JobRequest]) -> JobRequest:
    return job_request_factory()


@pytest.fixture(scope="function")
def mock_orchestrator() -> MockOrchestrator:
    config = KubeConfig(
        jobs_ingress_name="platformjobsingress",
        jobs_domain_name_template="{job_id}.jobs",
        named_jobs_domain_name_template="{job_name}-{job_owner}.jobs",
        ssh_domain_name="ssh",
        ssh_auth_domain_name="ssh-auth",
        endpoint_url="http://k8s:1234",
        resource_pool_types=[ResourcePoolType()],
    )
    return MockOrchestrator(config=config)


@pytest.fixture
def mock_jobs_storage() -> MockJobsStorage:
    return MockJobsStorage()


@pytest.fixture
def jobs_config() -> JobsConfig:
    return JobsConfig(orphaned_job_owner="compute")


@pytest.fixture
def jobs_service(
    mock_orchestrator: MockOrchestrator,
    mock_jobs_storage: MockJobsStorage,
    jobs_config: JobsConfig,
) -> JobsService:
    return JobsService(
        orchestrator=mock_orchestrator,
        jobs_storage=mock_jobs_storage,
        jobs_config=jobs_config,
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
