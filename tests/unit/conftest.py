import asyncio
from pathlib import PurePath

import pytest

from platform_api.orchestrator.jobs_storage import InMemoryJobsStorage, JobStorageTransactionError
from platform_api.config import RegistryConfig, StorageConfig, OrchestratorConfig
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
from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.resource import ResourcePoolType


class MockOrchestrator(Orchestrator):
    def __init__(self, config):
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING

        self.raise_on_get_job_status = False
        self.raise_on_delete = False
        self._successfully_deleted_jobs = []

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config

    async def start_job(self, job: Job, token: str):
        job.status = JobStatus.PENDING
        return JobStatus.PENDING

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if self.raise_on_get_job_status:
            raise JobNotFoundException(f"job {job.id} was not found")
        return JobStatusItem.create(self._mock_status_to_return)

    async def delete_job(self, job: Job) -> JobStatus:
        if self.raise_on_delete:
            raise JobError()
        self._successfully_deleted_jobs.append(job)
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus):
        self._mock_status_to_return = new_status

    def get_successfully_deleted_jobs(self):
        return self._successfully_deleted_jobs

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass

    async def get_job_telemetry(self, job: Job) -> Telemetry:
        pass


@pytest.fixture
def job_request_factory():
    def factory():
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    return factory


@pytest.fixture(scope="function")
def mock_job_request(job_request_factory):
    return job_request_factory()


@pytest.fixture(scope="function")
def mock_orchestrator():
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))
    registry_config = RegistryConfig()
    config = KubeConfig(
        storage=storage_config,
        registry=registry_config,
        jobs_ingress_name="platformjobsingress",
        jobs_domain_name_template="{job_id}.jobs",
        ssh_domain_name="ssh",
        ssh_auth_domain_name="ssh-auth",
        endpoint_url="http://k8s:1234",
        resource_pool_types=[ResourcePoolType()],
        orphaned_job_owner="compute",
    )
    return MockOrchestrator(config=config)


@pytest.fixture(scope="function")
def jobs_service(mock_orchestrator):
    return JobsService(orchestrator=mock_orchestrator)


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


class MockJobsStorage(InMemoryJobsStorage):
    # TODO (ajuszkowski, 7-mar-2019) get rid of InMemoryJobsStorage, keep only
    # this mocked jobs storage
    def __init__(self, orchestrator_config: OrchestratorConfig) -> None:
        super().__init__(orchestrator_config)
        self.fail_set_job_transaction = False

    async def set_job(self, job: Job) -> None:
        if self.fail_set_job_transaction:
            raise JobStorageTransactionError("transaction failed")
        await super().set_job(job)
