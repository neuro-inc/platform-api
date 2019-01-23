import asyncio
import json
from pathlib import PurePath
from typing import Dict, List

import pytest

from platform_api.config import OrchestratorConfig, RegistryConfig, StorageConfig
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
from platform_api.orchestrator.jobs_storage import JobsStorage
from platform_api.resource import ResourcePoolType


class MockOrchestrator(Orchestrator):
    def __init__(self, config: KubeConfig):
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING

        self.raise_on_get_job_status = False
        self.raise_on_delete = False

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

    async def delete_job(self, *args, **kwargs):
        if self.raise_on_delete:
            raise JobError()
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus):
        self._mock_status_to_return = new_status

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass

    async def get_job_telemetry(self, job: Job) -> Telemetry:
        pass


class MockRedisJobsStorage(JobsStorage):
    def __init__(self, orchestrator_config: OrchestratorConfig) -> None:
        self._orchestrator_config = orchestrator_config
        self._job_records: Dict[str, str] = {}

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator_config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = self._job_records.get(job_id)
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    async def get_all_jobs(self) -> List[Job]:
        jobs = []
        for payload in self._job_records.values():
            jobs.append(self._parse_job_payload(payload))
        return jobs

    async def get_running_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.is_running]

    async def get_jobs_for_deletion(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.should_be_deleted]

    async def get_unfinished_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if not job.is_finished]


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
        jobs_domain_name="jobs",
        ssh_domain_name="ssh",
        ssh_auth_domain_name="ssh-auth",
        endpoint_url="http://k8s:1234",
        resource_pool_types=[ResourcePoolType()],
        orphaned_job_owner="compute",
    )
    return MockOrchestrator(config=config)


@pytest.fixture(scope="function")
def mock_jobs_storage_factory():
    def impl(mock_orchestrator_config):
        return MockRedisJobsStorage(mock_orchestrator_config)

    return impl


@pytest.fixture(scope="function")
def mock_jobs_storage(mock_jobs_storage_factory, mock_orchestrator):
    return mock_jobs_storage_factory(mock_orchestrator.config)


@pytest.fixture(scope="function")
def jobs_service(mock_orchestrator, mock_jobs_storage):
    return JobsService(orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage)


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
