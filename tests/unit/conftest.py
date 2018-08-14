import asyncio
from pathlib import PurePath

import pytest

from platform_api.config import StorageConfig
from platform_api.orchestrator import (
    Job, JobError, JobRequest, JobsService, JobStatus, KubeConfig, LogReader,
    Orchestrator
)
from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources


class MockOrchestrator(Orchestrator):
    def __init__(self, config):
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING

        self.raise_on_delete = False

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, config):
        self._config = config

    async def start_job(self, job: Job):
        job.status = JobStatus.PENDING
        return JobStatus.PENDING

    async def status_job(self, *args, **kwargs) -> JobStatus:
        return (await self.get_job_status(*args, **kwargs)).status

    async def get_job_status(self, job_id: str) -> JobStatusItem:
        return JobStatusItem.create(self._mock_status_to_return)

    async def delete_job(self, *args, **kwargs):
        if self.raise_on_delete:
            raise JobError()
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus):
        self._mock_status_to_return = new_status

    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass


@pytest.fixture
def job_request_factory():
    def factory():
        return JobRequest.create(Container(
            image='testimage',
            resources=ContainerResources(cpu=1, memory_mb=128),
        ))
    return factory


@pytest.fixture(scope='function')
def mock_job_request(job_request_factory):
    return job_request_factory()


@pytest.fixture(scope='function')
def mock_orchestrator():
    storage_config = StorageConfig(host_mount_path=PurePath('/tmp'))
    config = KubeConfig(
        storage=storage_config,
        jobs_ingress_name='platformjobsingress',
        jobs_domain_name='jobs',
        endpoint_url='http://k8s:1234'
    )
    return MockOrchestrator(config=config)


@pytest.fixture(scope='function')
def jobs_service(mock_orchestrator):
    return JobsService(orchestrator=mock_orchestrator)


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
