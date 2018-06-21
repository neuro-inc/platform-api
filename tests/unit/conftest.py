import asyncio
from pathlib import PurePath

import pytest

from platform_api.config import KubeConfig
from platform_api.orchestrator import JobStatus, InMemoryJobsService, JobRequest, Orchestrator


class MockOrchestrator(Orchestrator):
    def __init__(self, config):
        self._config = config
        self._mock_status_to_return = JobStatus.PENDING

    @property
    def config(self):
        return self._config

    async def start_job(self, *args, **kwargs):
        return JobStatus.PENDING

    async def status_job(self, *args, **kwargs):
        return self._mock_status_to_return

    async def delete_job(self, *args, **kwargs):
        return JobStatus.SUCCEEDED

    def update_status_to_return(self, new_status: JobStatus):
        self._mock_status_to_return = new_status


@pytest.fixture(scope="function")
def mock_job_request():
    return JobRequest.create(container=None)


@pytest.fixture(scope="function")
def mock_orchestrator():
    config = KubeConfig(
        storage_mount_path=PurePath('/tmp'),
        jobs_ingress_name='platformjobsingress',
        jobs_ingress_domain_name='jobs',
        endpoint_url='http://k8s:1234'
    )
    return MockOrchestrator(config=config)


@pytest.fixture(scope="function")
def jobs_service(mock_orchestrator):
    return InMemoryJobsService(orchestrator=mock_orchestrator)


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
