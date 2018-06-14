import asyncio

import pytest

from platform_api.orchestrator import JobStatus, InMemoryJobsService, JobRequest, Orchestrator


class MockOrchestrator(Orchestrator):
    def __init__(self):
        self._mock_status_to_return = JobStatus.PENDING

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
    return MockOrchestrator()


@pytest.fixture(scope="function")
def jobs_service(mock_orchestrator):
    return InMemoryJobsService(orchestrator=mock_orchestrator)


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()
