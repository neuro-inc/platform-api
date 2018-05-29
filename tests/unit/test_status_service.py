import pytest
from platform_api.orchestrator import InMemoryStatusService, JobStatus


class MockJob:

    def __init__(self):
        self.job_status = JobStatus.SUCCEEDED

    @property
    def id(self):
        return 1

    async def status(self):
        return self.job_status


@pytest.fixture(scope="function")
def mock_job():
    return MockJob()


class TestInMemoryStatusService:

    @pytest.mark.asyncio
    async def test_get_not_exist_status(self):
        not_exist_status = 'not_exist_status'
        status_service = InMemoryStatusService()
        status = await status_service.get(not_exist_status)
        assert status is None

    @pytest.mark.asyncio
    async def test_status_same_job(self, mock_job):
        status_service = InMemoryStatusService()
        status = await status_service.create(mock_job)
        assert status.job.id is mock_job.id

    @pytest.mark.asyncio
    async def test_get_same_status(self, mock_job):
        status_service = InMemoryStatusService()
        status = await status_service.create(mock_job)
        new_status = await status_service.get(status_id=status.status_id)
        assert status.status_id == new_status.status_id

    @pytest.mark.asyncio
    async def test_get_status_and_value(self, mock_job):
        status_service = InMemoryStatusService()
        status = await status_service.create(mock_job)
        status_value = await status.value()
        assert status_value == JobStatus.SUCCEEDED

        new_status = await status_service.get(status_id=status.status_id)
        status_value = await new_status.value()
        assert status_value == JobStatus.SUCCEEDED

        mock_job.job_status = JobStatus.FAILED

        status_value = await new_status.value()
        assert status_value == JobStatus.FAILED
