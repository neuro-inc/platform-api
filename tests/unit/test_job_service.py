import pytest
from platform_api.orchestrator import JobStatus


class MockJob:

    def __init__(self):
        self.job_status = JobStatus.SUCCEEDED
        self.is_deleted = False

    @property
    def id(self):
        return 1

    async def status(self):
        return self.job_status

    async def delete(self):
        self.is_deleted = True
        return self.job_status


@pytest.fixture(scope="function")
def mock_job():
    return MockJob()


class TestInMemoryJobsService:

    @pytest.mark.asyncio
    async def test_create_job(self):

        pass

    @pytest.mark.asyncio
    async def test_get_status_by_status_id(self, mock_job):
        pass

    @pytest.mark.asyncio
    async def test_get(self, mock_job):
        pass

    @pytest.mark.asyncio
    async def test_set(self, mock_job):
        pass

    @pytest.mark.asyncio
    async def test_get_job_status(self, mock_job):
        pass

    @pytest.mark.asyncio
    async def test_delete(self, mock_job):
        pass

    async def get_all(self, mock_job):
        pass
