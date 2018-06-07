import pytest
from platform_api.orchestrator import JobStatus, InMemoryJobsService, JobRequest, Orchestrator


class MockOrchestrator(Orchestrator):
    async def start_job(self, *args, **kwargs):
        return JobStatus.SUCCEEDED

    async def status_job(self, *args, **kwargs):
        return JobStatus.SUCCEEDED

    async def delete_job(self, *args, **kwargs):
        return JobStatus.SUCCEEDED


@pytest.fixture(scope="function")
def mock_job_request():
    return JobRequest.create(container=None)


@pytest.fixture(scope="function")
def mock_orchestrator():
    return MockOrchestrator()


@pytest.fixture(scope="function")
def jobs_service(mock_orchestrator):
    return InMemoryJobsService(orchestrator=mock_orchestrator)


class TestInMemoryJobsService:

    @pytest.mark.asyncio
    async def test_create_job(self, jobs_service, mock_job_request):
        job, status = await jobs_service.create_job(job_request=mock_job_request)

        job_record = await jobs_service.get(job_id=job.id)
        assert job_record.job.id == job.id
        assert job_record.status.id == status.id
        assert job_record.status.value == status.value

    @pytest.mark.asyncio
    async def test_get_status_by_job_id(self, jobs_service, mock_job_request):
        job, status = await jobs_service.create_job(job_request=mock_job_request)
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == status.value

    @pytest.mark.asyncio
    async def test_get_all(self, jobs_service):
        job_ids = []
        num_jobs = 1000
        for _ in range(num_jobs):
            job_request = JobRequest.create(container=None)
            job, _ = await jobs_service.create_job(job_request=job_request)
            job_ids.append(job.id)

        jobs = await jobs_service.get_all()
        assert job_ids == [x['job_id'] for x in jobs]

    @pytest.mark.asyncio
    async def test_delete(self, jobs_service):
        num_jobs = 10
        for _ in range(num_jobs):
            job_request = JobRequest.create(container=None)
            await jobs_service.create_job(job_request=job_request)

        jobs = await jobs_service.get_all()
        for job in jobs:
            await jobs_service.delete(job_id=job['job_id'])

        jobs = await jobs_service.get_all()
        assert len(jobs) == 10
        for job in jobs:
            assert job['status'] == JobStatus.DELETED
