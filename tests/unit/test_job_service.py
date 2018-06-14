import asyncio
import pytest
from platform_api.orchestrator import JobStatus, JobRequest, InMemoryJobsService, JobsStatusPooling


class TestInMemoryJobsService:

    @pytest.mark.asyncio
    async def test_create_job(self, jobs_service, mock_job_request):
        job, status = await jobs_service.create_job(job_request=mock_job_request)

        job_record = await jobs_service.get_job(job_id=job.id)
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

        jobs = await jobs_service.get_all_jobs()
        assert job_ids == [x['job_id'] for x in jobs]

    @pytest.mark.asyncio
    async def test_delete(self, mock_orchestrator, event_loop):
        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        jobs_service = InMemoryJobsService(orchestrator=mock_orchestrator)
        jobs_status_pooling = JobsStatusPooling(jobs_service=jobs_service, loop=event_loop, interval_s=1)
        await jobs_status_pooling.start()

        num_jobs = 10
        for _ in range(num_jobs):
            job_request = JobRequest.create(container=None)
            await jobs_service.create_job(job_request=job_request)

        jobs = await jobs_service.get_all_jobs()
        for job in jobs:
            await jobs_service.delete_job(job_id=job['job_id'])

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        for _ in range(10):
            jobs = await jobs_service.get_all_jobs()
            if not all(x['status'].value == JobStatus.SUCCEEDED for x in jobs):
                await asyncio.sleep(1)
        await jobs_status_pooling.stop()