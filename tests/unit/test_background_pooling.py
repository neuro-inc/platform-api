import asyncio
import pytest

from platform_api.orchestrator import (
    JobsStatusPooling, JobStatus, JobsService,)


class TestJobsStatusPooling:

    async def wait_for_job_status(self, jobs_service, num=10, interval=1):
        for _ in range(num):
            all_jobs = await jobs_service.get_all_jobs()
            if all(job.status == JobStatus.SUCCEEDED for job in all_jobs):
                break
            else:
                await asyncio.sleep(interval)
        else:
            pytest.fail('Not all jobs have succeeded')

    async def create_job_pooling(self, mock_orchestrator, event_loop):
        jobs_service = JobsService(orchestrator=mock_orchestrator)
        jobs_status_pooling = JobsStatusPooling(jobs_service=jobs_service, loop=event_loop, interval_s=1)
        await jobs_status_pooling.start()
        return jobs_status_pooling, jobs_service

    @pytest.mark.asyncio
    async def test_pooling(
            self, mock_orchestrator, event_loop, job_request_factory):
        jobs_status_pooling, jobs_service = await self.create_job_pooling(mock_orchestrator, event_loop)

        await jobs_service.create_job(job_request_factory())
        await jobs_service.create_job(job_request_factory())

        all_jobs = await jobs_service.get_all_jobs()
        assert all(job.status == JobStatus.PENDING for job in all_jobs)

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await self.wait_for_job_status(jobs_service=jobs_service)
        await jobs_status_pooling.stop()

    @pytest.mark.asyncio
    async def test_pooling_exception(
            self, event_loop, mock_orchestrator, job_request_factory):
        jobs_status_pooling, jobs_service = await self.create_job_pooling(mock_orchestrator, event_loop)

        await jobs_service.create_job(job_request_factory())
        await jobs_service.create_job(job_request_factory())

        all_jobs = await jobs_service.get_all_jobs()
        assert all(job.status == JobStatus.PENDING for job in all_jobs)

        def update_jobs_status():
            print("update_jobs_status with error")
            raise ValueError("some unknown error")
        jobs_service.update_jobs_status = update_jobs_status

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await self.wait_for_job_status(jobs_service=jobs_service)
        await jobs_status_pooling.stop()
