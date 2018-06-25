import pytest
from platform_api.orchestrator import (
    Job, JobStatus, JobsService, JobRequest, JobsStatusPooling)
from platform_api.orchestrator.job_request import (
    Container, ContainerResources,)
from platform_api.orchestrator.jobs_service import InMemoryJobsStorage


class TestInMemoryJobsStorage:
    @pytest.mark.asyncio
    async def test_get_all_jobs_empty(self, mock_orchestrator):
        jobs_storage = InMemoryJobsStorage(orchestrator=mock_orchestrator)
        jobs = await jobs_storage.get_all_jobs()
        assert not jobs

    def _create_job_request(self):
        return JobRequest.create(Container(
            image='testimage',
            resources=ContainerResources(cpu=1, memory_mb=128),
        ))

    @pytest.mark.asyncio
    async def test_set_get_job(self, mock_orchestrator):
        jobs_storage = InMemoryJobsStorage(orchestrator=mock_orchestrator)

        pending_job = Job(
            orchestrator=mock_orchestrator,
            job_request=self._create_job_request())
        await jobs_storage.set_job(pending_job)

        succeeded_job = Job(
            orchestrator=mock_orchestrator,
            job_request=self._create_job_request(),
            status=JobStatus.SUCCEEDED)
        await jobs_storage.set_job(succeeded_job)

        job = await jobs_storage.get_job(pending_job.id)
        assert job.id == pending_job.id
        assert job.request == pending_job.request

        jobs = await jobs_storage.get_all_jobs()
        assert {job.id for job in jobs} == {pending_job.id, succeeded_job.id}

        jobs = await jobs_storage.get_running_jobs()
        assert {job.id for job in jobs} == {pending_job.id}


class TestInMemoryJobsService:

    @pytest.mark.asyncio
    async def test_create_job(self, jobs_service, mock_job_request):
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request)
        assert original_job.status == JobStatus.PENDING
        assert not original_job.is_finished

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.id == original_job.id
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_status_by_job_id(self, jobs_service, mock_job_request):
        job, _ = await jobs_service.create_job(job_request=mock_job_request)
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_all(self, jobs_service, job_request_factory):
        job_ids = []
        num_jobs = 1000
        for _ in range(num_jobs):
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(job_request=job_request)
            job_ids.append(job.id)

        jobs = await jobs_service.get_all_jobs()
        assert job_ids == [job.id for job in jobs]

    @pytest.mark.asyncio
    async def test_update_jobs_statuses(
            self, mock_orchestrator, job_request_factory):
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(
            job_request=job_request_factory())
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_delete(
            self, mock_orchestrator, event_loop, job_request_factory):
        jobs_service = JobsService(orchestrator=mock_orchestrator)

        num_jobs = 10
        for _ in range(num_jobs):
            job_request = job_request_factory()
            await jobs_service.create_job(job_request=job_request)

        jobs = await jobs_service.get_all_jobs()
        assert len(jobs) == num_jobs
        for job in jobs:
            await jobs_service.delete_job(job_id=job.id)

        jobs = await jobs_service.get_all_jobs()
        assert len(jobs) == num_jobs
        assert all(job.status == JobStatus.PENDING for job in jobs)

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)

        # making sure job statuses get updated at least once
        jobs_status_pooling = JobsStatusPooling(
            jobs_service=jobs_service, loop=event_loop, interval_s=1)
        await jobs_status_pooling.start()
        await jobs_status_pooling.stop()

        jobs = await jobs_service.get_all_jobs()
        assert len(jobs) == num_jobs
        assert all(job.status == JobStatus.SUCCEEDED for job in jobs)
