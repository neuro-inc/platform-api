import dataclasses

import pytest

from platform_api.orchestrator import Job, JobRequest, JobsService, JobStatus
from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources, User
from platform_api.orchestrator.jobs_service import InMemoryJobsStorage


class TestInMemoryJobsStorage:
    @pytest.mark.asyncio
    async def test_get_all_jobs_empty(self, mock_orchestrator):
        jobs_storage = InMemoryJobsStorage(orchestrator=mock_orchestrator)
        jobs = await jobs_storage.get_all_jobs()
        assert not jobs

    def _create_job_request(self):
        return JobRequest.create(
            User("user_name", "user_token"),
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            ),
        )

    @pytest.mark.asyncio
    async def test_set_get_job(self, mock_orchestrator):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        jobs_storage = InMemoryJobsStorage(orchestrator=mock_orchestrator)

        pending_job = Job(
            orchestrator_config=config, job_request=self._create_job_request()
        )
        await jobs_storage.set_job(pending_job)

        running_job = Job(
            orchestrator_config=config,
            job_request=self._create_job_request(),
            status=JobStatus.RUNNING,
        )
        await jobs_storage.set_job(running_job)

        succeeded_job = Job(
            orchestrator_config=config,
            job_request=self._create_job_request(),
            status=JobStatus.SUCCEEDED,
        )
        await jobs_storage.set_job(succeeded_job)

        job = await jobs_storage.get_job(pending_job.id)
        assert job.id == pending_job.id
        assert job.request == pending_job.request

        jobs = await jobs_storage.get_all_jobs()
        assert {job.id for job in jobs} == {
            pending_job.id,
            running_job.id,
            succeeded_job.id,
        }

        jobs = await jobs_storage.get_running_jobs()
        assert {job.id for job in jobs} == {running_job.id}

        jobs = await jobs_storage.get_unfinished_jobs()
        assert {job.id for job in jobs} == {pending_job.id, running_job.id}

        jobs = await jobs_storage.get_jobs_for_deletion()
        assert {job.id for job in jobs} == {succeeded_job.id}


class TestJobsService:
    @pytest.mark.asyncio
    async def test_create_job(self, jobs_service, mock_job_request):
        original_job, _ = await jobs_service.create_job(job_request=mock_job_request)
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
    async def test_update_jobs_statuses_running(
        self, mock_orchestrator, job_request_factory
    ):
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(job_request=job_request_factory())
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_for_deletion(
        self, mock_orchestrator, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(job_request=job_request_factory())

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_missing(
        self, mock_orchestrator, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_orchestrator.raise_on_get_job_status = True
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(job_request=job_request_factory())

        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted
        assert job.status_history.current == JobStatusItem.create(
            JobStatus.FAILED,
            reason="Missing",
            description="The job could not be scheduled or was preempted.",
        )

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_succeeded_missing(
        self, mock_orchestrator, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_orchestrator.raise_on_delete = True
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(job_request=job_request_factory())

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_delete_running(self, mock_orchestrator, job_request_factory):
        service = JobsService(orchestrator=mock_orchestrator)

        original_job, _ = await service.create_job(job_request=job_request_factory())
        assert original_job.status == JobStatus.PENDING

        await service.delete_job(original_job.id)

        job = await service.get_job(original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted
