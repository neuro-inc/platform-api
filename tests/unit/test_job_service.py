import dataclasses

import pytest

from platform_api.orchestrator import JobsService, JobStatus
from platform_api.orchestrator.job import JobStatusItem
from platform_api.user import User


class TestJobsService:
    @pytest.mark.asyncio
    async def test_create_job(self, jobs_service, mock_job_request):
        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user
        )
        assert original_job.status == JobStatus.PENDING
        assert not original_job.is_finished

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.id == original_job.id
        assert job.status == JobStatus.PENDING
        assert job.owner == "testuser"

    @pytest.mark.asyncio
    async def test_get_status_by_job_id(self, jobs_service, mock_job_request):
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_all(self, jobs_service, job_request_factory):
        user = User(name="testuser", token="")
        job_ids = []
        num_jobs = 1000
        for _ in range(num_jobs):
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(job_request=job_request, user=user)
            job_ids.append(job.id)

        jobs = await jobs_service.get_all_jobs()
        assert job_ids == [job.id for job in jobs]

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_running(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        service = JobsService(
            orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )
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
        self, mock_orchestrator, mock_jobs_storage_factory, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_jobs_storage = mock_jobs_storage_factory(config)
        service = JobsService(
            orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_missing(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_orchestrator.raise_on_get_job_status = True
        service = JobsService(
            orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )

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
        self, mock_orchestrator, mock_jobs_storage_factory, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_orchestrator.raise_on_delete = True
        mock_jobs_storage = mock_jobs_storage_factory(config)
        service = JobsService(
            orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_delete_running(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        service = JobsService(
            orchestrator=mock_orchestrator, jobs_storage=mock_jobs_storage
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await service.delete_job(original_job.id)

        job = await service.get_job(original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted
