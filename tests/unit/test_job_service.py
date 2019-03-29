import dataclasses
from unittest import mock
from unittest.mock import MagicMock

import pytest

from platform_api.handlers.jobs_handler import convert_job_to_job_response
from platform_api.orchestrator import Job, JobRequest, JobsService, JobStatus
from platform_api.orchestrator.job import JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.jobs_service import (
    GpuQuotaExceededError,
    JobsServiceException,
    NonGpuQuotaExceededError,
)
from platform_api.orchestrator.jobs_storage import JobFilter, JobStorageJobFoundError
from platform_api.user import User
from tests.unit.conftest import MockJobsStorage, create_quota


class TestMockJobsStorage:
    @pytest.mark.asyncio
    async def test_get_all_jobs_empty(self, mock_orchestrator):
        jobs_storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs = await jobs_storage.get_all_jobs()
        assert not jobs

    def _create_job_request(self):
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    def _create_job_request_with_description(self):
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            ),
            description="test test description",
        )

    @pytest.mark.asyncio
    async def test_job_to_job_response(self, mock_orchestrator):
        job = Job(
            orchestrator_config=mock_orchestrator.config,
            job_request=self._create_job_request_with_description(),
            name="test-job-name",
        )
        response = convert_job_to_job_response(job, MagicMock())
        assert response == {
            "id": job.id,
            "owner": "compute",
            "status": "pending",
            "history": {
                "status": "pending",
                "reason": None,
                "description": None,
                "created_at": mock.ANY,
            },
            "container": {
                "image": "testimage",
                "env": {},
                "volumes": [],
                "resources": {"cpu": 1, "memory_mb": 128},
            },
            "name": "test-job-name",
            "description": "test test description",
            "ssh_auth_server": "ssh://nobody@ssh-auth:22",
            "is_preemptible": False,
        }

    @pytest.mark.asyncio
    async def test_set_get_job(self, mock_orchestrator):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        jobs_storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)

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

        job_filter = JobFilter(statuses={JobStatus.PENDING, JobStatus.RUNNING})
        jobs = await jobs_storage.get_all_jobs(job_filter)
        assert {job.id for job in jobs} == {running_job.id, pending_job.id}

        jobs = await jobs_storage.get_running_jobs()
        assert {job.id for job in jobs} == {running_job.id}

        jobs = await jobs_storage.get_unfinished_jobs()
        assert {job.id for job in jobs} == {pending_job.id, running_job.id}

        jobs = await jobs_storage.get_jobs_for_deletion()
        assert {job.id for job in jobs} == {succeeded_job.id}

    @pytest.mark.asyncio
    async def test_try_create_job(self, mock_orchestrator):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        jobs_storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)

        job = Job(
            orchestrator_config=config,
            job_request=self._create_job_request(),
            name="job-name",
        )

        async with jobs_storage.try_create_job(job):
            pass

        retrieved_job = await jobs_storage.get_job(job.id)
        assert retrieved_job.id == job.id

        with pytest.raises(JobStorageJobFoundError):
            async with jobs_storage.try_create_job(job):
                pass


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
    async def test_create_job__name_conflict_with_pending(
        self, jobs_service, job_request_factory
    ):
        user = User(name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert job_1.status == JobStatus.PENDING
        assert not job_1.is_finished

        with pytest.raises(
            JobsServiceException,
            match=f"job with name '{job_name}' and owner '{user.name}'"
            f" already exists: '{job_1.id}'",
        ):
            job_2, _ = await jobs_service.create_job(request, user, job_name=job_name)

    @pytest.mark.asyncio
    async def test_create_job__name_conflict_with_running(
        self, mock_orchestrator, job_request_factory
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert job_1.status == JobStatus.PENDING
        assert not job_1.is_finished

        job_1.status = JobStatus.RUNNING
        await storage.set_job(job_1)

        job = await jobs_service.get_job(job_id=job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.RUNNING

        with pytest.raises(
            JobsServiceException,
            match=f"job with name '{job_name}' and owner '{user.name}'"
            f" already exists: '{job_1.id}'",
        ):
            job_2, _ = await jobs_service.create_job(request, user, job_name=job_name)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.FAILED, JobStatus.SUCCEEDED]
    )
    async def test_create_job__name_no_conflict_with_another_in_terminal_status(
        self, mock_orchestrator, job_request_factory, first_job_status
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()

        first_job, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert first_job.status == JobStatus.PENDING
        assert not first_job.is_finished

        first_job.status = first_job_status
        await storage.set_job(first_job)

        job = await jobs_service.get_job(job_id=first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        second_job, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert second_job.status == JobStatus.PENDING
        assert not second_job.is_finished

        job = await jobs_service.get_job(job_id=second_job.id)
        assert job.id == second_job.id
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_create_job__clean_up_the_job_on_transaction_error__ok(
        self, mock_orchestrator, job_request_factory
    ):
        mock_orchestrator.raise_on_delete = False

        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        storage.fail_set_job_transaction = True
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)

        user = User(name="testuser", token="")
        job_name = "test-Job_name"

        request = job_request_factory()

        with pytest.raises(
            JobsServiceException, match=f"Failed to create job: transaction failed"
        ):
            job, _ = await jobs_service.create_job(request, user, job_name=job_name)
            # check that the job was cleaned up:
            assert job in mock_orchestrator.get_successfully_deleted_jobs()

    @pytest.mark.asyncio
    async def test_create_job__clean_up_the_job_on_transaction_error__fail(
        self, mock_orchestrator, job_request_factory
    ):
        mock_orchestrator.raise_on_delete = True

        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        storage.fail_set_job_transaction = True
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)

        user = User(name="testuser", token="")
        job_name = "test-Job_name"

        request = job_request_factory()

        with pytest.raises(
            JobsServiceException, match=f"Failed to create job: transaction failed"
        ):
            job, _ = await jobs_service.create_job(request, user, job_name=job_name)
            # check that the job failed to be cleaned up (failure ignored):
            assert job not in mock_orchestrator.get_successfully_deleted_jobs()

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
    async def test_get_all_filter_by_status(
        self, mock_orchestrator, job_request_factory
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="")

        async def create_job():
            job_request = job_request_factory()
            job, _ = await service.create_job(job_request=job_request, user=user)
            return job

        job_pending = await create_job()

        job_running = await create_job()
        job_running.status = JobStatus.RUNNING
        await storage.set_job(job_running)

        job_succeeded = await create_job()
        job_succeeded.status = JobStatus.SUCCEEDED
        await storage.set_job(job_succeeded)

        job_failed = await create_job()
        job_failed.status = JobStatus.FAILED
        await storage.set_job(job_failed)

        jobs = await service.get_all_jobs()
        job_ids = {job.id for job in jobs}
        assert job_ids == {
            job_pending.id,
            job_running.id,
            job_succeeded.id,
            job_failed.id,
        }

        job_filter = JobFilter(statuses={JobStatus.SUCCEEDED, JobStatus.RUNNING})
        jobs = await service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_succeeded.id, job_running.id}

        job_filter = JobFilter(statuses={JobStatus.FAILED, JobStatus.PENDING})
        jobs = await service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_failed.id, job_pending.id}

        job_filter = JobFilter(statuses={JobStatus.RUNNING})
        jobs = await service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_running.id}

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_running(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        service = JobsService(mock_orchestrator, mock_jobs_storage)

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
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_jobs_storage.orchestrator_config = config
        service = JobsService(mock_orchestrator, mock_jobs_storage)

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
        service = JobsService(mock_orchestrator, mock_jobs_storage)

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
        self, mock_orchestrator, mock_jobs_storage, job_request_factory
    ):
        config = dataclasses.replace(mock_orchestrator.config, job_deletion_delay_s=0)
        mock_orchestrator.config = config
        mock_jobs_storage.orchestrator_config = config
        service = JobsService(mock_orchestrator, mock_jobs_storage)

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
        service = JobsService(mock_orchestrator, mock_jobs_storage)

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "quota",
        [
            create_quota(),
            create_quota(time_gpu_minutes=100),
            create_quota(time_non_gpu_minutes=100),
            create_quota(time_non_gpu_minutes=100, time_gpu_minutes=100),
        ],
    )
    async def test_create_job_quota_allows(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory, quota
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        job, _ = await jobs_service.create_job(request, user)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "quota",
        [
            create_quota(time_gpu_minutes=0),
            create_quota(time_gpu_minutes=0, time_non_gpu_minutes=100),
        ],
    )
    async def test_raise_for_quota_raise_for_gpu(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory, quota
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        with pytest.raises(GpuQuotaExceededError, match="GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "quota",
        [
            create_quota(time_non_gpu_minutes=0),
            create_quota(time_non_gpu_minutes=0, time_gpu_minutes=100),
        ],
    )
    async def test_raise_for_quota_raise_for_non_gpu(
        self, mock_orchestrator, mock_jobs_storage, job_request_factory, quota
    ):
        storage = MockJobsStorage(orchestrator_config=mock_orchestrator.config)
        jobs_service = JobsService(orchestrator=mock_orchestrator, jobs_storage=storage)
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)
