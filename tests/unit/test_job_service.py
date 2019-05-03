from typing import Callable

import pytest

from platform_api.config import JobsConfig
from platform_api.orchestrator import Job, JobRequest, JobsService, JobStatus
from platform_api.orchestrator.job import AggregatedRunTime, JobRecord, JobStatusItem
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.jobs_service import (
    GpuQuotaExceededError,
    JobsServiceException,
    NonGpuQuotaExceededError,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.user import User

from .conftest import MockJobsStorage, MockOrchestrator, create_quota


class TestJobsService:
    @pytest.mark.asyncio
    async def test_create_job(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
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
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
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
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        storage = MockJobsStorage()
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
        user = User(name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert job_1.status == JobStatus.PENDING
        assert not job_1.is_finished

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        await jobs_service.update_jobs_statuses()

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
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        first_job_status: JobStatus,
    ) -> None:
        storage = MockJobsStorage()
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
        user = User(name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()

        first_job, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert first_job.status == JobStatus.PENDING
        assert not first_job.is_finished

        mock_orchestrator.update_status_to_return(first_job_status)
        await jobs_service.update_jobs_statuses()

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
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_delete = False

        storage = MockJobsStorage()
        storage.fail_set_job_transaction = True
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )

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
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_delete = True

        storage = MockJobsStorage()
        storage.fail_set_job_transaction = True
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )

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
    async def test_get_status_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_all(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
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
        self,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        storage = MockJobsStorage()
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
        user = User(name="testuser", token="")

        async def create_job() -> Job:
            job_request = job_request_factory()
            job, _ = await service.create_job(job_request=job_request, user=user)
            return job

        job_pending = await create_job()

        job_running = await create_job()
        async with storage.try_update_job(job_running.id) as record:
            record.status = JobStatus.RUNNING

        job_succeeded = await create_job()
        async with storage.try_update_job(job_succeeded.id) as record:
            record.status = JobStatus.SUCCEEDED

        job_failed = await create_job()
        async with storage.try_update_job(job_failed.id) as record:
            record.status = JobStatus.FAILED

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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(deletion_delay_s=60),
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(deletion_delay_s=0),
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_get_job_status = True
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(deletion_delay_s=0),
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
    async def test_update_jobs_statuses_pending_errimagepull(
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_reason_to_return("ErrImagePull")
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_imagepullbackoff(
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
        )

        user = User(name="testuser", token="")
        original_job, _ = await service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_reason_to_return("ImagePullBackOff")
        await service.update_jobs_statuses()

        job = await service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_succeeded_missing(
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(deletion_delay_s=0),
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        storage = MockJobsStorage()
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        storage = MockJobsStorage()
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
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
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        storage = MockJobsStorage()
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=storage,
            jobs_config=JobsConfig(),
        )
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)


class TestJobFilter:
    def _create_job_request(self) -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    def test_check_empty_filter(self) -> None:
        job = JobRecord.create(request=self._create_job_request(), owner="testuser")
        assert JobFilter().check(job)

    def test_check_statuses(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(),
            owner="testuser",
            status=JobStatus.PENDING,
        )
        assert not JobFilter(statuses={JobStatus.RUNNING}).check(job)

    def test_check_owners(self) -> None:
        job = JobRecord.create(request=self._create_job_request(), owner="testuser")
        assert not JobFilter(owners={"anotheruser"}).check(job)

    def test_check_name(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(), owner="testuser", name="testname"
        )
        assert not JobFilter(name="anothername").check(job)

    def test_check_all(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(),
            status=JobStatus.PENDING,
            owner="testuser",
            name="testname",
        )
        assert JobFilter(
            statuses={JobStatus.PENDING}, owners={"testuser"}, name="testname"
        ).check(job)
