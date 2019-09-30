from dataclasses import replace
from typing import AsyncIterator, Callable
from unittest.mock import MagicMock

import pytest
from notifications_client import Client as NotificationsClient, JobTransition

from platform_api.cluster import Cluster, ClusterConfig, ClusterRegistry
from platform_api.config import JobsConfig
from platform_api.orchestrator.job import (
    AggregatedRunTime,
    Job,
    JobStatusItem,
    JobStatusReason,
)
from platform_api.orchestrator.job_request import JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import (
    GpuQuotaExceededError,
    JobsService,
    JobsServiceException,
    NonGpuQuotaExceededError,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.user import User

from .conftest import (
    MockCluster,
    MockJobsStorage,
    MockNotificationsClient,
    MockOrchestrator,
    create_quota,
)


class TestJobsService:
    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_registry: ClusterRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
    ) -> Callable[..., JobsService]:
        def _factory(deletion_delay_s: int = 0) -> JobsService:
            return JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(deletion_delay_s=deletion_delay_s),
                notifications_client=mock_notifications_client,
            )

        return _factory

    @pytest.fixture
    def jobs_service(
        self, jobs_service_factory: Callable[..., JobsService]
    ) -> JobsService:
        return jobs_service_factory()

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
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
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
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        first_job_status: JobStatus,
    ) -> None:
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
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_delete = False
        mock_jobs_storage.fail_set_job_transaction = True

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
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_delete = True
        mock_jobs_storage.fail_set_job_transaction = True

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
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = User(name="testuser", token="")

        async def create_job() -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(job_request=job_request, user=user)
            return job

        job_pending = await create_job()

        job_running = await create_job()
        async with mock_jobs_storage.try_update_job(job_running.id) as record:
            record.status = JobStatus.RUNNING

        job_succeeded = await create_job()
        async with mock_jobs_storage.try_update_job(job_succeeded.id) as record:
            record.status = JobStatus.SUCCEEDED

        job_failed = await create_job()
        async with mock_jobs_storage.try_update_job(job_failed.id) as record:
            record.status = JobStatus.FAILED

        jobs = await jobs_service.get_all_jobs()
        job_ids = {job.id for job in jobs}
        assert job_ids == {
            job_pending.id,
            job_running.id,
            job_succeeded.id,
            job_failed.id,
        }

        job_filter = JobFilter(statuses={JobStatus.SUCCEEDED, JobStatus.RUNNING})
        jobs = await jobs_service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_succeeded.id, job_running.id}

        job_filter = JobFilter(statuses={JobStatus.FAILED, JobStatus.PENDING})
        jobs = await jobs_service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_failed.id, job_pending.id}

        job_filter = JobFilter(statuses={JobStatus.RUNNING})
        jobs = await jobs_service.get_all_jobs(job_filter)
        job_ids = {job.id for job in jobs}
        assert job_ids == {job_running.id}

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_running(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_for_deletion(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_get_job_status = True
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted
        assert job.status_history.current == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.NOT_FOUND,
            description="The job could not be scheduled or was preempted.",
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reason,description",
        [
            (JobStatusReason.ERR_IMAGE_PULL, "Image can not be pulled"),
            (JobStatusReason.IMAGE_PULL_BACK_OFF, "Image can not be pulled"),
            (JobStatusReason.INVALID_IMAGE_NAME, "Invalid image name"),
        ],
    )
    async def test_update_jobs_statuses_pending_errimagepull(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        reason: str,
        description: str,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_reason_to_return(reason)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.COLLECTED
        assert status_item.description == description

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_scale_up(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        mock_orchestrator.update_status_to_return(JobStatus.FAILED)
        mock_orchestrator.update_reason_to_return(
            JobStatusReason.CLUSTER_SCALE_UP_FAILED
        )
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_succeeded_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.is_deleted

    @pytest.mark.asyncio
    async def test_delete_running(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = User(name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.delete_job(original_job.id)

        job = await jobs_service.get_job(original_job.id)
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
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
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
        jobs_service: JobsService,
        gpu_job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        user = User(name="testuser", token="token", quota=quota)
        request = gpu_job_request_factory()

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
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    @pytest.mark.asyncio
    async def test_create_job_quota_gpu_exceeded_cpu_allows_ok_for_cpu_job(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        quota = create_quota(time_gpu_minutes=0, time_non_gpu_minutes=100)
        user = User(name="testuser", token="token", quota=quota)
        request = job_request_factory()

        job, _ = await jobs_service.create_job(request, user)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_create_job_quota_gpu_allows_cpu_exceeded_raise_for_gpu_job(
        self,
        jobs_service: JobsService,
        gpu_job_request_factory: Callable[[], JobRequest],
    ) -> None:
        # Even GPU-jobs require CPU
        quota = create_quota(time_gpu_minutes=100, time_non_gpu_minutes=0)
        user = User(name="testuser", token="token", quota=quota)
        request = gpu_job_request_factory()

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    def test_get_cluster_name_non_empty(self, jobs_service: JobsService) -> None:
        mocked_job = MagicMock(cluster_name="my-cluster")
        assert jobs_service.get_cluster_name(mocked_job) == "my-cluster"

    def test_get_cluster_name_empty(self, jobs_service: JobsService) -> None:
        mocked_job = MagicMock(cluster_name="")
        default_cluster_name = jobs_service._jobs_config.default_cluster_name
        assert jobs_service.get_cluster_name(mocked_job) == default_cluster_name


class TestJobsServiceCluster:
    @pytest.fixture
    async def cluster_registry(self) -> AsyncIterator[ClusterRegistry]:
        def _cluster_factory(config: ClusterConfig) -> Cluster:
            orchestrator = MockOrchestrator(config)
            return MockCluster(config, orchestrator)

        async with ClusterRegistry(factory=_cluster_factory) as registry:
            yield registry

    @pytest.fixture
    def jobs_service(
        self,
        cluster_registry: ClusterRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
    ) -> JobsService:
        return JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(default_cluster_name="default"),
            notifications_client=mock_notifications_client,
        )

    @pytest.mark.asyncio
    async def test_create_job_missing_cluster(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(name="testuser", token="testtoken", cluster_name="missing")

        with pytest.raises(JobsServiceException, match="Cluster 'missing' not found"):
            await jobs_service.create_job(mock_job_request, user)

    @pytest.mark.asyncio
    async def test_create_job_user_cluster_name_fallback(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(name="testuser", token="testtoken")

        with pytest.raises(JobsServiceException, match="Cluster 'default' not found"):
            await jobs_service.create_job(mock_job_request, user)

    @pytest.mark.asyncio
    async def test_update_pending_job_missing_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
        )
        await cluster_registry.add(cluster_config)

        user = User(name="testuser", token="testtoken", cluster_name="default")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status_history.current == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_NOT_FOUND,
            description="Cluster 'default' not found",
        )
        assert record.is_deleted

    @pytest.mark.asyncio
    async def test_update_succeeded_job_missing_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
        )
        await cluster_registry.add(cluster_config)

        user = User(name="testuser", token="testtoken", cluster_name="default")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        async with mock_jobs_storage.try_update_job(job.id) as record:
            record.status = JobStatus.SUCCEEDED

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.SUCCEEDED

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.SUCCEEDED
        assert record.is_deleted

    @pytest.mark.asyncio
    async def test_get_job_fallback(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
        )
        await cluster_registry.add(cluster_config)  # "default"
        await cluster_registry.add(replace(cluster_config, name="missing"))

        user = User(name="testuser", token="testtoken", cluster_name="missing")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"

        await cluster_registry.remove("missing")

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"

    @pytest.mark.asyncio
    async def test_delete_missing_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
        )
        await cluster_registry.add(cluster_config)

        user = User(name="testuser", token="testtoken")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.delete_job(job.id)

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.SUCCEEDED
        assert record.is_deleted


class TestJobServiceNotification:
    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_registry: ClusterRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
    ) -> Callable[..., JobsService]:
        def _factory(deletion_delay_s: int = 0) -> JobsService:
            return JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(deletion_delay_s=deletion_delay_s),
                notifications_client=mock_notifications_client,
            )

        return _factory

    @pytest.fixture
    def jobs_service(
        self, jobs_service_factory: Callable[..., JobsService]
    ) -> JobsService:
        return jobs_service_factory()

    @pytest.mark.asyncio
    async def test_new_job_created(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_status_update_same_status_will_send_notification(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)

        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_reason_to_return(JobStatusReason.CONTAINER_CREATING)
        mock_orchestrator.update_status_to_return(JobStatus.PENDING)
        await jobs_service.update_jobs_statuses()
        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CONTAINER_CREATING,
                description=None,
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=prev_transition_time,
            )
        )
        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_job_failed_errimagepull_workflow(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_reason_to_return(JobStatusReason.ERR_IMAGE_PULL)
        await jobs_service.update_jobs_statuses()
        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.FAILED,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.COLLECTED,
                description="Image can not be pulled",
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=prev_transition_time,
            )
        )

        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_job_succeeded_workflow(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(name="testuser", token="")
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_reason_to_return(JobStatusReason.CONTAINER_CREATING)
        mock_orchestrator.update_status_to_return(JobStatus.PENDING)
        await jobs_service.update_jobs_statuses()
        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CONTAINER_CREATING,
                description=None,
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=prev_transition_time,
            )
        )
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        mock_orchestrator.update_reason_to_return(None)
        await jobs_service.update_jobs_statuses()
        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.RUNNING,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=prev_transition_time,
            )
        )
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        mock_orchestrator.update_reason_to_return(None)
        mock_orchestrator.update_exit_code_to_return(0)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.SUCCEEDED,
                transition_time=job.status_history.current.transition_time,
                reason=None,
                description=None,
                exit_code=0,
                prev_status=JobStatus.RUNNING,
                prev_transition_time=prev_transition_time,
            )
        )

        assert notifications == mock_notifications_client.sent_notifications
