import base64
import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable

import pytest
from _pytest.logging import LogCaptureFixture
from neuro_auth_client import AuthClient, Permission
from notifications_client import Client as NotificationsClient, JobTransition
from yarl import URL

from platform_api.cluster import Cluster, ClusterConfig, ClusterRegistry
from platform_api.cluster_config import CircuitBreakerConfig
from platform_api.config import JobsConfig, JobsSchedulerConfig
from platform_api.orchestrator.job import (
    AggregatedRunTime,
    Job,
    JobRecord,
    JobStatusItem,
    JobStatusReason,
    current_datetime_factory,
)
from platform_api.orchestrator.job_request import JobError, JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import (
    NEURO_PASSED_CONFIG,
    GpuQuotaExceededError,
    JobsScheduler,
    JobsService,
    JobsServiceException,
    NonGpuQuotaExceededError,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.user import User

from .conftest import (
    MockAuthClient,
    MockCluster,
    MockJobsStorage,
    MockNotificationsClient,
    MockOrchestrator,
    create_quota,
)


class MockJobsScheduler(JobsScheduler):
    _now: datetime

    def __init__(self) -> None:
        self._now = datetime.now(timezone.utc)
        super().__init__(
            JobsSchedulerConfig(
                is_waiting_min_time_sec=1,
                run_quantum_sec=10,
                max_suspended_time_sec=100,
            ),
            self.current_datetime_factory,
        )

    def current_datetime_factory(self) -> datetime:
        return self._now

    def tick_quantum(self) -> None:
        self._now += self._config.run_quantum

    def tick_min_waiting(self) -> None:
        self._now += self._config.is_waiting_min_time

    def tick_max_suspended(self) -> None:
        self._now += self._config.max_suspended_time


class TestJobsService:
    @pytest.fixture
    def test_scheduler(self) -> MockJobsScheduler:
        return MockJobsScheduler()

    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_registry: ClusterRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
        test_scheduler: MockJobsScheduler,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> Callable[..., JobsService]:
        def _factory(
            deletion_delay_s: int = 0, image_pull_error_delay_s: int = 0
        ) -> JobsService:
            return JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(
                    deletion_delay_s=deletion_delay_s,
                    image_pull_error_delay_s=image_pull_error_delay_s,
                ),
                notifications_client=mock_notifications_client,
                scheduler=test_scheduler,
                auth_client=mock_auth_client,
                api_base_url=mock_api_base,
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
        user = User(cluster_name="test-cluster", name="testuser", token="")
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
    async def test_create_job_pass_config(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_api_base: URL,
        mock_auth_client: MockAuthClient,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )
        assert original_job.status == JobStatus.PENDING
        assert original_job.pass_config
        passed_data_str = original_job.request.container.env[NEURO_PASSED_CONFIG]
        passed_data = json.loads(base64.b64decode(passed_data_str).decode())
        assert URL(passed_data["url"]) == mock_api_base
        assert passed_data["token"] == f"token-{user.name}"
        assert passed_data["cluster"] == original_job.cluster_name
        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client.grants[0] == (
            user.name,
            [Permission(uri=token_uri, action="read")],
        )

    @pytest.mark.asyncio
    async def test_pass_config_revoke_after_complete(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()
        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_after_failure(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )

        mock_orchestrator.update_status_to_return(JobStatus.FAILED)
        await jobs_service.update_jobs_statuses()
        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_fail_to_start(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )

        def _f(job: Job) -> Exception:
            raise JobError(f"Bad job {job.id}")

        mock_orchestrator.raise_on_start_job_status = True
        mock_orchestrator.get_job_status_exc_factory = _f
        await jobs_service.update_jobs_statuses()

        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_fail_on_update(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )

        await jobs_service.update_jobs_statuses()
        mock_orchestrator.raise_on_get_job_status = True
        await jobs_service.update_jobs_statuses()

        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_cluster_unavail(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, pass_config=True
        )

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        await jobs_service.update_jobs_statuses()

        await cluster_registry.remove(cluster_config.name)
        await jobs_service.update_jobs_statuses()

        token_uri = f"token://job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_create_job_pass_config_env_present(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_api_base: URL,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="test-token")
        mock_job_request.container.env[NEURO_PASSED_CONFIG] = "anything"
        with pytest.raises(
            JobsServiceException,
            match=f"Cannot pass config: ENV '{NEURO_PASSED_CONFIG}' "
            "already specified",
        ):
            await jobs_service.create_job(
                job_request=mock_job_request, user=user, pass_config=True
            )

    @pytest.mark.asyncio
    async def test_create_job_fail(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_orchestrator: MockOrchestrator,
        caplog: LogCaptureFixture,
    ) -> None:
        def _f(job: Job) -> Exception:
            raise JobError(f"Bad job {job.id}")

        mock_orchestrator.raise_on_start_job_status = True
        mock_orchestrator.get_job_status_exc_factory = _f

        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished

        assert caplog.text == ""

        await jobs_service.update_jobs_statuses()

        assert f"Failed to start job {job.id}. Reason: Bad job {job.id}" in caplog.text
        assert f"JobError: Bad job {job.id}" in caplog.text
        assert "Unexpected exception in cluster" not in caplog.text

    @pytest.mark.asyncio
    async def test_create_job__name_conflict_with_pending(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
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
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert job_1.status == JobStatus.PENDING
        assert job_1.status_history.current.reason == JobStatusReason.CREATING
        assert not job_1.is_finished

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.PENDING
        assert job.status_history.current.reason == JobStatusReason.CONTAINER_CREATING

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
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job_name = "test-Job_name"
        request = job_request_factory()

        first_job, _ = await jobs_service.create_job(request, user, job_name=job_name)
        assert first_job.status == JobStatus.PENDING
        assert first_job.status_history.current.reason == JobStatusReason.CREATING
        assert not first_job.is_finished

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=first_job.id)
        assert job.id == first_job.id
        assert job.status == JobStatus.PENDING
        assert job.status_history.current.reason == JobStatusReason.CONTAINER_CREATING

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

        user = User(cluster_name="test-cluster", name="testuser", token="")
        job_name = "test-Job_name"

        request = job_request_factory()

        with pytest.raises(
            JobsServiceException, match="Failed to create job: transaction failed"
        ):
            await jobs_service.create_job(request, user, job_name=job_name)
        # check that the job was cleaned up:
        assert request.job_id in {
            job.id for job in mock_orchestrator.get_successfully_deleted_jobs()
        }

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

        user = User(cluster_name="test-cluster", name="testuser", token="")
        job_name = "test-Job_name"

        request = job_request_factory()

        with pytest.raises(
            JobsServiceException, match="Failed to create job: transaction failed"
        ):
            job, _ = await jobs_service.create_job(request, user, job_name=job_name)
        # check that the job failed to be cleaned up (failure ignored):
        assert request.job_id not in {
            job.id for job in mock_orchestrator.get_successfully_deleted_jobs()
        }

    @pytest.mark.asyncio
    async def test_get_status_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_set_status_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        job_id = job.id
        job_status = await jobs_service.get_job_status(job_id)
        assert job_status == JobStatus.PENDING
        job_status = await jobs_service.get_job_status(job_id)
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CREATING

        await jobs_service.set_job_status(
            job_id, JobStatusItem.create(JobStatus.RUNNING)
        )
        job_status = await jobs_service.get_job_status(job_id)
        assert job_status == JobStatus.RUNNING
        job = await jobs_service.get_job(job_id)
        status_item = job.status_history.last
        assert status_item.reason is None

        await jobs_service.set_job_status(
            job_id, JobStatusItem.create(JobStatus.FAILED, reason="Test failure")
        )
        job_status = await jobs_service.get_job_status(job_id)
        assert job_status == JobStatus.FAILED
        job = await jobs_service.get_job(job_id)
        status_item = job.status_history.last
        assert status_item.reason == "Test failure"

    @pytest.mark.asyncio
    async def test_get_all(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
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
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")

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
    async def test_get_job_by_name(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        otheruser = User(cluster_name="test-cluster", name="otheruser", token="")

        async def create_job(job_name: str, user: User) -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(
                job_request=job_request, job_name=job_name, user=user
            )
            return job

        job1 = await create_job("job1", user)
        async with mock_jobs_storage.try_update_job(job1.id) as record:
            record.status = JobStatus.SUCCEEDED

        await create_job("job2", user)
        await create_job("job1", otheruser)

        job = await jobs_service.get_job_by_name("job1", user)
        assert job.id == job1.id

        job2 = await create_job("job1", user)

        job = await jobs_service.get_job_by_name("job1", user)
        assert job.id != job1.id
        assert job.id == job2.id

        with pytest.raises(JobError):
            await jobs_service.get_job_by_name("job3", user)

        with pytest.raises(JobError):
            await jobs_service.get_job_by_name("job2", otheruser)

    @pytest.mark.asyncio
    async def test_get_all_filter_by_date_range(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")

        async def create_job() -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(job_request=job_request, user=user)
            return job

        t1 = current_datetime_factory()
        job1 = await create_job()
        t2 = current_datetime_factory()
        job2 = await create_job()
        t3 = current_datetime_factory()
        job3 = await create_job()
        t4 = current_datetime_factory()

        job_filter = JobFilter(since=t1, until=t4)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job1.id, job2.id, job3.id}

        job_filter = JobFilter(since=t2)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job2.id, job3.id}

        job_filter = JobFilter(until=t2)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job1.id}

        job_filter = JobFilter(since=t3)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job3.id}

        job_filter = JobFilter(until=t3)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job1.id, job2.id}

        job_filter = JobFilter(since=t2, until=t3)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == {job2.id}

        job_filter = JobFilter(since=t3, until=t2)
        job_ids = {job.id for job in await jobs_service.get_all_jobs(job_filter)}
        assert job_ids == set()

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_running(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_for_deletion(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_get_job_status = True
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        assert job.status_history.current == JobStatusItem.create(
            JobStatus.PENDING,
            reason=JobStatusReason.CONTAINER_CREATING,
            description=None,
        )

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized
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

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CONTAINER_CREATING
        assert status_item.description is None

        mock_orchestrator.update_reason_to_return(reason)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.COLLECTED
        assert status_item.description == description

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reason",
        [
            JobStatusReason.ERR_IMAGE_PULL,
            JobStatusReason.IMAGE_PULL_BACK_OFF,
        ],
    )
    async def test_update_jobs_statuses_pending_errimagepull_with_delay(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        reason: str,
    ) -> None:
        jobs_service = jobs_service_factory(image_pull_error_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CONTAINER_CREATING
        assert status_item.description is None

        mock_orchestrator.update_reason_to_return(reason)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        status_item = job.status_history.last
        assert status_item.reason == reason

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_scale_up(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.FAILED)
        mock_orchestrator.update_reason_to_return(
            JobStatusReason.CLUSTER_SCALE_UP_FAILED
        )
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_succeeded_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_preemptible_additional_when_no_pending(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(10):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(), user=user, is_preemptible=True
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        test_scheduler.tick_min_waiting()

        additional_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, is_preemptible=True
        )

        await jobs_service.update_jobs_statuses()

        # Should try to start new job because there is no waiting jobs
        job = await jobs_service.get_job(additional_job.id)
        assert job.status == JobStatus.PENDING
        assert job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_preemptible_additional_when_has_pending(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(10):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(), user=user, is_preemptible=True
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING

        test_scheduler.tick_min_waiting()

        additional_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, is_preemptible=True
        )

        await jobs_service.update_jobs_statuses()

        # Should not even try to start this job because there is another waiting jobs
        job = await jobs_service.get_job(additional_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_preemptible_cycling(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(9):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(), user=user, is_preemptible=True
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING

        test_scheduler.tick_quantum()
        for job in jobs[3:6]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUSPENDED
            assert not job.materialized

        for job in jobs[3:6]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        for job in jobs[6:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING

        test_scheduler.tick_quantum()

        for job in jobs[6:]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs[:6]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUSPENDED
            assert not job.materialized

        for job in jobs[6:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        # One additional update required

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.PENDING)

        await jobs_service.update_jobs_statuses()

        # When all jobs are either running or not materialized, service
        # should materialize new job
        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            if job.materialized:
                break
        else:
            raise AssertionError("Materialized job not found")

        test_scheduler.tick_quantum()

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING
            assert job.materialized

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUSPENDED
            assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_preemptible_max_suspended_time(
        self,
        jobs_service_factory: Callable[..., JobsService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = User(cluster_name="test-cluster", name="testuser", token="")

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        job1, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, is_preemptible=True
        )
        assert job1.status == JobStatus.PENDING

        job2, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, is_preemptible=True
        )
        assert job1.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        mock_orchestrator.update_status_to_return_single(job1.id, JobStatus.RUNNING)

        await jobs_service.update_jobs_statuses()

        job1 = await jobs_service.get_job(job1.id)
        assert job1.status == JobStatus.RUNNING
        assert job1.materialized

        job2 = await jobs_service.get_job(job2.id)
        assert job2.status == JobStatus.PENDING
        assert job2.materialized

        test_scheduler.tick_min_waiting()

        job3, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, is_preemptible=True
        )
        assert job3.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        job3 = await jobs_service.get_job(job3.id)
        assert job3.status == JobStatus.PENDING
        assert not job3.materialized

        test_scheduler.tick_max_suspended()

        await jobs_service.update_jobs_statuses()

        job3 = await jobs_service.get_job(job3.id)
        assert job3.status == JobStatus.PENDING
        assert job3.materialized

    @pytest.mark.asyncio
    async def test_cancel_running(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        await jobs_service.cancel_job(original_job.id)

        job = await jobs_service.get_job(original_job.id)
        assert job.status == JobStatus.CANCELLED
        assert job.is_finished
        assert job.finished_at
        assert job.materialized

    @pytest.mark.asyncio
    async def test_cancel_deleted_after_sync(
        self,
        jobs_service_factory: Callable[[float], JobsService],
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(3600 * 7)  # Set huge deletion timeout
        user = User(cluster_name="test-cluster", name="testuser", token="")
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        await jobs_service.cancel_job(original_job.id)

        await jobs_service.update_jobs_statuses()

        job = await jobs_service.get_job(original_job.id)
        assert job.status == JobStatus.CANCELLED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

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
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
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
    async def test_raise_for_quota_raise_for_gpu_first_job(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[..., JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory(with_gpu=True)

        with pytest.raises(GpuQuotaExceededError, match="GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    @pytest.mark.asyncio
    async def test_raise_for_quota_raise_for_gpu_second_job(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        quota = create_quota(time_gpu_minutes=100)
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory(with_gpu=True)
        await jobs_service.jobs_storage.set_job(
            JobRecord.create(
                request=request,
                cluster_name=user.cluster_name,
                owner=user.name,
                status=JobStatus.RUNNING,
                current_datetime_factory=lambda: datetime.utcnow()
                - quota.total_gpu_run_time_delta,
            )
        )

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
    async def test_raise_for_quota_raise_for_non_gpu_first_job(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        quota: AggregatedRunTime,
    ) -> None:
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory()

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    @pytest.mark.asyncio
    async def test_raise_for_quota_raise_for_non_gpu_second_job(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        quota = create_quota(time_non_gpu_minutes=100)
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory()
        await jobs_service.jobs_storage.set_job(
            JobRecord.create(
                request=request,
                cluster_name=user.cluster_name,
                owner=user.name,
                status=JobStatus.RUNNING,
                current_datetime_factory=lambda: datetime.utcnow()
                - quota.total_non_gpu_run_time_delta,
            )
        )

        with pytest.raises(NonGpuQuotaExceededError, match="non-GPU quota exceeded"):
            await jobs_service.create_job(request, user)

    @pytest.mark.asyncio
    async def test_create_job_quota_gpu_exceeded_cpu_allows_ok_for_cpu_job(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        quota = create_quota(time_gpu_minutes=0, time_non_gpu_minutes=100)
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory()

        job, _ = await jobs_service.create_job(request, user)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_create_job_quota_gpu_allows_cpu_exceeded_ok_for_gpu_job(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        quota = create_quota(time_gpu_minutes=100, time_non_gpu_minutes=0)
        user = User(
            cluster_name="test-cluster", name="testuser", token="token", quota=quota
        )
        request = job_request_factory(with_gpu=True)

        job, _ = await jobs_service.create_job(request, user)
        assert job.status == JobStatus.PENDING


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
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> JobsService:
        return JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
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
        user = User(cluster_name="test-cluster", name="testuser", token="testtoken")

        with pytest.raises(
            JobsServiceException, match="Cluster 'test-cluster' not found"
        ):
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
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)

        async with cluster_registry.get(cluster_config.name) as cluster:

            def _f(*args: Any, **kwargs: Any) -> Exception:
                raise RuntimeError("test")

            cluster.orchestrator.raise_on_get_job_status = True
            cluster.orchestrator.get_job_status_exc_factory = _f

        user = User(name="testuser", token="testtoken", cluster_name="test-cluster")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

        await jobs_service.update_jobs_statuses()

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_update_pending_job_unavail_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)

        user = User(name="testuser", token="testtoken", cluster_name="test-cluster")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status_history.current == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_NOT_FOUND,
            description="Cluster 'test-cluster' not found",
        )
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_update_succeeded_job_missing_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)

        user = User(name="testuser", token="testtoken", cluster_name="test-cluster")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        async with mock_jobs_storage.try_update_job(job.id) as record:
            record.status = JobStatus.SUCCEEDED

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.SUCCEEDED

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.SUCCEEDED
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_get_job_fallback(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)  # "test-cluster"
        await cluster_registry.replace(replace(cluster_config, name="default"))
        await cluster_registry.replace(replace(cluster_config, name="missing"))

        user = User(name="testuser", token="testtoken", cluster_name="missing")
        job, _ = await jobs_service.create_job(mock_job_request, user)
        assert job.cluster_name == "missing"

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"

        await cluster_registry.remove("missing")

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"
        assert job.http_host == f"{job.id}.missing-cluster"
        assert job.http_host_named is None

    @pytest.mark.asyncio
    async def test_get_job_unavail_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        cluster_config = replace(
            cluster_config, circuit_breaker=CircuitBreakerConfig(open_threshold=1)
        )
        await cluster_registry.replace(cluster_config)  # "test-cluster"

        user = User(name="testuser", token="testtoken", cluster_name="test-cluster")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        # forcing the cluster to become unavailable
        async with cluster_registry.get(cluster_config.name):
            raise RuntimeError("test")

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "test-cluster"
        assert job.http_host == f"{job.id}.jobs"
        assert job.http_host_named is None

    @pytest.mark.asyncio
    async def test_delete_missing_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)

        user = User(cluster_name="test-cluster", name="testuser", token="testtoken")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        await cluster_registry.remove(cluster_config.name)

        await jobs_service.cancel_job(job.id)
        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.CANCELLED
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_delete_unavail_cluster(
        self,
        cluster_registry: ClusterRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_registry=cluster_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            scheduler=JobsScheduler(JobsSchedulerConfig()),
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_registry.replace(cluster_config)

        async with cluster_registry.get(cluster_config.name) as cluster:

            def _f(*args: Any, **kwargs: Any) -> Exception:
                raise RuntimeError("test")

            cluster.orchestrator.raise_on_delete = True
            cluster.orchestrator.delete_job_exc_factory = _f

        user = User(cluster_name="test-cluster", name="testuser", token="testtoken")
        job, _ = await jobs_service.create_job(mock_job_request, user)

        await jobs_service.cancel_job(job.id)
        await jobs_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.CANCELLED
        assert not record.materialized


class TestJobServiceNotification:
    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_registry: ClusterRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> Callable[..., JobsService]:
        def _factory(deletion_delay_s: int = 0) -> JobsService:
            return JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(deletion_delay_s=deletion_delay_s),
                notifications_client=mock_notifications_client,
                scheduler=JobsScheduler(JobsSchedulerConfig()),
                auth_client=mock_auth_client,
                api_base_url=mock_api_base,
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
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CREATING,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        assert notifications == mock_notifications_client.sent_notifications

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
                prev_transition_time=job.status_history.first.transition_time,
            )
        )
        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_status_update_same_status_will_send_notification(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(job_request=mock_job_request, user=user)

        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CREATING,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

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

        await jobs_service.update_jobs_statuses()

        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_job_failed_errimagepull_workflow(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CREATING,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

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
        user = User(cluster_name="test-cluster", name="testuser", token="")
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user
        )
        notifications = [
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.CREATING,
                description=None,
                exit_code=None,
                prev_status=None,
            )
        ]
        prev_transition_time = job.status_history.current.transition_time

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

        await jobs_service.update_jobs_statuses()

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

    @pytest.mark.asyncio
    async def test_create_job_bad_name(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = User(cluster_name="test-cluster", name="testuser", token="")
        with pytest.raises(JobsServiceException) as cm:
            await jobs_service.create_job(
                job_request=mock_job_request, user=user, job_name="job-name"
            )
        assert (
            str(cm.value)
            == "Failed to create job: job name cannot start with 'job-' prefix."
        )
