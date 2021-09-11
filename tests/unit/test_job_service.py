import asyncio
import base64
import json
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Callable
from unittest import mock

import pytest
from _pytest.logging import LogCaptureFixture
from neuro_auth_client import (
    AuthClient,
    Cluster as AuthCluster,
    Permission,
    Quota as AuthQuota,
    User as AuthUser,
)
from neuro_notifications_client import (
    Client as NotificationsClient,
    JobCannotStartNoCredits,
    JobTransition,
)
from yarl import URL

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterConfigRegistry,
    ClusterHolder,
)
from platform_api.config import JobsConfig, JobsSchedulerConfig
from platform_api.orchestrator.job import (
    Job,
    JobStatusItem,
    JobStatusReason,
    current_datetime_factory,
)
from platform_api.orchestrator.job_request import JobError, JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import (
    NEURO_PASSED_CONFIG,
    JobsService,
    JobsServiceException,
    NoCreditsError,
    RunningJobsQuotaExceededError,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.orchestrator.poller_service import JobsPollerService, JobsScheduler

from .conftest import (
    MockAuthClient,
    MockCluster,
    MockJobsPollerApi,
    MockJobsStorage,
    MockNotificationsClient,
    MockOrchestrator,
)


class MockJobsScheduler(JobsScheduler):
    _now: datetime

    def __init__(self, auth_client: AuthClient) -> None:
        self._now = datetime.now(timezone.utc)
        super().__init__(
            JobsSchedulerConfig(
                is_waiting_min_time_sec=1,
                run_quantum_sec=10,
                max_suspended_time_sec=100,
            ),
            auth_client,
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
    def test_scheduler(self, mock_auth_client: AuthClient) -> MockJobsScheduler:
        return MockJobsScheduler(mock_auth_client)

    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_config_registry: ClusterConfigRegistry,
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
                cluster_config_registry=cluster_config_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(
                    deletion_delay_s=deletion_delay_s,
                    image_pull_error_delay_s=image_pull_error_delay_s,
                ),
                notifications_client=mock_notifications_client,
                auth_client=mock_auth_client,
                api_base_url=mock_api_base,
            )

        return _factory

    @pytest.fixture
    def poller_service_factory(
        self,
        cluster_holder: ClusterHolder,
        mock_jobs_storage: MockJobsStorage,
        test_scheduler: MockJobsScheduler,
        mock_auth_client: AuthClient,
        mock_poller_api: MockJobsPollerApi,
    ) -> Callable[..., JobsPollerService]:
        def _factory(
            deletion_delay_s: int = 0, image_pull_error_delay_s: int = 0
        ) -> JobsPollerService:
            return JobsPollerService(
                cluster_holder=cluster_holder,
                jobs_config=JobsConfig(
                    deletion_delay_s=deletion_delay_s,
                    image_pull_error_delay_s=image_pull_error_delay_s,
                ),
                scheduler=test_scheduler,
                auth_client=mock_auth_client,
                api=mock_poller_api,
            )

        return _factory

    @pytest.fixture
    def jobs_service(
        self, jobs_service_factory: Callable[..., JobsService]
    ) -> JobsService:
        return jobs_service_factory()

    @pytest.fixture
    def jobs_poller_service(
        self, poller_service_factory: Callable[..., JobsPollerService]
    ) -> JobsPollerService:
        return poller_service_factory()

    @pytest.mark.asyncio
    async def test_create_job(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
        )
        assert original_job.status == JobStatus.PENDING
        assert not original_job.is_finished

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.id == original_job.id
        assert job.status == JobStatus.PENDING
        assert job.owner == "testuser"

    @pytest.mark.asyncio
    async def test_create_job_privileged_not_allowed(
        self,
        cluster_config: ClusterConfig,
        cluster_config_registry: ClusterConfigRegistry,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
    ) -> None:
        cluster_config = replace(
            cluster_config,
            orchestrator=replace(
                cluster_config.orchestrator, allow_privileged_mode=False
            ),
        )

        await cluster_config_registry.replace(cluster_config)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        with pytest.raises(
            JobsServiceException,
            match="Cluster test-cluster does not allow privileged jobs",
        ):
            original_job, _ = await jobs_service.create_job(
                job_request=mock_job_request,
                user=user,
                cluster_name="test-cluster",
                privileged=True,
            )

    @pytest.mark.asyncio
    async def test_create_job_privileged_allowed(
        self,
        cluster_config: ClusterConfig,
        cluster_config_registry: ClusterConfigRegistry,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
    ) -> None:
        cluster_config = replace(
            cluster_config,
            orchestrator=replace(
                cluster_config.orchestrator, allow_privileged_mode=True
            ),
        )

        await cluster_config_registry.replace(cluster_config)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            privileged=True,
        )
        assert original_job.privileged

    @pytest.mark.asyncio
    async def test_create_job_pass_config(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_api_base: URL,
        mock_auth_client: MockAuthClient,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )
        assert original_job.status == JobStatus.PENDING
        assert original_job.pass_config
        passed_data_str = original_job.request.container.env[NEURO_PASSED_CONFIG]
        passed_data = json.loads(base64.b64decode(passed_data_str).decode())
        assert URL(passed_data["url"]) == mock_api_base
        assert passed_data["token"] == f"token-{user.name}"
        assert passed_data["cluster"] == original_job.cluster_name
        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client.grants[0] == (
            user.name,
            [Permission(uri=token_uri, action="read")],
        )

    @pytest.mark.asyncio
    async def test_pass_config_revoke_after_complete(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_poller_service.update_jobs_statuses()
        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_after_failure(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        mock_orchestrator.update_status_to_return(JobStatus.FAILED)
        await jobs_poller_service.update_jobs_statuses()
        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_fail_to_start(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        def _f(job: Job) -> Exception:
            raise JobError(f"Bad job {job.id}")

        mock_orchestrator.raise_on_start_job_status = True
        mock_orchestrator.get_job_status_exc_factory = _f
        await jobs_poller_service.update_jobs_statuses()

        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_fail_on_update(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        await jobs_poller_service.update_jobs_statuses()
        mock_orchestrator.raise_on_get_job_status = True
        await jobs_poller_service.update_jobs_statuses()

        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_pass_config_revoke_cluster_unavail(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_auth_client: MockAuthClient,
        mock_orchestrator: MockOrchestrator,
        cluster_holder: ClusterHolder,
        cluster_config: ClusterConfig,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=mock_job_request,
            user=user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        await jobs_poller_service.update_jobs_statuses()

        await cluster_holder.clean()
        await jobs_poller_service.update_jobs_statuses()

        token_uri = f"token://{original_job.cluster_name}/job/{original_job.id}"
        assert mock_auth_client._revokes[0] == (user.name, [token_uri])

    @pytest.mark.asyncio
    async def test_create_job_pass_config_env_present(
        self,
        jobs_service: JobsService,
        mock_job_request: JobRequest,
        mock_api_base: URL,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        mock_job_request.container.env[NEURO_PASSED_CONFIG] = "anything"
        with pytest.raises(
            JobsServiceException,
            match=f"Cannot pass config: ENV '{NEURO_PASSED_CONFIG}' "
            "already specified",
        ):
            await jobs_service.create_job(
                job_request=mock_job_request,
                user=user,
                cluster_name="test-cluster",
                pass_config=True,
            )

    @pytest.mark.asyncio
    async def test_create_job_fail(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_orchestrator: MockOrchestrator,
        caplog: LogCaptureFixture,
    ) -> None:
        def _f(job: Job) -> Exception:
            raise JobError(f"Bad job {job.id}")

        mock_orchestrator.raise_on_start_job_status = True
        mock_orchestrator.get_job_status_exc_factory = _f

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )
        assert job.status == JobStatus.PENDING
        assert not job.is_finished

        assert caplog.text == ""

        await jobs_poller_service.update_jobs_statuses()

        assert f"Failed to start job {job.id}. Reason: Bad job {job.id}" in caplog.text
        assert f"JobError: Bad job {job.id}" in caplog.text
        assert "Unexpected exception in cluster" not in caplog.text

    @pytest.mark.asyncio
    async def test_create_job__name_conflict_with_pending(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(
            request, user=user, cluster_name="test-cluster", job_name=job_name
        )
        assert job_1.status == JobStatus.PENDING
        assert not job_1.is_finished

        with pytest.raises(
            JobsServiceException,
            match=f"job with name '{job_name}' and owner '{user.name}'"
            f" already exists: '{job_1.id}'",
        ):
            job_2, _ = await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster", job_name=job_name
            )

    @pytest.mark.asyncio
    async def test_create_job__name_conflict_with_running(
        self,
        mock_orchestrator: MockOrchestrator,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job_name = "test-Job_name"
        request = job_request_factory()
        job_1, _ = await jobs_service.create_job(
            request, user=user, cluster_name="test-cluster", job_name=job_name
        )
        assert job_1.status == JobStatus.PENDING
        assert job_1.status_history.current.reason == JobStatusReason.CREATING
        assert not job_1.is_finished

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.PENDING
        assert job.status_history.current.reason == JobStatusReason.CONTAINER_CREATING

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.RUNNING

        with pytest.raises(
            JobsServiceException,
            match=f"job with name '{job_name}' and owner '{user.name}'"
            f" already exists: '{job_1.id}'",
        ):
            job_2, _ = await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster", job_name=job_name
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.FAILED, JobStatus.SUCCEEDED]
    )
    async def test_create_job__name_no_conflict_with_another_in_terminal_status(
        self,
        mock_orchestrator: MockOrchestrator,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        job_request_factory: Callable[[], JobRequest],
        first_job_status: JobStatus,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job_name = "test-Job_name"
        request = job_request_factory()

        first_job, _ = await jobs_service.create_job(
            request, user=user, cluster_name="test-cluster", job_name=job_name
        )
        assert first_job.status == JobStatus.PENDING
        assert first_job.status_history.current.reason == JobStatusReason.CREATING
        assert not first_job.is_finished

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=first_job.id)
        assert job.id == first_job.id
        assert job.status == JobStatus.PENDING
        assert job.status_history.current.reason == JobStatusReason.CONTAINER_CREATING

        mock_orchestrator.update_status_to_return(first_job_status)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        second_job, _ = await jobs_service.create_job(
            request, user=user, cluster_name="test-cluster", job_name=job_name
        )
        assert second_job.status == JobStatus.PENDING
        assert not second_job.is_finished

        job = await jobs_service.get_job(job_id=second_job.id)
        assert job.id == second_job.id
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_create_job__transaction_error(
        self,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_jobs_storage.fail_set_job_transaction = True

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job_name = "test-Job_name"

        request = job_request_factory()

        with pytest.raises(
            JobsServiceException, match="Failed to create job: transaction failed"
        ):
            await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster", job_name=job_name
            )

    @pytest.mark.asyncio
    async def test_get_status_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )
        job_status = await jobs_service.get_job_status(job_id=job.id)
        assert job_status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_set_status_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )
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
    async def test_set_materialized_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )

        await jobs_service.set_job_materialized(job.id, True)
        job = await jobs_service.get_job(job.id)
        assert job.materialized

        await jobs_service.set_job_materialized(job.id, False)
        job = await jobs_service.get_job(job.id)
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_max_run_time_by_job_id(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )

        await jobs_service.update_max_run_time(job.id, max_run_time_minutes=10)
        job = await jobs_service.get_job(job.id)
        assert job.max_run_time_minutes == 10

        await jobs_service.update_max_run_time(
            job.id, additional_max_run_time_minutes=15
        )
        job = await jobs_service.get_job(job.id)
        assert job.max_run_time_minutes == 25

    @pytest.mark.asyncio
    async def test_get_all(
        self, jobs_service: JobsService, job_request_factory: Callable[[], JobRequest]
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job_ids = []
        num_jobs = 1000
        for _ in range(num_jobs):
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(
                job_request=job_request, user=user, cluster_name="test-cluster"
            )
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
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])

        async def create_job() -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(
                job_request=job_request, user=user, cluster_name="test-cluster"
            )
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
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        otheruser = AuthUser(
            name="otheruser", clusters=[AuthCluster(name="test-cluster")]
        )

        async def create_job(job_name: str, user: AuthUser) -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(
                job_request=job_request,
                job_name=job_name,
                user=user,
                cluster_name="test-cluster",
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
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])

        async def create_job() -> Job:
            job_request = job_request_factory()
            job, _ = await jobs_service.create_job(
                job_request=job_request, user=user, cluster_name="test-cluster"
            )
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
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_for_deletion(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        mock_orchestrator.raise_on_get_job_status = True
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )

        await jobs_poller_service.update_jobs_statuses()

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

        await jobs_poller_service.update_jobs_statuses()

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
            (JobStatusReason.ERR_IMAGE_PULL, "Image 'testimage' can not be pulled"),
            (
                JobStatusReason.IMAGE_PULL_BACK_OFF,
                "Image 'testimage' can not be pulled",
            ),
            (JobStatusReason.INVALID_IMAGE_NAME, "Invalid image name 'testimage'"),
        ],
    )
    async def test_update_jobs_statuses_pending_errimagepull(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        reason: str,
        description: str,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CONTAINER_CREATING
        assert status_item.description is None

        mock_orchestrator.update_reason_to_return(reason)
        await jobs_poller_service.update_jobs_statuses()

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
        poller_service_factory: Callable[..., JobsPollerService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        reason: str,
    ) -> None:
        jobs_service = jobs_service_factory(image_pull_error_delay_s=60)
        jobs_poller_service = poller_service_factory(image_pull_error_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CONTAINER_CREATING
        assert status_item.description is None

        mock_orchestrator.update_reason_to_return(reason)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        status_item = job.status_history.last
        assert status_item.reason == reason

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_image_errors_cycle(
        self,
        jobs_service_factory: Callable[..., JobsService],
        poller_service_factory: Callable[..., JobsPollerService],
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(image_pull_error_delay_s=0.3)
        jobs_poller_service = poller_service_factory(image_pull_error_delay_s=0.3)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.CONTAINER_CREATING
        assert status_item.description is None

        mock_orchestrator.update_reason_to_return(JobStatusReason.ERR_IMAGE_PULL)
        await jobs_poller_service.update_jobs_statuses()
        await asyncio.sleep(0.1)

        mock_orchestrator.update_reason_to_return(JobStatusReason.IMAGE_PULL_BACK_OFF)
        await jobs_poller_service.update_jobs_statuses()
        await asyncio.sleep(0.1)

        mock_orchestrator.update_reason_to_return(JobStatusReason.ERR_IMAGE_PULL)
        await jobs_poller_service.update_jobs_statuses()
        await asyncio.sleep(0.1)

        mock_orchestrator.update_reason_to_return(JobStatusReason.IMAGE_PULL_BACK_OFF)
        await jobs_poller_service.update_jobs_statuses()
        await asyncio.sleep(0.1)

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized
        status_item = job.status_history.last
        assert status_item.reason == JobStatusReason.COLLECTED
        assert status_item.description == "Image 'testimage' can not be pulled"

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_pending_scale_up(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.FAILED)
        mock_orchestrator.update_reason_to_return(
            JobStatusReason.CLUSTER_SCALE_UP_FAILED
        )
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.FAILED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_statuses_succeeded_missing(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=0)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.is_finished
        assert job.finished_at is None
        assert job.materialized

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(job_id=original_job.id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_handles_running_quota(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        mock_auth_client: MockAuthClient,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(
            name="testuser",
            clusters=[
                AuthCluster("test-cluster", quota=AuthQuota(total_running_jobs=5))
            ],
        )
        mock_auth_client.user_to_return = AuthUser(
            "testuser",
            clusters=[
                AuthCluster("test-cluster", quota=AuthQuota(total_running_jobs=5))
            ],
        )
        jobs = []

        # Start bunch of jobs
        for _ in range(10):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(),
                user=user,
                cluster_name="test-cluster",
                wait_for_jobs_quota=True,
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_poller_service.update_jobs_statuses()

        # Only 5 first should be materialized:
        for job in jobs[:5]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized
        for job in jobs[5:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert not job.materialized

        for job in jobs[:5]:
            mock_orchestrator.update_status_to_return_single(
                job.id, JobStatus.SUCCEEDED
            )

        # Two ticks - first will move remove running from queue,
        # second will start pending
        await jobs_poller_service.update_jobs_statuses()
        await jobs_poller_service.update_jobs_statuses()

        for job in jobs[:5]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUCCEEDED

        for job in jobs[5:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs[5:]:
            mock_orchestrator.update_status_to_return_single(
                job.id, JobStatus.SUCCEEDED
            )

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_update_jobs_scheduled_additional_when_no_pending(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(10):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(),
                user=user,
                cluster_name="test-cluster",
                scheduler_enabled=True,
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        test_scheduler.tick_min_waiting()

        additional_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
            scheduler_enabled=True,
        )

        await jobs_poller_service.update_jobs_statuses()

        # Should try to start new job because there is no waiting jobs
        job = await jobs_service.get_job(additional_job.id)
        assert job.status == JobStatus.PENDING
        assert job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_scheduled_additional_when_has_pending(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(10):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(),
                user=user,
                cluster_name="test-cluster",
                scheduler_enabled=True,
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING

        test_scheduler.tick_min_waiting()

        additional_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
            scheduler_enabled=True,
        )

        await jobs_poller_service.update_jobs_statuses()

        # Should not even try to start this job because there is another waiting jobs
        job = await jobs_service.get_job(additional_job.id)
        assert job.status == JobStatus.PENDING
        assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_scheduled_cycling(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        jobs = []

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        # Start initial bunch of jobs
        for _ in range(9):
            job, _ = await jobs_service.create_job(
                job_request=job_request_factory(),
                user=user,
                cluster_name="test-cluster",
                scheduler_enabled=True,
            )
            assert job.status == JobStatus.PENDING
            jobs.append(job)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING
            assert job.materialized

        for job in jobs[:3]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.PENDING

        test_scheduler.tick_quantum()
        for job in jobs[3:6]:
            mock_orchestrator.update_status_to_return_single(job.id, JobStatus.RUNNING)

        await jobs_poller_service.update_jobs_statuses()

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

        await jobs_poller_service.update_jobs_statuses()

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

        await jobs_poller_service.update_jobs_statuses()

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

        await jobs_poller_service.update_jobs_statuses()

        for job in jobs[:3]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.RUNNING
            assert job.materialized

        for job in jobs[3:]:
            job = await jobs_service.get_job(job.id)
            assert job.status == JobStatus.SUSPENDED
            assert not job.materialized

    @pytest.mark.asyncio
    async def test_update_jobs_scheduled_max_suspended_time(
        self,
        jobs_service_factory: Callable[..., JobsService],
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        test_scheduler: MockJobsScheduler,
    ) -> None:
        jobs_service = jobs_service_factory(deletion_delay_s=60)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])

        # Synchronize time
        mock_orchestrator.current_datetime_factory = (
            test_scheduler.current_datetime_factory
        )

        job1, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
            scheduler_enabled=True,
        )
        assert job1.status == JobStatus.PENDING

        job2, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
            scheduler_enabled=True,
        )
        assert job1.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        mock_orchestrator.update_status_to_return_single(job1.id, JobStatus.RUNNING)

        await jobs_poller_service.update_jobs_statuses()

        job1 = await jobs_service.get_job(job1.id)
        assert job1.status == JobStatus.RUNNING
        assert job1.materialized

        job2 = await jobs_service.get_job(job2.id)
        assert job2.status == JobStatus.PENDING
        assert job2.materialized

        test_scheduler.tick_min_waiting()

        job3, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
            scheduler_enabled=True,
        )
        assert job3.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        job3 = await jobs_service.get_job(job3.id)
        assert job3.status == JobStatus.PENDING
        assert not job3.materialized

        test_scheduler.tick_max_suspended()

        await jobs_poller_service.update_jobs_statuses()

        job3 = await jobs_service.get_job(job3.id)
        assert job3.status == JobStatus.PENDING
        assert job3.materialized

    @pytest.mark.asyncio
    async def test_cancel_running(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        jobs_poller_service: JobsPollerService,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(),
            user=user,
            cluster_name="test-cluster",
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

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
        jobs_poller_service: JobsPollerService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_service = jobs_service_factory(3600 * 7)  # Set huge deletion timeout
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        original_job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        assert original_job.status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        await jobs_service.cancel_job(original_job.id)

        await jobs_poller_service.update_jobs_statuses()

        job = await jobs_service.get_job(original_job.id)
        assert job.status == JobStatus.CANCELLED
        assert job.is_finished
        assert job.finished_at
        assert not job.materialized

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "quota",
        [
            AuthQuota(),
            AuthQuota(credits=Decimal("100")),
        ],
    )
    async def test_create_job_has_credits(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        quota: AuthQuota,
    ) -> None:
        user = AuthUser(
            name="testuser", clusters=[AuthCluster(name="test-cluster", quota=quota)]
        )
        request = job_request_factory()

        job, _ = await jobs_service.create_job(
            request, user, cluster_name="test-cluster"
        )
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "quota",
        [
            AuthQuota(credits=Decimal("0")),
            AuthQuota(credits=Decimal("-0.5")),
        ],
    )
    async def test_raise_no_credits(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[..., JobRequest],
        quota: AuthQuota,
    ) -> None:
        user = AuthUser(
            name="testuser", clusters=[AuthCluster(name="test-cluster", quota=quota)]
        )
        request = job_request_factory(with_gpu=True)

        with pytest.raises(
            NoCreditsError, match=f"No credits left for user '{user.name}'"
        ):
            await jobs_service.create_job(request, user, cluster_name="test-cluster")

    @pytest.mark.asyncio
    async def test_raise_for_jobs_limit(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[..., JobRequest],
    ) -> None:
        user = AuthUser(
            name="testuser",
            clusters=[
                AuthCluster(
                    name="test-cluster",
                    quota=AuthQuota(total_running_jobs=5),
                )
            ],
        )
        for _ in range(5):
            request = job_request_factory()
            await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster"
            )

        request = job_request_factory()

        with pytest.raises(RunningJobsQuotaExceededError):
            await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster"
            )

    @pytest.mark.asyncio
    async def test_no_raise_for_jobs_limit_if_wait_flag(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[..., JobRequest],
    ) -> None:
        user = AuthUser(
            name="testuser",
            clusters=[
                AuthCluster(
                    name="test-cluster",
                    quota=AuthQuota(total_running_jobs=5),
                )
            ],
        )
        for _ in range(5):
            request = job_request_factory()
            await jobs_service.create_job(
                request, user=user, cluster_name="test-cluster"
            )

        request = job_request_factory()

        # Should not raise anything:
        await jobs_service.create_job(
            request, user=user, cluster_name="test-cluster", wait_for_jobs_quota=True
        )

    @pytest.mark.asyncio
    async def test_job_billing_defaults(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )
        assert not job.fully_billed
        assert job.last_billed is None
        assert job.total_price_credits == Decimal("0")

    @pytest.mark.asyncio
    async def test_job_update_billing(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
        )
        now = datetime.now(timezone.utc)
        await jobs_service.update_job_billing(
            job.id, last_billed=now, fully_billed=False, new_charge=Decimal("5.00")
        )

        await jobs_service.update_job_billing(
            job.id, last_billed=now, fully_billed=True, new_charge=Decimal("6.11")
        )
        job = await jobs_service.get_job(job.id)
        assert job.fully_billed
        assert job.last_billed == now
        assert job.total_price_credits == Decimal("11.11")

    @pytest.mark.asyncio
    async def test_get_not_billed_jobs(
        self, jobs_service: JobsService, job_request_factory: Callable[..., JobRequest]
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job1, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        job2, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
        )
        now = datetime.now(timezone.utc)
        await jobs_service.update_job_billing(
            job1.id, last_billed=now, fully_billed=True, new_charge=Decimal("5.00")
        )
        async with jobs_service.get_not_billed_jobs() as it:
            job_ids = [job.id async for job in it]
        assert job_ids == [job2.id]


class TestJobsServiceCluster:
    @pytest.fixture
    async def cluster_holder(self) -> AsyncIterator[ClusterHolder]:
        def _cluster_factory(config: ClusterConfig) -> Cluster:
            orchestrator = MockOrchestrator(config)
            return MockCluster(config, orchestrator)

        async with ClusterHolder(factory=_cluster_factory) as registry:
            yield registry

    @pytest.fixture
    async def cluster_config_registry(self) -> ClusterConfigRegistry:
        return ClusterConfigRegistry()

    @pytest.fixture
    def jobs_service(
        self,
        cluster_config_registry: ClusterConfigRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> JobsService:
        return JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )

    @pytest.mark.asyncio
    async def test_create_job_missing_cluster(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="missing")])

        with pytest.raises(JobsServiceException, match="Cluster 'missing' not found"):
            await jobs_service.create_job(
                mock_job_request, user=user, cluster_name="missing"
            )

    @pytest.mark.asyncio
    async def test_create_job_user_cluster_name_fallback(
        self, jobs_service: JobsService, mock_job_request: JobRequest
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])

        with pytest.raises(
            JobsServiceException, match="Cluster 'test-cluster' not found"
        ):
            await jobs_service.create_job(
                mock_job_request, user=user, cluster_name="test-cluster"
            )

    @pytest.mark.asyncio
    async def test_update_pending_job_missing_cluster(
        self,
        cluster_holder: ClusterHolder,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        jobs_poller_service = JobsPollerService(
            cluster_holder=cluster_holder,
            jobs_config=jobs_config,
            scheduler=JobsScheduler(JobsSchedulerConfig(), mock_auth_client),
            auth_client=mock_auth_client,
            api=MockJobsPollerApi(jobs_service, mock_jobs_storage),
        )
        await cluster_holder.update(cluster_config)
        await cluster_config_registry.replace(cluster_config)

        async with cluster_holder.get() as cluster:

            def _f(*args: Any, **kwargs: Any) -> Exception:
                raise RuntimeError("test")

            assert isinstance(cluster.orchestrator, MockOrchestrator)
            cluster.orchestrator.raise_on_get_job_status = True
            cluster.orchestrator.get_job_status_exc_factory = _f

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="test-cluster"
        )

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

        await jobs_poller_service.update_jobs_statuses()

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_update_pending_job_unavail_cluster(
        self,
        cluster_holder: ClusterHolder,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        jobs_poller_service = JobsPollerService(
            cluster_holder=cluster_holder,
            jobs_config=jobs_config,
            scheduler=JobsScheduler(JobsSchedulerConfig(), mock_auth_client),
            auth_client=mock_auth_client,
            api=MockJobsPollerApi(jobs_service, mock_jobs_storage),
        )
        await cluster_holder.update(cluster_config)
        await cluster_config_registry.replace(cluster_config)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="test-cluster"
        )

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.PENDING

        await cluster_holder.clean()
        cluster_config_registry.remove(cluster_config.name)

        await jobs_poller_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status_history.current == JobStatusItem.create(
            JobStatus.FAILED,
            reason=JobStatusReason.CLUSTER_NOT_FOUND,
            description="Cluster is not present",
        )
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_update_succeeded_job_missing_cluster(
        self,
        cluster_holder: ClusterHolder,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        jobs_poller_service = JobsPollerService(
            cluster_holder=cluster_holder,
            jobs_config=jobs_config,
            scheduler=JobsScheduler(JobsSchedulerConfig(), mock_auth_client),
            auth_client=mock_auth_client,
            api=MockJobsPollerApi(jobs_service, mock_jobs_storage),
        )
        await cluster_holder.update(cluster_config)
        await cluster_config_registry.replace(cluster_config)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="test-cluster"
        )

        async with mock_jobs_storage.try_update_job(job.id) as record:
            record.status = JobStatus.SUCCEEDED

        status = await jobs_service.get_job_status(job.id)
        assert status == JobStatus.SUCCEEDED

        await cluster_holder.clean()
        cluster_config_registry.remove(cluster_config.name)

        await jobs_poller_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.SUCCEEDED
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_get_job_fallback(
        self,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        await cluster_config_registry.replace(cluster_config)  # "test-cluster"
        await cluster_config_registry.replace(replace(cluster_config, name="default"))
        await cluster_config_registry.replace(replace(cluster_config, name="missing"))

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="missing")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="missing"
        )
        assert job.cluster_name == "missing"

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"

        cluster_config_registry.remove("missing")

        job = await jobs_service.get_job(job.id)
        assert job.cluster_name == "missing"
        assert job.http_host == f"{job.id}.missing-cluster"
        assert job.http_host_named is None

    @pytest.mark.asyncio
    async def test_delete_missing_cluster(
        self,
        cluster_holder: ClusterHolder,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        jobs_poller_service = JobsPollerService(
            cluster_holder=cluster_holder,
            jobs_config=jobs_config,
            scheduler=JobsScheduler(JobsSchedulerConfig(), mock_auth_client),
            auth_client=mock_auth_client,
            api=MockJobsPollerApi(jobs_service, mock_jobs_storage),
        )
        await cluster_holder.update(cluster_config)
        await cluster_config_registry.replace(cluster_config)

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="test-cluster"
        )

        await cluster_holder.clean()
        cluster_config_registry.remove(cluster_config.name)

        await jobs_service.cancel_job(job.id)
        await jobs_poller_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.CANCELLED
        assert not record.materialized

    @pytest.mark.asyncio
    async def test_delete_unavail_cluster(
        self,
        cluster_holder: ClusterHolder,
        cluster_config_registry: ClusterConfigRegistry,
        cluster_config: ClusterConfig,
        mock_jobs_storage: MockJobsStorage,
        mock_job_request: JobRequest,
        jobs_config: JobsConfig,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> None:
        jobs_service = JobsService(
            cluster_config_registry=cluster_config_registry,
            jobs_storage=mock_jobs_storage,
            jobs_config=jobs_config,
            notifications_client=mock_notifications_client,
            auth_client=mock_auth_client,
            api_base_url=mock_api_base,
        )
        jobs_poller_service = JobsPollerService(
            cluster_holder=cluster_holder,
            jobs_config=jobs_config,
            scheduler=JobsScheduler(JobsSchedulerConfig(), mock_auth_client),
            auth_client=mock_auth_client,
            api=MockJobsPollerApi(jobs_service, mock_jobs_storage),
        )
        await cluster_holder.update(cluster_config)
        await cluster_config_registry.replace(cluster_config)

        async with cluster_holder.get() as cluster:

            def _f(*args: Any, **kwargs: Any) -> Exception:
                raise RuntimeError("test")

            assert isinstance(cluster.orchestrator, MockOrchestrator)
            cluster.orchestrator.raise_on_delete = True
            cluster.orchestrator.delete_job_exc_factory = _f

        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            mock_job_request, user=user, cluster_name="test-cluster"
        )

        await jobs_service.cancel_job(job.id)
        await jobs_poller_service.update_jobs_statuses()

        record = await mock_jobs_storage.get_job(job.id)
        assert record.status == JobStatus.CANCELLED
        assert not record.materialized


class TestJobServiceNotification:
    @pytest.fixture
    def jobs_service_factory(
        self,
        cluster_config_registry: ClusterConfigRegistry,
        mock_jobs_storage: MockJobsStorage,
        mock_notifications_client: NotificationsClient,
        mock_auth_client: AuthClient,
        mock_api_base: URL,
    ) -> Callable[..., JobsService]:
        def _factory(deletion_delay_s: int = 0) -> JobsService:
            return JobsService(
                cluster_config_registry=cluster_config_registry,
                jobs_storage=mock_jobs_storage,
                jobs_config=JobsConfig(deletion_delay_s=deletion_delay_s),
                notifications_client=mock_notifications_client,
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
    async def test_no_credits(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = AuthUser(
            name="testuser",
            clusters=[
                AuthCluster(name="test-cluster", quota=AuthQuota(credits=Decimal("0")))
            ],
        )

        with pytest.raises(NoCreditsError):
            await jobs_service.create_job(
                job_request=mock_job_request, user=user, cluster_name="test-cluster"
            )

        assert mock_notifications_client.sent_notifications == [
            JobCannotStartNoCredits(
                user_id=user.name,
                cluster_name="test-cluster",
            )
        ]

    @pytest.mark.asyncio
    async def test_new_job_created(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
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
        assert notifications == mock_notifications_client.sent_notifications

        await jobs_poller_service.update_jobs_statuses()
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
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        mock_job_request: JobRequest,
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=mock_job_request, user=user, cluster_name="test-cluster"
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

        await jobs_poller_service.update_jobs_statuses()
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

        await jobs_poller_service.update_jobs_statuses()

        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_job_failed_errimagepull_workflow(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
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
        assert notifications == mock_notifications_client.sent_notifications
        prev_transition_time = job.status_history.current.transition_time

        await jobs_poller_service.update_jobs_statuses()
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
        prev_transition_time = job.status_history.current.transition_time

        mock_orchestrator.update_reason_to_return(JobStatusReason.ERR_IMAGE_PULL)
        await jobs_poller_service.update_jobs_statuses()
        job = await jobs_service.get_job(job.id)

        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.PENDING,
                transition_time=mock.ANY,
                reason=JobStatusReason.ERR_IMAGE_PULL,
                description=None,
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=prev_transition_time,
            )
        )
        notifications.append(
            JobTransition(
                job_id=job.id,
                status=JobStatus.FAILED,
                transition_time=job.status_history.current.transition_time,
                reason=JobStatusReason.COLLECTED,
                description="Image 'testimage' can not be pulled",
                exit_code=None,
                prev_status=JobStatus.PENDING,
                prev_transition_time=mock.ANY,
            )
        )

        assert notifications == mock_notifications_client.sent_notifications

    @pytest.mark.asyncio
    async def test_job_succeeded_workflow(
        self,
        jobs_service: JobsService,
        jobs_poller_service: JobsPollerService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
        mock_notifications_client: MockNotificationsClient,
    ) -> None:
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        job, _ = await jobs_service.create_job(
            job_request=job_request_factory(), user=user, cluster_name="test-cluster"
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

        await jobs_poller_service.update_jobs_statuses()
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

        await jobs_poller_service.update_jobs_statuses()

        mock_orchestrator.update_status_to_return(JobStatus.RUNNING)
        mock_orchestrator.update_reason_to_return(None)
        await jobs_poller_service.update_jobs_statuses()
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
        await jobs_poller_service.update_jobs_statuses()

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
        user = AuthUser(name="testuser", clusters=[AuthCluster(name="test-cluster")])
        with pytest.raises(JobsServiceException) as cm:
            await jobs_service.create_job(
                job_request=mock_job_request,
                user=user,
                cluster_name="test-cluster",
                job_name="job-name",
            )
        assert (
            str(cm.value)
            == "Failed to create job: job name cannot start with 'job-' prefix."
        )
