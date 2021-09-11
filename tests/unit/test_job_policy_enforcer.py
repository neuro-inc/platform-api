import asyncio
import datetime
import logging
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
)

import pytest
from neuro_auth_client import (
    Cluster as AuthCluster,
    Quota as AuthQuota,
    User as AuthUser,
)
from neuro_notifications_client import CreditsWillRunOutSoon
from yarl import URL

from platform_api.cluster import ClusterConfigRegistry
from platform_api.cluster_config import ClusterConfig
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.billing_log.service import BillingLogService
from platform_api.orchestrator.billing_log.storage import (
    BillingLogEntry,
    BillingLogStorage,
    BillingLogSyncRecord,
    InMemoryBillingLogStorage,
)
from platform_api.orchestrator.job import Job, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_policy_enforcer import (
    BillingEnforcer,
    CreditsLimitEnforcer,
    CreditsNotificationsEnforcer,
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
    RetentionPolicyEnforcer,
    RuntimeLimitEnforcer,
    StopOnClusterRemoveEnforcer,
)
from platform_api.orchestrator.job_request import JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.utils.update_notifier import InMemoryNotifier
from tests.unit.conftest import MockAuthClient, MockNotificationsClient


_EnforcePollingRunner = Callable[
    [JobPolicyEnforcer], AsyncContextManager[JobPolicyEnforcePoller]
]


@pytest.fixture
def job_policy_enforcer_config() -> JobPolicyEnforcerConfig:
    return JobPolicyEnforcerConfig(
        platform_api_url=URL("http://localhost:8080"),
        token="admin-token",
        interval_sec=0.1,
    )


class TestRuntimeLimitEnforcer:
    @pytest.mark.asyncio
    async def test_enforce_nothing_killed(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = RuntimeLimitEnforcer(jobs_service)
        await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        await jobs_service.create_job(
            job_request_factory(),
            test_user,
            max_run_time_minutes=1,
            cluster_name="test-cluster",
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(),
            test_user,
            max_run_time_minutes=5,
            cluster_name="test-cluster",
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        before_2_mins = now - datetime.timedelta(minutes=2)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.RUNNING, transition_time=before_2_mins)
        )
        await enforcer.enforce()
        cancelled = await jobs_service.get_all_jobs(
            JobFilter(statuses={JobStatus.CANCELLED})
        )
        assert cancelled == []

    @pytest.mark.asyncio
    async def test_enforce_killed(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = RuntimeLimitEnforcer(jobs_service)
        job, _ = await jobs_service.create_job(
            job_request_factory(),
            test_user,
            cluster_name="test-cluster",
            max_run_time_minutes=1,
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        before_2_mins = now - datetime.timedelta(minutes=2)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.RUNNING, transition_time=before_2_mins)
        )
        await enforcer.enforce()
        job = await jobs_service.get_job(job.id)
        assert job.status == JobStatus.CANCELLED


class MockedJobPolicyEnforcer(JobPolicyEnforcer):
    def __init__(
        self, *, raise_exception: bool = False, enforce_time_sec: float = 0
    ) -> None:
        self._raise_exception = raise_exception
        self._enforce_time_sec = enforce_time_sec
        self._called_times: int = 0

    @property
    def called_times(self) -> int:
        return self._called_times

    @property
    def enforce_time_sec(self) -> float:
        return self._enforce_time_sec

    async def enforce(self) -> None:
        self._called_times += 1
        await asyncio.sleep(self._enforce_time_sec)
        if self._raise_exception:
            raise RuntimeError("exception in job policy enforcer")


class TestJobPolicyEnforcePoller:
    @pytest.fixture
    async def run_enforce_polling(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> Callable[[JobPolicyEnforcer], AsyncContextManager[JobPolicyEnforcePoller]]:
        @asynccontextmanager
        async def _factory(
            enforcer: JobPolicyEnforcer,
        ) -> AsyncIterator[JobPolicyEnforcePoller]:
            async with JobPolicyEnforcePoller(
                config=job_policy_enforcer_config, enforcers=[enforcer]
            ) as poller:
                yield poller

        return _factory

    @pytest.mark.asyncio
    async def test_basic_no_exception_short_response(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=False, enforce_time_sec=interval * 0.1
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 2

    @pytest.mark.asyncio
    async def test_basic_exception_thrown_short_response(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval * 0.1
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 2

    @pytest.mark.asyncio
    async def test_basic_no_exception_long_enforce(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=False, enforce_time_sec=interval * 2
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 1

    @pytest.mark.asyncio
    async def test_basic_exception_thrown_long_enforce(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval * 2
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 1

    @pytest.mark.asyncio
    async def test_concurrent_call_not_allowed(
        self,
        run_enforce_polling: _EnforcePollingRunner,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval
        )
        async with run_enforce_polling(enforcer) as poller:
            with pytest.raises(
                RuntimeError, match="Concurrent usage of enforce poller not allowed"
            ):
                async with poller:
                    pass


class TestHasCreditsEnforcer:
    @pytest.fixture()
    def has_credits_enforcer(
        self, jobs_service: JobsService, mock_auth_client: MockAuthClient
    ) -> CreditsLimitEnforcer:
        return CreditsLimitEnforcer(jobs_service, mock_auth_client)

    @pytest.fixture()
    def make_jobs(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> Callable[[AuthUser, int], Awaitable[List[Job]]]:
        async def _make_jobs(user: AuthUser, count: int) -> List[Job]:
            return [
                (
                    await jobs_service.create_job(
                        job_request_factory(), user, cluster_name="test-cluster"
                    )
                )[0]
                for _ in range(count)
            ]

        return _make_jobs

    @pytest.fixture()
    def check_not_cancelled(
        self, jobs_service: JobsService
    ) -> Callable[[Iterable[Job]], Awaitable[None]]:
        async def _check(jobs: Iterable[Job]) -> None:
            for job in jobs:
                job = await jobs_service.get_job(job.id)
                assert job.status != JobStatus.CANCELLED

        return _check

    @pytest.fixture()
    def check_cancelled(
        self, jobs_service: JobsService
    ) -> Callable[[Iterable[Job]], Awaitable[None]]:
        async def _check(jobs: Iterable[Job], reason: Optional[str] = None) -> None:
            for job in jobs:
                job = await jobs_service.get_job(job.id)
                assert job.status == JobStatus.CANCELLED
                assert job.status_history.current.reason == reason

        return _check

    def make_auth_user(
        self, user: AuthUser, cluster_credits: Mapping[str, Optional[Decimal]]
    ) -> AuthUser:
        return AuthUser(
            name=user.name,
            clusters=[
                AuthCluster(cluster_name, quota=AuthQuota(credits=credits))
                for cluster_name, credits in cluster_credits.items()
            ],
        )

    @pytest.mark.asyncio
    async def test_user_credits_disabled_do_nothing(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, int], Awaitable[List[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
    ) -> None:
        jobs = await make_jobs(test_user, 5)

        mock_auth_client.user_to_return = self.make_auth_user(
            test_user, {"test-cluster": None}
        )

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    @pytest.mark.asyncio
    async def test_user_has_credits_do_nothing(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, int], Awaitable[List[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
    ) -> None:
        jobs = await make_jobs(test_user, 5)

        mock_auth_client.user_to_return = self.make_auth_user(
            test_user, {"test-cluster": Decimal("1.00")}
        )

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    @pytest.mark.parametrize("credits", [Decimal("0"), Decimal("-0.5")])
    @pytest.mark.asyncio
    async def test_user_has_no_credits_kill_all(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, int], Awaitable[List[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
        credits: Decimal,
    ) -> None:
        jobs = await make_jobs(test_user, 5)

        mock_auth_client.user_to_return = self.make_auth_user(
            test_user, {"test-cluster": credits}
        )

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)

    @pytest.mark.asyncio
    async def test_user_has_no_access_to_cluster_kill_all(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, int], Awaitable[List[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
    ) -> None:
        jobs = await make_jobs(test_user, 5)

        mock_auth_client.user_to_return = self.make_auth_user(
            test_user, {"test-another-cluster": None}
        )

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)


class TestBillingEnforcer:
    @pytest.fixture()
    def billing_log_storage(self) -> BillingLogStorage:
        return InMemoryBillingLogStorage()

    @pytest.fixture()
    async def billing_service(
        self, billing_log_storage: BillingLogStorage
    ) -> AsyncIterator[BillingLogService]:
        async with BillingLogService(
            storage=billing_log_storage,
            new_entry=InMemoryNotifier(),
            entry_done=InMemoryNotifier(),
        ) as service:
            yield service

    @pytest.mark.asyncio
    async def test_jobs_charged(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        cluster_config: ClusterConfig,
        billing_service: BillingLogService,
        billing_log_storage: BillingLogStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = BillingEnforcer(jobs_service, billing_service)
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        before_1_5_hour = now - datetime.timedelta(hours=1, minutes=30)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.RUNNING, transition_time=before_1_5_hour)
        )

        per_hour = cluster_config.orchestrator.presets[0].credits_per_hour
        second = Decimal("1") / 3600
        await enforcer.enforce()
        async with billing_log_storage.iter_entries() as it:
            entries = [entry async for entry in it]
        assert len(entries) == 1
        assert entries[0].job_id == job.id
        assert entries[0].charge >= Decimal("1.5") * per_hour
        assert entries[0].charge <= (Decimal("1.5") + second) * per_hour
        assert not entries[0].fully_billed

    @pytest.mark.asyncio
    async def test_idempotency_key_unique(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        cluster_config: ClusterConfig,
        billing_service: BillingLogService,
        billing_log_storage: BillingLogStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = BillingEnforcer(jobs_service, billing_service)
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.RUNNING, transition_time=now)
        )
        await billing_log_storage.get_or_create_sync_record()
        for index in range(1000):
            await enforcer.enforce()
            await billing_log_storage.update_sync_record(
                BillingLogSyncRecord(index + 1)
            )
            await billing_service._entry_done_notifier.notify()
        async with billing_log_storage.iter_entries() as it:
            keys = {entry.idempotency_key async for entry in it}
        assert len(keys) == 1000

    @pytest.mark.asyncio
    async def test_jobs_charged_fully(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        cluster_config: ClusterConfig,
        billing_service: BillingLogService,
        billing_log_storage: BillingLogStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = BillingEnforcer(jobs_service, billing_service)
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.SUCCEEDED, now)
        )

        await enforcer.enforce()
        async with billing_log_storage.iter_entries() as it:
            entries = [entry async for entry in it]
        assert len(entries) == 1
        assert entries[0].fully_billed

    @pytest.mark.asyncio
    async def test_waits_for_previous_entry(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        cluster_config: ClusterConfig,
        billing_service: BillingLogService,
        billing_log_storage: BillingLogStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = BillingEnforcer(jobs_service, billing_service)
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        now = datetime.datetime.now(datetime.timezone.utc)
        await jobs_service.set_job_status(
            job.id, JobStatusItem(JobStatus.RUNNING, transition_time=now)
        )
        entry = BillingLogEntry(
            job_id=job.id,
            charge=Decimal(1),
            fully_billed=False,
            idempotency_key="key",
            last_billed=now,
        )
        async with billing_service.entries_inserter() as inserter:
            await inserter.insert([entry])
        # Should not proceed if there is pending item
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(enforcer.enforce(), timeout=0.2)
        # Should unblock and proceed when item is done
        task = asyncio.create_task(enforcer.enforce())
        await jobs_service.update_job_billing(
            job_id=entry.job_id,
            last_billed=entry.last_billed,
            fully_billed=entry.fully_billed,
            new_charge=entry.charge,
        )
        await asyncio.sleep(1)
        await billing_log_storage.get_or_create_sync_record()
        await billing_log_storage.update_sync_record(BillingLogSyncRecord(1))
        await billing_service._entry_done_notifier.notify()
        await asyncio.wait_for(task, timeout=0.2)
        async with billing_log_storage.iter_entries() as it:
            entries = [entry async for entry in it]
        assert len(entries) == 2

        per_hour = cluster_config.orchestrator.presets[0].credits_per_hour
        second = Decimal("1") / 3600

        assert entries[1].charge >= second * per_hour
        assert entries[1].charge <= 2 * second * per_hour
        assert not entries[1].fully_billed

        delta = entries[1].last_billed - entries[0].last_billed
        assert (
            int(delta.total_seconds() * 1e6) / Decimal(1e6) / 3600 * per_hour
            == entries[1].charge
        )


class TestCreditsNotificationEnforcer:
    @pytest.mark.asyncio
    async def test_credits_almost_run_out_user_notified(
        self,
        jobs_service: JobsService,
        mock_auth_client: MockAuthClient,
        mock_notifications_client: MockNotificationsClient,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = AuthUser(
            name="test_user",
            clusters=[
                AuthCluster(name="test-cluster", quota=AuthQuota(credits=Decimal("10")))
            ],
        )

        enforcer = CreditsNotificationsEnforcer(
            jobs_service,
            mock_auth_client,
            mock_notifications_client,
            notification_threshold=Decimal("2000"),
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(), user, cluster_name="test-cluster"
        )
        mock_auth_client.user_to_return = user
        await enforcer.enforce()
        assert (
            CreditsWillRunOutSoon(
                user_id=user.name,
                cluster_name="test-cluster",
                credits=Decimal("10"),
            )
            in mock_notifications_client.sent_notifications
        )

    @pytest.mark.asyncio
    async def test_no_credits_not_notified(
        self,
        jobs_service: JobsService,
        mock_auth_client: MockAuthClient,
        mock_notifications_client: MockNotificationsClient,
        job_request_factory: Callable[[], JobRequest],
        caplog: Any,
    ) -> None:
        user = AuthUser(
            name="test_user",
            clusters=[AuthCluster(name="test-cluster", quota=AuthQuota(credits=None))],
        )

        enforcer = CreditsNotificationsEnforcer(
            jobs_service,
            mock_auth_client,
            mock_notifications_client,
            notification_threshold=Decimal("2000"),
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(), user, cluster_name="test-cluster"
        )
        mock_auth_client.user_to_return = user
        await enforcer.enforce()
        assert not any(
            isinstance(notification, CreditsWillRunOutSoon)
            for notification in mock_notifications_client.sent_notifications
        )
        assert not any(record.levelno >= logging.ERROR for record in caplog.records), [
            record for record in caplog.records
        ]


class TestStopOnClusterRemoveEnforcer:
    @pytest.mark.asyncio
    async def test_job_untouched_by_default(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        mock_auth_client: MockAuthClient,
        cluster_config_registry: ClusterConfigRegistry,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = StopOnClusterRemoveEnforcer(
            jobs_service, cluster_config_registry, mock_auth_client
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_job_removed_cluster_gone(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        mock_auth_client: MockAuthClient,
        cluster_config_registry: ClusterConfigRegistry,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = StopOnClusterRemoveEnforcer(
            jobs_service, cluster_config_registry, mock_auth_client
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        cluster_config_registry.remove("test-cluster")
        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert job.status == JobStatus.FAILED
        assert job.status_history.current.reason == JobStatusReason.CLUSTER_NOT_FOUND

    @pytest.mark.asyncio
    async def test_job_with_pass_config_removed_cluster_gone(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        mock_auth_client: MockAuthClient,
        cluster_config_registry: ClusterConfigRegistry,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = StopOnClusterRemoveEnforcer(
            jobs_service, cluster_config_registry, mock_auth_client
        )
        job, _ = await jobs_service.create_job(
            job_request_factory(),
            test_user,
            cluster_name="test-cluster",
            pass_config=True,
        )

        cluster_config_registry.remove("test-cluster")
        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert job.status == JobStatus.FAILED
        assert job.status_history.current.reason == JobStatusReason.CLUSTER_NOT_FOUND
        token_uri = f"token://{job.cluster_name}/job/{job.id}"
        assert mock_auth_client._revokes[0] == (job.owner, [token_uri])


class TestRetentionPolicyEnforcer:
    @pytest.mark.asyncio
    async def test_job_untouched_by_default(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = RetentionPolicyEnforcer(jobs_service, datetime.timedelta(days=1))
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_job_not_marked_to_drop_if_delay_is_smaller(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = RetentionPolicyEnforcer(jobs_service, datetime.timedelta(days=3))
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        await jobs_service.set_job_status(
            job.id,
            JobStatusItem(
                JobStatus.SUCCEEDED, transition_time=now - datetime.timedelta(days=2)
            ),
        )
        await jobs_service.set_job_materialized(job.id, False)

        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert not job.being_dropped

    @pytest.mark.asyncio
    async def test_job_marked_to_drop(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = RetentionPolicyEnforcer(jobs_service, datetime.timedelta(days=1))
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        now = datetime.datetime.now(datetime.timezone.utc)
        await jobs_service.set_job_status(
            job.id,
            JobStatusItem(
                JobStatus.SUCCEEDED, transition_time=now - datetime.timedelta(days=2)
            ),
        )
        await jobs_service.set_job_materialized(job.id, False)

        job = await jobs_service.get_job(job.id)
        assert not job.being_dropped

        await enforcer.enforce()

        job = await jobs_service.get_job(job.id)
        assert job.being_dropped
