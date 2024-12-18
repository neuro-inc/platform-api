import asyncio
import datetime
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from typing import Optional

import pytest
from neuro_admin_client import AdminClient, Balance, Quota
from neuro_auth_client import User as AuthUser
from yarl import URL

from platform_api.cluster import ClusterConfigRegistry
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import Job, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_policy_enforcer import (
    CreditsLimitEnforcer,
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
    RetentionPolicyEnforcer,
    RuntimeLimitEnforcer,
    StopOnClusterRemoveEnforcer,
)
from platform_api.orchestrator.job_request import JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
from tests.unit.conftest import (
    MockAdminClient,
    MockAuthClient,
    OrgFactory,
    UserFactory,
)

_EnforcePollingRunner = Callable[
    [JobPolicyEnforcer], AbstractAsyncContextManager[JobPolicyEnforcePoller]
]


@pytest.fixture
def job_policy_enforcer_config() -> JobPolicyEnforcerConfig:
    return JobPolicyEnforcerConfig(
        platform_api_url=URL("http://localhost:8080"),
        interval_sec=0.1,
    )


class TestRuntimeLimitEnforcer:
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
    ) -> Callable[
        [JobPolicyEnforcer], AbstractAsyncContextManager[JobPolicyEnforcePoller]
    ]:
        @asynccontextmanager
        async def _factory(
            enforcer: JobPolicyEnforcer,
        ) -> AsyncIterator[JobPolicyEnforcePoller]:
            async with JobPolicyEnforcePoller(
                config=job_policy_enforcer_config, enforcers=[enforcer]
            ) as poller:
                yield poller

        return _factory

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
        self, jobs_service: JobsService, mock_admin_client: AdminClient
    ) -> CreditsLimitEnforcer:
        return CreditsLimitEnforcer(jobs_service, mock_admin_client)

    @pytest.fixture()
    def make_jobs(
        self,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
    ) -> Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]]:
        async def _make_jobs(
            user: AuthUser, org_name: Optional[str], count: int
        ) -> list[Job]:
            return [
                (
                    await jobs_service.create_job(
                        job_request_factory(),
                        user,
                        cluster_name="test-cluster",
                        org_name=org_name,
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

    async def test_user_credits_disabled_do_nothing(
        self,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
        user_factory: UserFactory,
        test_cluster: str,
    ) -> None:
        user = await user_factory("some-user", [(test_cluster, Balance(), Quota())])
        jobs = await make_jobs(user, None, 5)

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    async def test_user_has_credits_do_nothing(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
        user_factory: UserFactory,
        test_cluster: str,
    ) -> None:
        user = await user_factory(
            "some-user", [(test_cluster, Balance(credits=Decimal("1.00")), Quota())]
        )
        jobs = await make_jobs(user, None, 5)

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    @pytest.mark.parametrize("credits", [Decimal("0"), Decimal("-0.5")])
    async def test_user_has_no_credits_kill_all(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
        credits: Decimal,
        mock_admin_client: MockAdminClient,
    ) -> None:
        jobs = await make_jobs(test_user, None, 5)
        old_cluster_user = mock_admin_client.cluster_users[test_user.name][0]

        mock_admin_client.cluster_users[test_user.name] = [
            replace(old_cluster_user, balance=Balance(credits=credits))
        ]

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)

    async def test_user_has_no_access_to_cluster_kill_all(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
        mock_admin_client: MockAdminClient,
    ) -> None:
        jobs = await make_jobs(test_user, None, 5)
        mock_admin_client.cluster_users[test_user.name] = []

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)

    async def test_orgs_credits_disabled_do_nothing(
        self,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
        org_factory: OrgFactory,
        user_factory: UserFactory,
        test_cluster: str,
    ) -> None:
        org = await org_factory("some-org", [(test_cluster, Balance(), Quota())])
        user = await user_factory(
            "some-user", [(test_cluster, org, Balance(), Quota())]
        )
        jobs = await make_jobs(user, org, 5)

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    async def test_org_has_credits_do_nothing(
        self,
        test_user: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_not_cancelled: Callable[[Iterable[Job]], Awaitable[None]],
        org_factory: OrgFactory,
        user_factory: UserFactory,
        test_cluster: str,
    ) -> None:
        org = await org_factory(
            "some-org", [(test_cluster, Balance(credits=Decimal("1.00")), Quota())]
        )
        user = await user_factory(
            "some-user", [(test_cluster, org, Balance(), Quota())]
        )

        jobs = await make_jobs(user, org, 5)

        await has_credits_enforcer.enforce()

        await check_not_cancelled(jobs)

    @pytest.mark.parametrize("credits", [Decimal("0"), Decimal("-0.5")])
    async def test_org_has_no_credits_kill_all(
        self,
        test_org: str,
        test_user_with_org: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
        credits: Decimal,
        mock_admin_client: MockAdminClient,
    ) -> None:
        jobs = await make_jobs(test_user_with_org, test_org, 5)
        old_org_cluster = mock_admin_client.org_clusters[test_org][0]

        mock_admin_client.org_clusters[test_org] = [
            replace(old_org_cluster, balance=Balance(credits=credits))
        ]

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)

    async def test_org_has_no_access_to_cluster_kill_all(
        self,
        test_org: str,
        test_user_with_org: AuthUser,
        has_credits_enforcer: CreditsLimitEnforcer,
        mock_auth_client: MockAuthClient,
        make_jobs: Callable[[AuthUser, Optional[str], int], Awaitable[list[Job]]],
        check_cancelled: Callable[[Iterable[Job], str], Awaitable[None]],
        mock_admin_client: MockAdminClient,
    ) -> None:
        jobs = await make_jobs(test_user_with_org, test_org, 5)
        mock_admin_client.org_clusters[test_org] = []

        await has_credits_enforcer.enforce()

        await check_cancelled(jobs, JobStatusReason.QUOTA_EXHAUSTED)


class TestStopOnClusterRemoveEnforcer:
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
