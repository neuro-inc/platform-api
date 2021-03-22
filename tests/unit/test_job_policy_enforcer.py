import asyncio
import datetime
from decimal import Decimal
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

import pytest
from async_generator import asynccontextmanager
from neuro_auth_client import (
    Cluster as AuthCluster,
    Quota as AuthQuota,
    User as AuthUser,
)
from yarl import URL

from platform_api.admin_client import AdminClient
from platform_api.cluster_config import ClusterConfig
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import Job, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_policy_enforcer import (
    BillingEnforcer,
    CreditsLimitEnforcer,
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
    RuntimeLimitEnforcer,
)
from platform_api.orchestrator.job_request import JobRequest, JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
from tests.unit.conftest import MockAuthClient


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
    ) -> Callable[[JobPolicyEnforcer], AsyncIterator[JobPolicyEnforcePoller]]:
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


class MockAdminClient(AdminClient):
    def __init__(self) -> None:
        self.change_log: List[Tuple[str, str, Decimal]] = []

    async def change_user_credits(
        self, cluster_name: str, username: str, delta: Decimal
    ) -> None:
        self.change_log.append((cluster_name, username, delta))


class TestBillingEnforcer:
    @pytest.fixture()
    def admin_client(self) -> MockAdminClient:
        return MockAdminClient()

    @pytest.mark.asyncio
    async def test_jobs_charged(
        self,
        test_user: AuthUser,
        jobs_service: JobsService,
        cluster_config: ClusterConfig,
        admin_client: MockAdminClient,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        enforcer = BillingEnforcer(jobs_service, admin_client)
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
        assert len(admin_client.change_log) == 1
        assert admin_client.change_log[0][0] == job.cluster_name
        assert admin_client.change_log[0][1] == job.owner
        assert -admin_client.change_log[0][2] >= Decimal("1.5") * per_hour
        assert -admin_client.change_log[0][2] <= (Decimal("1.5") + second) * per_hour
        await asyncio.sleep(1)
        await enforcer.enforce()
        assert len(admin_client.change_log) == 2
        assert admin_client.change_log[1][0] == job.cluster_name
        assert admin_client.change_log[1][1] == job.owner
        assert -admin_client.change_log[1][2] >= second * per_hour
        assert -admin_client.change_log[1][2] <= 2 * second * per_hour
