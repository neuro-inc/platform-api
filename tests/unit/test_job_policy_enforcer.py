import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Dict, List, Set

import pytest
from aiohttp import web
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforcer import (
    AbstractPlatformApiClient,
    JobInfo,
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
    JobsByUser,
    PlatformApiClient,
    QuotaEnforcer,
    UserQuotaInfo,
)
from platform_api.orchestrator.job_request import JobStatus
from tests.integration.api import ApiConfig
from tests.integration.conftest import ApiRunner


_EnforcePollingRunner = Callable[
    [JobPolicyEnforcer], AsyncContextManager[JobPolicyEnforcePoller]
]


@dataclass(frozen=True)
class PlatformApiEndpoints:
    url: URL

    @property
    def endpoint(self) -> str:
        return str(self.url)

    @property
    def platform_config_url(self) -> str:
        return f"{self.endpoint}/config"

    @property
    def jobs_base_url(self) -> str:
        return f"{self.endpoint}/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"


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


@pytest.fixture
def job_policy_enforcer_config() -> JobPolicyEnforcerConfig:
    return JobPolicyEnforcerConfig(
        platform_api_url=URL("http://localhost:8080"),
        token="admin-token",
        interval_sec=0.1,
    )


class TestPlatformApiClient:
    def test_convert_response_regular(self) -> None:
        runtime = AbstractPlatformApiClient.convert_response_to_runtime(
            {"total_gpu_run_time_minutes": 10, "total_non_gpu_run_time_minutes": 15}
        )
        assert runtime.total_gpu_run_time_delta == timedelta(seconds=600)
        assert runtime.total_non_gpu_run_time_delta == timedelta(seconds=900)

    def test_from_primitive_empty(self) -> None:
        runtime = AbstractPlatformApiClient.convert_response_to_runtime({})
        assert runtime.total_gpu_run_time_delta == timedelta.max
        assert runtime.total_non_gpu_run_time_delta == timedelta.max

    def test_from_primitive_only_gpu(self) -> None:
        runtime = AbstractPlatformApiClient.convert_response_to_runtime(
            {"total_gpu_run_time_minutes": 10}
        )
        assert runtime.total_gpu_run_time_delta == timedelta(seconds=600)
        assert runtime.total_non_gpu_run_time_delta == timedelta.max

    def test_from_primitive_only_non_gpu(self) -> None:
        runtime = AbstractPlatformApiClient.convert_response_to_runtime(
            {"total_non_gpu_run_time_minutes": 15}
        )
        assert runtime.total_gpu_run_time_delta == timedelta.max
        assert runtime.total_non_gpu_run_time_delta == timedelta(seconds=900)


class TestJobInfo:
    def test_from_json_gpu(self) -> None:
        payload = {
            "id": "job1",
            "owner": "user1",
            "status": "pending",
            "container": {"resources": {"gpu": 1}},
        }
        job_info = JobInfo.from_json(payload)
        assert job_info.id == "job1"
        assert job_info.owner == "user1"
        assert job_info.status == JobStatus.PENDING
        assert job_info.is_gpu is True

    def test_from_json_cpu(self) -> None:
        payload = {
            "id": "job123",
            "owner": "user2",
            "status": "running",
            "container": {"resources": {"cpu": 4}},
        }
        job_info = JobInfo.from_json(payload)
        assert job_info.id == "job123"
        assert job_info.owner == "user2"
        assert job_info.status == JobStatus.RUNNING
        assert job_info.is_gpu is False


class MockPlatformApiClient(AbstractPlatformApiClient):
    def __init__(self, gpu_quota: int = 10, cpu_quota: int = 10):
        self._gpu_quota = gpu_quota
        self._cpu_quota = cpu_quota
        self._killed_jobs: Set[str] = set()

    async def get_non_terminated_jobs(self) -> List[JobInfo]:
        return [
            JobInfo("job1", JobStatus.RUNNING, "user1", False),
            JobInfo("job2", JobStatus.PENDING, "user1", False),
            JobInfo("job3", JobStatus.RUNNING, "user2", False),
            JobInfo("job4", JobStatus.PENDING, "user2", False),
            JobInfo("job5", JobStatus.PENDING, "user2", True),
        ]

    async def get_user_stats(self, username: str) -> UserQuotaInfo:
        quota = AbstractPlatformApiClient.convert_response_to_runtime(
            {
                "total_gpu_run_time_minutes": self._gpu_quota,
                "total_non_gpu_run_time_minutes": self._cpu_quota,
            }
        )
        jobs = AbstractPlatformApiClient.convert_response_to_runtime(
            {"total_gpu_run_time_minutes": 9, "total_non_gpu_run_time_minutes": 8}
        )
        return UserQuotaInfo(quota=quota, jobs=jobs)

    async def kill_job(self, job_id: str) -> None:
        self._killed_jobs.add(job_id)

    @property
    def killed_jobs(self) -> Set[str]:
        return self._killed_jobs


class TestQuotaEnforcer:
    @pytest.mark.asyncio
    async def test_get_users_with_active_jobs(self) -> None:
        client = MockPlatformApiClient()
        enforcer = QuotaEnforcer(client)
        result = await enforcer.get_active_users_and_jobs()
        assert result == [
            JobsByUser(username="user1", cpu_job_ids={"job1", "job2"}),
            JobsByUser(
                username="user2", cpu_job_ids={"job3", "job4"}, gpu_job_ids={"job5"}
            ),
        ]

    @pytest.mark.asyncio
    async def test_check_user_quota_ok(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient()
        enforcer = QuotaEnforcer(client)
        await enforcer.check_user_quota(JobsByUser("user2", cpu_jobs, gpu_jobs))
        assert len(client.killed_jobs) == 0

    @pytest.mark.asyncio
    async def test_check_user_quota_gpu_exceeded(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(gpu_quota=1)
        enforcer = QuotaEnforcer(client)
        await enforcer.check_user_quota(JobsByUser("user2", cpu_jobs, gpu_jobs))
        assert client.killed_jobs == gpu_jobs

    @pytest.mark.asyncio
    async def test_check_user_quota_cpu_exceeded(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(cpu_quota=1)
        enforcer = QuotaEnforcer(client)
        await enforcer.check_user_quota(JobsByUser("user2", cpu_jobs, gpu_jobs))
        assert client.killed_jobs == cpu_jobs | gpu_jobs

    @pytest.mark.asyncio
    async def test_enforce_ok(self) -> None:
        client = MockPlatformApiClient()
        enforcer = QuotaEnforcer(client)
        await enforcer.enforce()
        assert len(client.killed_jobs) == 0

    @pytest.mark.asyncio
    async def test_enforce_gpu_exceeded(self) -> None:
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(gpu_quota=1)
        enforcer = QuotaEnforcer(client)
        await enforcer.enforce()
        assert client.killed_jobs == gpu_jobs

    @pytest.mark.asyncio
    async def test_enforce_cpu_exceeded(self) -> None:
        cpu_jobs = {f"job{i}" for i in range(1, 5)}
        gpu_jobs = {"job5"}
        client = MockPlatformApiClient(cpu_quota=1)
        enforcer = QuotaEnforcer(client)
        await enforcer.enforce()
        assert client.killed_jobs == gpu_jobs | cpu_jobs


class TestJobPolicyEnforcer:
    @pytest.fixture
    async def run_enforce_polling(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> Callable[[JobPolicyEnforcer], AsyncIterator[JobPolicyEnforcePoller]]:
        @asynccontextmanager
        async def _factory(
            enforcer: JobPolicyEnforcer,
        ) -> AsyncIterator[JobPolicyEnforcePoller]:
            async with JobPolicyEnforcePoller(
                policy_enforcer=enforcer, config=job_policy_enforcer_config
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


@pytest.fixture
async def mock_api() -> AsyncIterator[ApiConfig]:
    async def _get_jobs(request: web.Request) -> web.Response:
        statuses = request.query.getall("status")
        assert statuses == ["pending", "running"]
        payload: Dict[str, Any] = {
            "jobs": [
                {
                    "id": "job1",
                    "status": "running",
                    "owner": "user1",
                    "container": {"resources": {"cpu": 1.0}},
                },
                {
                    "id": "job2",
                    "status": "pending",
                    "owner": "user1",
                    "container": {"resources": {"cpu": 1.0}},
                },
                {
                    "id": "job3",
                    "status": "running",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0}},
                },
                {
                    "id": "job4",
                    "status": "pending",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0}},
                },
                {
                    "id": "job5",
                    "status": "pending",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0, "gpu": 0.5}},
                },
            ]
        }

        return web.json_response(payload)

    async def _kill_job(request: web.Request) -> web.Response:
        job_id = request.match_info.get("job_id")
        assert job_id is not None
        return web.Response()

    async def _user_stats(request: web.Request) -> web.Response:
        username = request.match_info["username"]
        payload: Dict[str, Any] = {
            "name": username,
            "jobs": {
                "total_gpu_run_time_minutes": 0,
                "total_non_gpu_run_time_minutes": 0,
            },
            "quota": {
                "total_gpu_run_time_minutes": 60 * 100,
                "total_non_gpu_run_time_minutes": 60 * 100,
            },
        }
        return web.json_response(payload)

    def _create_app() -> web.Application:
        app = web.Application()
        app.add_routes(
            [
                web.get("/api/v1/jobs", _get_jobs),
                web.delete("/api/v1/jobs/{job_id}", _kill_job),
                web.get("/api/v1/stats/users/{username}", _user_stats),
            ]
        )
        return app

    app = _create_app()
    runner = ApiRunner(app, port=8080)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


class TestRealJobPolicyEnforcerClientWrapper:
    @pytest.mark.asyncio
    async def test_get_stats(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        client = PlatformApiClient(job_policy_enforcer_config)
        response = await client.get_user_stats("user1")
        expected_quota = AbstractPlatformApiClient.convert_response_to_runtime(
            {
                "total_gpu_run_time_minutes": 60 * 100,
                "total_non_gpu_run_time_minutes": 60 * 100,
            }
        )
        expected_jobs = AbstractPlatformApiClient.convert_response_to_runtime(
            {"total_gpu_run_time_minutes": 0, "total_non_gpu_run_time_minutes": 0}
        )
        expected_response = UserQuotaInfo(quota=expected_quota, jobs=expected_jobs)
        assert response == expected_response

    @pytest.mark.asyncio
    async def test_get_non_terminated_jobs(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        client = PlatformApiClient(job_policy_enforcer_config)
        response = await client.get_non_terminated_jobs()
        assert len(response) == 5
