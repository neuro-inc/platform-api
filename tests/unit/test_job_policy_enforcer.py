import asyncio
from dataclasses import dataclass
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Dict, Set

import pytest
from aiohttp import web
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforce_poller import JobPolicyEnforcePoller
from platform_api.orchestrator.job_policy_enforcer import (
    JobPolicyEnforcer,
    JobPolicyEnforcerClientWrapper,
    QuotaJobPolicyEnforcer,
    RealJobPolicyEnforcerClientWrapper,
)
from tests.integration.api import ApiConfig
from tests.integration.conftest import ApiRunner


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
        interval_sec=1,
    )


class TestJobPolicyEnforcePoller:
    @pytest.fixture
    async def run_enforce_polling(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> Callable[[JobPolicyEnforcer], AsyncIterator[None]]:
        @asynccontextmanager
        async def _factory(enforcer: JobPolicyEnforcer) -> AsyncIterator[None]:
            poller = JobPolicyEnforcePoller(
                policy_enforcer=enforcer, config=job_policy_enforcer_config
            )
            await poller.start()
            yield
            await poller.stop()

        return _factory

    @pytest.mark.asyncio
    async def test_basic_no_exception_short_response(
        self,
        run_enforce_polling: Callable[[JobPolicyEnforcer], AsyncContextManager[None]],
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
        run_enforce_polling: Callable[[JobPolicyEnforcer], AsyncContextManager[None]],
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
        run_enforce_polling: Callable[[JobPolicyEnforcer], AsyncContextManager[None]],
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
        run_enforce_polling: Callable[[JobPolicyEnforcer], AsyncContextManager[None]],
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
    ) -> None:
        interval = job_policy_enforcer_config.interval_sec
        enforcer = MockedJobPolicyEnforcer(
            raise_exception=True, enforce_time_sec=interval * 2
        )
        async with run_enforce_polling(enforcer):
            await asyncio.sleep(interval * 1.5)
            assert enforcer.called_times == 1


class MockJobPolicyEnforcerClientWrapper(JobPolicyEnforcerClientWrapper):
    def __init__(self, gpu_quota: int = 10, cpu_quota: int = 10):
        self._gpu_quota = gpu_quota
        self._cpu_quota = cpu_quota
        self._killed_jobs: Set[str] = set()

    async def get_users_with_active_jobs(self) -> Dict[Any, Any]:
        return {
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
                {
                    "id": "job6",
                    "status": "succeeded",
                    "owner": "user2",
                    "container": {"resources": {"cpu": 1.0, "gpu": 0.5}},
                },
            ]
        }

    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        return {
            "quota": {
                "total_gpu_run_time_minutes": self._gpu_quota,
                "total_non_gpu_run_time_minutes": self._cpu_quota,
            },
            "jobs": {
                "total_gpu_run_time_minutes": 9,
                "total_non_gpu_run_time_minutes": 8,
            },
        }

    async def kill_job(self, job_id: str) -> None:
        self._killed_jobs.add(job_id)

    @property
    def killed_jobs(self) -> Set[str]:
        return self._killed_jobs


class TestQuotaJobPolicyEnforcer:
    @pytest.mark.asyncio
    async def test_get_users_with_active_jobs(self) -> None:
        wrapper = MockJobPolicyEnforcerClientWrapper()
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        result = await enforcer.get_users_with_active_jobs()
        assert result == {
            "user1": {"cpu": {"job1", "job2"}, "gpu": set()},
            "user2": {"cpu": {"job3", "job4"}, "gpu": {"job5"}},
        }

    @pytest.mark.asyncio
    async def test_check_user_quota_ok(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        wrapper = MockJobPolicyEnforcerClientWrapper()
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.check_user_quota("user2", cpu_jobs, gpu_jobs)
        assert len(wrapper.killed_jobs) == 0

    @pytest.mark.asyncio
    async def test_check_user_quota_gpu_exceeded(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        wrapper = MockJobPolicyEnforcerClientWrapper(gpu_quota=1)
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.check_user_quota("user2", cpu_jobs, gpu_jobs)
        assert wrapper.killed_jobs == gpu_jobs

    @pytest.mark.asyncio
    async def test_check_user_quota_cpu_exceeded(self) -> None:
        cpu_jobs = {"job3", "job4"}
        gpu_jobs = {"job5"}
        wrapper = MockJobPolicyEnforcerClientWrapper(cpu_quota=1)
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.check_user_quota("user2", cpu_jobs, gpu_jobs)
        assert wrapper.killed_jobs == cpu_jobs.union(gpu_jobs)

    @pytest.mark.asyncio
    async def test_enforce_ok(self) -> None:
        wrapper = MockJobPolicyEnforcerClientWrapper()
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.enforce()
        assert len(wrapper.killed_jobs) == 0

    @pytest.mark.asyncio
    async def test_enforce_gpu_exceeded(self) -> None:
        gpu_jobs = {"job5"}
        wrapper = MockJobPolicyEnforcerClientWrapper(gpu_quota=1)
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.enforce()
        assert wrapper.killed_jobs == gpu_jobs

    @pytest.mark.asyncio
    async def test_enforce_cpu_exceeded(self) -> None:
        cpu_jobs = {f"job{i}" for i in range(1, 5)}
        gpu_jobs = {"job5"}
        wrapper = MockJobPolicyEnforcerClientWrapper(cpu_quota=1)
        enforcer = QuotaJobPolicyEnforcer(wrapper)
        await enforcer.enforce()
        assert wrapper.killed_jobs == gpu_jobs.union(cpu_jobs)


@pytest.fixture
async def mock_api() -> AsyncIterator[ApiConfig]:
    async def _get_jobs(request: web.Request) -> web.Response:
        # statuses = request.query["status"]
        # assert statuses == ["pending", "running"]
        payload: Dict[str, Any] = {}
        return web.json_response(payload)

    async def _kill_job(request: web.Request) -> web.Response:
        # job_id = request.match_info["job_id"]
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
                web.get("/api/v1/stats/user/{username}", _user_stats),
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
    async def test_kill_job(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        wrapper = RealJobPolicyEnforcerClientWrapper(job_policy_enforcer_config)
        await wrapper.kill_job("job123")
        # TODO Validate corresponding URL with correct parameter gets called

    @pytest.mark.asyncio
    async def test_get_stats(self, mock_api: ApiConfig) -> None:
        job_policy_enforcer_config = JobPolicyEnforcerConfig(
            URL(mock_api.endpoint), "random_token"
        )
        wrapper = RealJobPolicyEnforcerClientWrapper(job_policy_enforcer_config)
        response = await wrapper.get_user_stats("user1")
        assert response == {
            "name": "user1",
            "jobs": {
                "total_gpu_run_time_minutes": 0,
                "total_non_gpu_run_time_minutes": 0,
            },
            "quota": {
                "total_gpu_run_time_minutes": 60 * 100,
                "total_non_gpu_run_time_minutes": 60 * 100,
            },
        }

    @pytest.mark.asyncio
    async def test_get_jobs(self, mock_api: ApiConfig) -> None:
        pass
        # job_policy_enforcer_config = JobPolicyEnforcerConfig(
        #     URL(mock_api.endpoint), "random_token"
        # )
        # wrapper = RealJobPolicyEnforcerClientWrapper(job_policy_enforcer_config)
        # response = await wrapper.get_users_with_active_jobs()
        # TODO validate response
