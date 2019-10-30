import asyncio
from dataclasses import dataclass
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Dict

import pytest
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforce_poller import JobPolicyEnforcePoller
from platform_api.orchestrator.job_policy_enforcer import (
    JobPolicyEnforcer,
    JobPolicyEnforcerClientWrapper,
    QuotaJobPolicyEnforcer,
)


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
            ]
        }

    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        pass

    async def kill_job(self, job_id: str) -> None:
        pass


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
