import asyncio
from dataclasses import dataclass
from typing import AsyncContextManager, AsyncIterator, Callable

import pytest
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforcer import (
    JobPolicyEnforcePoller,
    JobPolicyEnforcer,
)


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


class TestJobPolicyEnforcer:
    @pytest.fixture
    async def run_enforce_polling(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> Callable[[JobPolicyEnforcer], AsyncIterator[JobPolicyEnforcePoller]]:
        @asynccontextmanager
        async def _factory(
            enforcer: JobPolicyEnforcer
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
