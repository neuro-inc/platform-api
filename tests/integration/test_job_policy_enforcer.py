import asyncio
from dataclasses import dataclass
from typing import AsyncIterator

import pytest
from yarl import URL

from platform_api.config import Config, JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforcer import JobPolicyEnforcer
from tests.integration.api import ApiConfig


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


class TestJobPolicyEnforcer:
    @pytest.fixture
    def job_policy_enforcer_config(self, config: Config) -> JobPolicyEnforcerConfig:
        return config.job_policy_enforcer

    @pytest.fixture
    async def enfocer(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> AsyncIterator[JobPolicyEnforcer]:
        async with JobPolicyEnforcer(config=job_policy_enforcer_config) as enforcer:
            yield enforcer

    @pytest.fixture
    async def enfocer_debug(
        self,
        job_policy_enforcer_config: JobPolicyEnforcerConfig,
        event_loop: asyncio.AbstractEventLoop,
    ) -> AsyncIterator[JobPolicyEnforcer]:

        closed = event_loop.create_future()

        def assert_no_exceptions(fut: "asyncio.Future[None]") -> None:
            try:
                assert fut.done()
                assert not fut.cancelled()
                assert fut.exception() is None
                closed.set_result(1)
            except Exception as exc:
                closed.set_exception(exc)

        async with JobPolicyEnforcer(config=job_policy_enforcer_config) as enforcer:
            assert enforcer._task is not None
            enforcer._task.add_done_callback(assert_no_exceptions)
            yield enforcer
            assert enforcer._is_active is None
            assert enforcer._task is None
            assert enforcer._session is None

        await closed

    @pytest.mark.asyncio
    async def test_basic(
        self, api: ApiConfig, enfocer_debug: JobPolicyEnforcer
    ) -> None:
        two_loops_timeout = enfocer_debug._interval_sec * 2
        await asyncio.sleep(two_loops_timeout)
