import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, List

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
    async def enforcer(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> AsyncIterator[JobPolicyEnforcer]:
        exceptions: List[BaseException] = []
        enforcer = JobPolicyEnforcer(
            config=job_policy_enforcer_config,
            exception_handler=lambda exc: exceptions.append(exc),
        )
        await enforcer.start()
        assert enforcer._task
        assert not enforcer._task.done()

        yield enforcer

        assert not enforcer._task.done()
        await enforcer.stop()
        assert enforcer._task is None
        assert enforcer._session.closed
        assert not exceptions

    @pytest.mark.asyncio
    async def test_basic(self, api: ApiConfig, enforcer: JobPolicyEnforcer) -> None:
        two_loops_timeout = enforcer._interval_sec * 2
        await asyncio.sleep(two_loops_timeout)

    @pytest.mark.asyncio
    async def test_basic2(self, api: ApiConfig, enforcer: JobPolicyEnforcer) -> None:
        two_loops_timeout = enforcer._interval_sec * 2
        await asyncio.sleep(two_loops_timeout)
