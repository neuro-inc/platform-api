from dataclasses import dataclass
from typing import AsyncIterator

import pytest
from yarl import URL

from platform_api.config import Config, JobPolicyEnforcerConfig
from platform_api.orchestrator.job_policy_enforce_poller import JobPolicyEnforcePoller
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


class MockJobPolicyEnforcer(JobPolicyEnforcer):
    async def enforce(self) -> None:
        pass


class TestJobPolicyEnforcePoller:
    @pytest.fixture
    def job_policy_enforcer_config(self, config: Config) -> JobPolicyEnforcerConfig:
        return config.job_policy_enforcer

    @pytest.fixture
    async def job_policy_enforce_poller(
        self, job_policy_enforcer_config: JobPolicyEnforcerConfig
    ) -> AsyncIterator[JobPolicyEnforcePoller]:
        enforcer = MockJobPolicyEnforcer()
        poller = JobPolicyEnforcePoller(
            config=job_policy_enforcer_config, policy_enforcer=enforcer
        )
        await poller.start()
        yield poller
        await poller.stop()

    @pytest.mark.asyncio
    async def test_basic(
        self, api: ApiConfig, job_policy_enforce_poller: JobPolicyEnforcePoller
    ) -> None:
        # TODO(artem): drop this test once we have tests on the policy enforcement logic
        await job_policy_enforce_poller._run_once()
