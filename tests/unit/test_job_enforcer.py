import pytest

from platform_api.orchestrator.job_enforcer import JobPolicyEnforcer


class MockJobPolicyEnforcer(JobPolicyEnforcer):
    def enforce(self) -> None:
        pass


class TestJobEnforcer:
    async def test_check_user_quota(self, mock_enforcer: JobPolicyEnforcer):
        pass
