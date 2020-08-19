from platform_api.orchestrator.job import JobStatus
from platform_api.orchestrator.status import Status


class TestStatus:
    def test_status_value(self) -> None:
        status = Status.create(JobStatus.SUCCEEDED)
        assert status.value == JobStatus.SUCCEEDED

    def test_set_value(self) -> None:
        status = Status.create(JobStatus.SUCCEEDED)
        assert status.value == JobStatus.SUCCEEDED
        status.set(JobStatus.FAILED)
        assert status.value == JobStatus.FAILED

    def test_values(self) -> None:
        assert JobStatus.values() == [
            "pending",
            "running",
            "succeeded",
            "cancelled",
            "failed",
        ]
