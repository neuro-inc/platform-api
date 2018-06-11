from platform_api.orchestrator import Status, JobStatus


class TestStatus:
    def test_status_value(self):
        status = Status.create(JobStatus.SUCCEEDED)
        assert status.value == JobStatus.SUCCEEDED

    def test_set_value(self):
        status = Status.create(JobStatus.SUCCEEDED)
        assert status.value == JobStatus.SUCCEEDED
        status.set(JobStatus.FAILED)
        assert status.value == JobStatus.FAILED
