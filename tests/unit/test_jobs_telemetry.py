import pytest

from platform_api.orchestrator.jobs_telemetry import JobTop, JobsTelemetry


@pytest.fixture
def job_top():
    return JobTop(cpu=1, mem=1)


@pytest.fixture
def jobs_telemetry():
    return JobsTelemetry.create()


class TestJobTop:

    def test_job_top_to_primitive(self, job_top):
        assert {'cpu': 1, 'mem': 1} == job_top.to_primitive()


class TestJobsTelemetry:

    async def test_get_job_top(self, jobs_telemetry):
        job_id = 'job_id'
        job_top = jobs_telemetry.get_job_top(job_id=job_id)
        assert {'cpu': 1, 'mem': 1} == job_top.to_primitive()
