import pytest
import time
from platform_api.orchestrator.jobs_telemetry import JobsTelemetry, JobTop


@pytest.fixture
def job_top():
    return JobTop(cpu=1, mem=1)


@pytest.fixture
def jobs_telemetry():
    return JobsTelemetry.create()


class TestJobTop:
    def test_job_top_to_primitive(self, job_top):
        job_top_primitive = job_top.to_primitive()
        del job_top_primitive['timestamp']
        assert {"cpu": 1, "mem": 1} == job_top_primitive

    def test_job_top_timestamp(self, job_top):
        job_top_primitive = job_top.to_primitive()
        assert job_top_primitive['timestamp'] < time.time()


class TestJobsTelemetry:
    async def test_get_job_top(self, jobs_telemetry):
        job_id = "job_id"
        job_top = await jobs_telemetry.get_job_top(job_id=job_id)
        assert {"cpu": 1, "mem": 1} == job_top.to_primitive()
