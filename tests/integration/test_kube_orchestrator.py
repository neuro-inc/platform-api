import asyncio
import uuid

import pytest

from platform_api.orchestrator import (
    KubeOrchestrator, JobRequest, JobStatus, JobError, Job
)


@pytest.fixture
async def kube_orchestrator(kube_config, event_loop):
    orchestrator = KubeOrchestrator(config=kube_config)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def job_nginx(kube_orchestrator):
    job_id = str(uuid.uuid4())
    container = Container(image='nginx')
    job_request = JobRequest(
        job_id=job_id, container=container)
    job = Job(orchestrator=kube_orchestrator, job_request=job_request)
    return job


class TestKubeOrchestrator:

    async def wait_for_completion(
            self, job: Job,
            interval_s: int=1, max_attempts: int=30):
        for _ in range(max_attempts):
            status = await job.status()
            if status != JobStatus.PENDING:
                return status
            else:
                await asyncio.sleep(interval_s)
        else:
            pytest.fail('too long')

    async def wait_for_failure(self, *args, **kwargs):
        status = await self.wait_for_completion(*args, **kwargs)
        assert status == JobStatus.FAILED

    async def wait_for_success(self, *args, **kwargs):
        status = await self.wait_for_completion(*args, **kwargs)
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_happy_path(self, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_for_success(job_nginx)
        status = await job_nginx.status()
        assert status == JobStatus.SUCCEEDED

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_start_job_broken_image(self, kube_orchestrator):
        job_id = str(uuid.uuid4())
        container = Container(image='notsuchdockerimage')
        job_request = JobRequest(job_id=job_id, container=container)
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)
        status = await job.start()
        assert status == JobStatus.PENDING

        await self.wait_for_failure(job)
        status = await job.status()
        assert status == JobStatus.FAILED

        status = await job.delete()
        assert status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_start_job_with_not_unique_id(self, kube_orchestrator, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_for_success(job_nginx)
        status = await job_nginx.status()
        assert status == JobStatus.SUCCEEDED

        job_id = await job_nginx.get_id()
        container = Container(image='python')
        job_request_second = JobRequest(job_id=job_id, container=container)
        job_second = Job(orchestrator=kube_orchestrator, job_request=job_request_second)
        with pytest.raises(JobError):
            await job_second.start()

        status = await job_nginx.delete()
        assert status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_status_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.delete()

    @pytest.mark.asyncio
    async def test_broken_job_id(self, kube_orchestrator):
        job_id = "some_BROCKEN_JOB-123@#$%^&*(______------ID"
        container = Container(image='python')
        job_request = JobRequest(job_id=job_id, container=container)
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)

        with pytest.raises(JobError):
            await job.start()
