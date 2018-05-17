import asyncio
import uuid

import pytest

from platform_api.orchestrator import KubeOrchestrator
from platform_api.job_request import JobRequest, JobStatus, JobError
from platform_api.job import Job


@pytest.fixture(scope='session')
def event_loop():
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()


@pytest.fixture
async def kube_orchestrator(event_loop):
    yield await KubeOrchestrator.from_env(event_loop)


@pytest.fixture(scope="function")
async def job_nginx(kube_orchestrator):
    job_id = str(uuid.uuid4())
    job_request = JobRequest(job_id=job_id, docker_image='nginx', container_name=job_id)
    job = Job(orchestrator=kube_orchestrator, job_request=job_request)
    return job


class TestKubeOrchestrator:

    async def wait_status(self, job: Job, job_status: JobStatus, interval_s: int=1, max_attempts: int=10):
        for _ in range(max_attempts):
            real_status = await job.status()
            if real_status == job_status:
                return real_status
            else:
                await asyncio.sleep(interval_s)
        raise RuntimeError("too long")

    @pytest.mark.asyncio
    async def test_start_job_happy_path(self, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_status(job_nginx, JobStatus.SUCCEEDED)
        status = await job_nginx.status()
        assert status == JobStatus.SUCCEEDED

        status = await job_nginx.delete()
        assert status == JobStatus.DELETED

    @pytest.mark.asyncio
    async def test_start_job_broken_image(self, kube_orchestrator):
        job_id = str(uuid.uuid4())
        job_request = JobRequest(job_id=job_id, docker_image='notsuchdockerimage', container_name=job_id)
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)
        status = await job.start()
        assert status == JobStatus.PENDING

        await self.wait_status(job, JobStatus.FAILED)
        status = await job.status()
        assert status == JobStatus.FAILED

        status = await job.delete()
        assert status == JobStatus.DELETED

    @pytest.mark.asyncio
    async def test_start_job_with_not_unique_id(self, kube_orchestrator, job_nginx):
        status = await job_nginx.start()
        assert status == JobStatus.PENDING

        await self.wait_status(job_nginx, JobStatus.SUCCEEDED)
        status = await job_nginx.status()
        assert status == JobStatus.SUCCEEDED

        job_id = await job_nginx.get_id()
        job_request_second = JobRequest(job_id=job_id, docker_image='python', container_name=job_id)
        job_second = Job(orchestrator=kube_orchestrator, job_request=job_request_second)
        with pytest.raises(JobError):
            await job_second.start()

        status = await job_nginx.delete()
        assert status == JobStatus.DELETED

    @pytest.mark.asyncio
    async def test_status_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.status()

    @pytest.mark.asyncio
    async def test_delete_job_not_exist(self, job_nginx):
        with pytest.raises(JobError):
            await job_nginx.delete()
