import asyncio
from pathlib import Path
import uuid

import pytest

from platform_api.orchestrator import (
    KubeOrchestrator, JobRequest, JobStatus, JobError, Job
)
from platform_api.orchestrator.kube_orchestrator import KubeConfig


@pytest.fixture(scope='session')
def event_loop():
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)

    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(loop)
    asyncio.get_event_loop_policy().set_child_watcher(watcher)

    yield loop
    loop.close()


@pytest.fixture(scope='session')
async def kube_endpoint_url():
    process = await asyncio.create_subprocess_exec(
        'minikube', 'ip', stdout=asyncio.subprocess.PIPE)
    output, _ = await process.communicate()
    ip = output.decode().strip()
    return f'https://{ip}:8443'


@pytest.fixture(scope='session')
async def kube_config(kube_endpoint_url):
    return KubeConfig(
        endpoint_url=kube_endpoint_url,
        cert_authority_path=Path('~/.minikube/ca.crt').expanduser(),
        auth_cert_path=Path('~/.minikube/client.crt').expanduser(),
        auth_cert_key_path=Path('~/.minikube/client.key').expanduser()
    )
    return KubeConfig(endpoint_url='http://localhost:8080')


@pytest.fixture
async def kube_orchestrator(kube_config, event_loop):
    orchestrator = KubeOrchestrator(config=kube_config)
    async with orchestrator:
        yield orchestrator


@pytest.fixture
async def job_nginx(kube_orchestrator):
    job_id = str(uuid.uuid4())
    job_request = JobRequest(
        job_id=job_id, docker_image='nginx', container_name=job_id)
    job = Job(orchestrator=kube_orchestrator, job_request=job_request)
    return job


class TestKubeOrchestrator:

    async def wait_status(
            self, job: Job, job_status: JobStatus,
            interval_s: int=1, max_attempts: int=30):
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
        assert status == JobStatus.SUCCEEDED

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
        assert status == JobStatus.FAILED

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
        job_request = JobRequest(job_id=job_id, docker_image='python', container_name=job_id)
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)

        with pytest.raises(JobError):
            await job.start()
