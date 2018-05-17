import asyncio


import pytest

from platform_api.orchestrator import KubeOrchestrator
from platform_api.job_request import JobRequest
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


class TestKubeOrchestrator:
    @pytest.mark.asyncio
    async def test_start_job(self, kube_orchestrator):
        job_request = JobRequest(job_id='nginx',
                                 docker_image='truskovskyi/test',
                                 args=['python'],
                                 container_name='containername')
        job = Job(orchestrator=kube_orchestrator, job_request=job_request)

        # status = await job.start()

        # status = await job.status()
        # print(f"status = {status}")

        status = await job.delete()
        print(f"status = {status}")
