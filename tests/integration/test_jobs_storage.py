import pytest

from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, JobError, JobRequest
)
from platform_api.orchestrator.jobs_storage import RedisJobsStorage


class TestRedisJobsStorage:
    # TODO: move
    @pytest.fixture
    async def clear_redis(self, redis_client):
        await redis_client.flushdb()
        yield
        await redis_client.flushdb()

    def _create_job_request(self):
        container = Container(
            image='ubuntu', command='sleep 5',
            resources=ContainerResources(cpu=0.1, memory_mb=256))
        return JobRequest.create(container)

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_set_get(self, redis_client, kube_orchestrator):
        original_job = Job(
            kube_orchestrator.config,
            job_request=self._create_job_request())
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_non_existent(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        with pytest.raises(JobError, match='no such job unknown'):
            await storage.get_job('unknown')

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_all_empty(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)

        jobs = await storage.get_all_jobs()
        assert not jobs

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_all(self, redis_client, kube_orchestrator):
        original_job = Job(
            kube_orchestrator.config,
            job_request=self._create_job_request())
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(original_job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == original_job.id
        assert job.status == original_job.status
