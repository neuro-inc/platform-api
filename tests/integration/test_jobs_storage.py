import pytest

from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container, ContainerResources, JobError, JobRequest, JobStatus
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

    def _create_pending_job(self, kube_orchestrator):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request())

    def _create_running_job(self, kube_orchestrator):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            status=JobStatus.RUNNING)

    def _create_succeeded_job(self, kube_orchestrator, is_deleted=False):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            status=JobStatus.SUCCEEDED,
            is_deleted=is_deleted)

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_set_get(self, redis_client, kube_orchestrator):
        original_job = self._create_pending_job(kube_orchestrator)
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
        original_job = self._create_pending_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(original_job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_running_empty(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)

        jobs = await storage.get_running_jobs()
        assert not jobs

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_running(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == running_job.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_for_deletion_empty(
            self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_get_for_deletion(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        deleted_job = self._create_succeeded_job(
            kube_orchestrator, is_deleted=True)
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)
        await storage.set_job(deleted_job)

        jobs = await storage.get_jobs_for_deletion()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == succeeded_job.id
        assert job.status == JobStatus.SUCCEEDED
        assert not job.is_deleted

    @pytest.mark.usefixtures('clear_redis')
    @pytest.mark.asyncio
    async def test_job_lifecycle(self, redis_client, kube_orchestrator):
        job = self._create_pending_job(kube_orchestrator)
        job_id = job.id
        storage = RedisJobsStorage(
            redis_client, orchestrator=kube_orchestrator)
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == job_id
        assert job.status == JobStatus.PENDING

        jobs = await storage.get_running_jobs()
        assert not jobs

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

        job.status = JobStatus.RUNNING
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == job_id
        assert job.status == JobStatus.RUNNING

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

        job.status = JobStatus.FAILED
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1

        jobs = await storage.get_running_jobs()
        assert not jobs

        jobs = await storage.get_jobs_for_deletion()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == job_id
        assert job.status == JobStatus.FAILED
        assert not job.is_deleted

        job.is_deleted = True
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1

        jobs = await storage.get_running_jobs()
        assert not jobs

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs
