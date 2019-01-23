from itertools import count

import aioredis
import pytest

from platform_api.config import OrchestratorConfig
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import (
    JOB_FILTER_RUNNING,
    JOB_FILTER_UNFINISHED,
    InMemoryJobsStorage,
    JobFilter,
    JobsStorage,
    RedisJobsStorage,
)


@pytest.fixture(scope="function")
def create_storage():
    def impl(
        storage_class_name: str,
        client: aioredis.Redis,
        orchestrator_config: OrchestratorConfig,
    ) -> JobsStorage:
        if storage_class_name == "InMemoryJobsStorage":
            return InMemoryJobsStorage(orchestrator_config=orchestrator_config)
        if storage_class_name == "RedisJobsStorage":
            return RedisJobsStorage(client, orchestrator_config=orchestrator_config)
        raise ValueError(f"Invalid storage class: {storage_class_name}")

    return impl


@pytest.mark.parametrize("storage_class", ["RedisJobsStorage", "InMemoryJobsStorage"])
class TestJobsStorage:
    _job_description_index_generator = count()

    def _create_job_request(self):
        container = Container(
            image="ubuntu",
            command="sleep 5",
            resources=ContainerResources(cpu=0.1, memory_mb=256),
        )
        description = f"test job {next(self._job_description_index_generator)}"
        return JobRequest.create(container, description=description)

    def _create_pending_job(self, kube_orchestrator):
        return Job(kube_orchestrator.config, job_request=self._create_job_request())

    def _create_running_job(self, kube_orchestrator):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            status=JobStatus.RUNNING,
        )

    def _create_succeeded_job(self, kube_orchestrator, is_deleted=False):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            status=JobStatus.SUCCEEDED,
            is_deleted=is_deleted,
        )

    @pytest.mark.asyncio
    async def test_set_get(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        original_job = self._create_pending_job(kube_orchestrator)
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_get_non_existent(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        with pytest.raises(JobError, match="no such job unknown"):
            await storage.get_job("unknown")

    @pytest.mark.asyncio
    async def test_get_all_empty(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        jobs = await storage.get_all_jobs()
        assert jobs == []

    @pytest.mark.parametrize("number_jobs", [1, 10, 1000])
    @pytest.mark.asyncio
    async def test_get_all_no_filter(
        self,
        redis_client,
        kube_orchestrator,
        number_jobs,
        create_storage,
        storage_class,
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        original_jobs = []
        for _ in range(number_jobs):
            job = self._create_pending_job(kube_orchestrator)
            await storage.set_job(job)
            original_jobs.append(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == number_jobs

        jobs = sorted(jobs, key=lambda j: j.id)
        original_jobs = sorted(original_jobs, key=lambda j: j.id)
        for job, original_job in zip(jobs, original_jobs):
            assert job.id == original_job.id
            assert job.status == original_job.status
            assert job.description == original_job.description

    @pytest.mark.asyncio
    async def test_get_all_filter_by_status_empty_result(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        original_pending_job = self._create_pending_job(kube_orchestrator)
        original_running_job = self._create_running_job(kube_orchestrator)
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(original_pending_job)
        await storage.set_job(original_running_job)

        job_filter = JobFilter.from_primitive({"status": "succeeded+failed"})
        jobs = await storage.get_all_jobs(job_filter)
        jobs = sorted(jobs, key=lambda j: j.status)
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_all_filter_by_status(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        original_pending_job = self._create_pending_job(kube_orchestrator)
        original_running_job = self._create_running_job(kube_orchestrator)
        original_succeeded_job = self._create_succeeded_job(kube_orchestrator)
        original_deleted_job = self._create_succeeded_job(
            kube_orchestrator, is_deleted=True
        )
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(original_pending_job)
        await storage.set_job(original_running_job)
        await storage.set_job(original_succeeded_job)
        await storage.set_job(original_deleted_job)

        job_filter = JobFilter.from_primitive({"status": "running+pending"})
        jobs = await storage.get_all_jobs(job_filter)
        jobs = sorted(jobs, key=lambda j: j.status)

        assert len(jobs) == 2

        job_pending = jobs[0]
        assert job_pending.status == JobStatus.PENDING
        assert job_pending.id == original_pending_job.id
        assert job_pending.description == original_pending_job.description

        job_running = jobs[1]
        assert job_running.status == JobStatus.RUNNING
        assert job_running.id == original_running_job.id
        assert job_running.description == original_running_job.description

    @pytest.mark.asyncio
    async def test_get_running_empty(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        jobs = await storage.get_running_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_running(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == running_job.id
        assert job.status == JobStatus.RUNNING

        filtered_jobs = await storage.get_all_jobs(JOB_FILTER_RUNNING)
        assert len(filtered_jobs) == 1
        filtered_job = filtered_jobs[0]
        assert filtered_job.id == job.id

    @pytest.mark.asyncio
    async def test_get_unfinished_empty(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        jobs = await storage.get_unfinished_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_unfinished(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        original_pending_job = self._create_pending_job(kube_orchestrator)
        original_running_job = self._create_running_job(kube_orchestrator)
        original_succeeded_job = self._create_succeeded_job(kube_orchestrator)
        original_failed_job = self._create_succeeded_job(kube_orchestrator)
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(original_pending_job)
        await storage.set_job(original_running_job)
        await storage.set_job(original_succeeded_job)
        await storage.set_job(original_failed_job)

        jobs = await storage.get_unfinished_jobs()
        jobs = sorted(jobs, key=lambda j: j.status)
        assert len(jobs) == 2
        pending_job = jobs[0]
        running_job = jobs[1]
        assert pending_job.id == original_pending_job.id
        assert running_job.id == original_running_job.id
        assert not pending_job.is_finished
        assert not running_job.is_finished

        filtered_jobs = await storage.get_all_jobs(job_filter=JOB_FILTER_UNFINISHED)
        assert len(filtered_jobs) == 2
        filtered_jobs_ids = {j.id for j in filtered_jobs}
        assert filtered_jobs_ids == {pending_job.id, running_job.id}

    @pytest.mark.asyncio
    async def test_get_for_deletion_empty(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        jobs = await storage.get_jobs_for_deletion()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_for_deletion(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        deleted_job = self._create_succeeded_job(kube_orchestrator, is_deleted=True)
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
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
        assert job.is_time_for_deletion

    @pytest.mark.asyncio
    async def test_job_lifecycle(
        self, redis_client, kube_orchestrator, create_storage, storage_class
    ):
        job = self._create_pending_job(kube_orchestrator)
        job_id = job.id
        storage = create_storage(
            storage_class_name=storage_class,
            client=redis_client,
            orchestrator_config=kube_orchestrator.config,
        )
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == job_id
        assert job.status == JobStatus.PENDING

        jobs = await storage.get_running_jobs()
        assert jobs == []

        jobs = await storage.get_jobs_for_deletion()
        assert jobs == []

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
        assert jobs == []

        job.status = JobStatus.FAILED
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1

        jobs = await storage.get_running_jobs()
        assert jobs == []

        jobs = await storage.get_jobs_for_deletion()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == job_id
        assert job.status == JobStatus.FAILED
        assert not job.is_deleted

        job.is_deleted = True
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 0

        jobs = await storage.get_running_jobs()
        assert jobs == []

        jobs = await storage.get_jobs_for_deletion()
        assert jobs == []
