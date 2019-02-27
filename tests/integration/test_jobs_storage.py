from typing import Optional

import pytest

from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import (
    JobFilter,
    JobsStorageException,
    JobStorageTransactionError,
    RedisJobsStorage,
)


class TestRedisJobsStorage:
    def _create_job_request(self, job_name: Optional[str] = None):
        container = Container(
            image="ubuntu",
            command="sleep 5",
            resources=ContainerResources(cpu=0.1, memory_mb=256),
        )
        return JobRequest.create(container, job_name=job_name)

    def _create_pending_job(self, kube_orchestrator, job_name: Optional[str] = None):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(job_name=job_name),
        )

    def _create_running_job(self, kube_orchestrator, job_name: Optional[str] = None):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(job_name=job_name),
            status=JobStatus.RUNNING,
        )

    def _create_succeeded_job(
        self, kube_orchestrator, is_deleted=False, job_name: Optional[str] = None
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(job_name=job_name),
            status=JobStatus.SUCCEEDED,
            is_deleted=is_deleted,
        )

    def _create_failed_job(
        self, kube_orchestrator, is_deleted=False, job_name: Optional[str] = None
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(job_name=job_name),
            status=JobStatus.FAILED,
            is_deleted=is_deleted,
        )

    @pytest.mark.asyncio
    async def test_set_get(self, redis_client, kube_orchestrator):
        original_job = self._create_pending_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_try_create_job__single_job(self, redis_client, kube_orchestrator):
        original_job = self._create_pending_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.try_create_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_try_create_job__name_conflict_with_pending_job(
        self, redis_client, kube_orchestrator
    ):
        job_name = "some-test-job-name"
        first_job = self._create_pending_job(kube_orchestrator, job_name=job_name)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.try_create_job(first_job)

        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job.status

        second_job = self._create_pending_job(kube_orchestrator, job_name=job_name)
        with pytest.raises(
            JobsStorageException,
            match=f"job with name '{job_name}' and owner '{first_job.owner}'"
            f" already exists: {first_job.id}",
        ):
            await storage.try_create_job(second_job)

    @pytest.mark.asyncio
    async def test_try_create_job__name_conflict_with_running_job(
        self, redis_client, kube_orchestrator
    ):
        job_name = "some-test-job-name"
        first_job = self._create_running_job(kube_orchestrator, job_name=job_name)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.try_create_job(first_job)

        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job.status

        second_job = self._create_pending_job(kube_orchestrator, job_name=job_name)
        with pytest.raises(
            JobsStorageException,
            match=f"job with name '{job_name}' and owner '{first_job.owner}'"
            f" already exists: {first_job.id}",
        ):
            await storage.try_create_job(second_job)

    @pytest.mark.asyncio
    async def test_try_create_job__same_name_with_succeeded_job(
        self, redis_client, kube_orchestrator
    ):
        job_name = "some-test-job-name"
        first_job = self._create_succeeded_job(kube_orchestrator, job_name=job_name)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.try_create_job(first_job)

        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job.status

        second_job = self._create_pending_job(kube_orchestrator, job_name=job_name)
        await storage.try_create_job(second_job)

        job = await storage.get_job(second_job.id)
        assert job.id == second_job.id
        assert job.status == second_job.status

    @pytest.mark.asyncio
    async def test_try_create_job__same_name_with_failed_job(
        self, redis_client, kube_orchestrator
    ):
        job_name = "some-test-job-name"
        first_job = self._create_failed_job(kube_orchestrator, job_name=job_name)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.try_create_job(first_job)

        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job.status

        second_job = self._create_pending_job(kube_orchestrator, job_name=job_name)
        await storage.try_create_job(second_job)

        job = await storage.get_job(second_job.id)
        assert job.id == second_job.id
        assert job.status == second_job.status

    @pytest.mark.asyncio
    async def test_get_non_existent(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        with pytest.raises(JobError, match="no such job unknown"):
            await storage.get_job("unknown")

    @pytest.mark.asyncio
    async def test_get_all_no_filter_empty_result(
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        jobs = await storage.get_all_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_all_no_filter_single_job(self, redis_client, kube_orchestrator):
        original_job = self._create_pending_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.set_job(original_job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_get_all_filter_by_status(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        storage = RedisJobsStorage(
            client=redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_all_jobs()
        job_ids = {job.id for job in jobs}
        assert job_ids == {pending_job.id, running_job.id, succeeded_job.id}

        filters = JobFilter(statuses={JobStatus.FAILED})
        jobs = await storage.get_all_jobs(filters)
        job_ids = {job.id for job in jobs}
        assert job_ids == set()

        filters = JobFilter(statuses={JobStatus.SUCCEEDED, JobStatus.RUNNING})
        jobs = await storage.get_all_jobs(filters)
        job_ids = {job.id for job in jobs}
        assert job_ids == {succeeded_job.id, running_job.id}

    @pytest.mark.asyncio
    async def test_get_running_empty(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        jobs = await storage.get_running_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_running(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == running_job.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_get_unfinished_empty(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        jobs = await storage.get_unfinished_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_unfinished(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_unfinished_jobs()
        assert len(jobs) == 2
        assert {job.id for job in jobs} == {pending_job.id, running_job.id}
        assert all([not job.is_finished for job in jobs])

    @pytest.mark.asyncio
    async def test_get_for_deletion_empty(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_for_deletion(self, redis_client, kube_orchestrator):
        pending_job = self._create_pending_job(kube_orchestrator)
        running_job = self._create_running_job(kube_orchestrator)
        succeeded_job = self._create_succeeded_job(kube_orchestrator)
        deleted_job = self._create_succeeded_job(kube_orchestrator, is_deleted=True)
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
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

    @pytest.mark.asyncio
    async def test_job_lifecycle(self, redis_client, kube_orchestrator):
        job = self._create_pending_job(kube_orchestrator)
        job_id = job.id
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
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

    @pytest.mark.asyncio
    async def test_try_update_job_success(
        self, redis_client, kube_orchestrator
    ) -> None:
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        pending_job = self._create_pending_job(kube_orchestrator)
        await storage.set_job(pending_job)

        async with storage.try_update_job(pending_job.id) as job:
            assert pending_job.status == JobStatus.PENDING
            job.status = JobStatus.RUNNING

        running_job = await storage.get_job(pending_job.id)
        assert running_job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_try_update_job_conflict(
        self, redis_client, kube_orchestrator
    ) -> None:
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        pending_job = self._create_pending_job(kube_orchestrator)
        await storage.set_job(pending_job)

        with pytest.raises(
            JobStorageTransactionError,
            match=f"Key jobs:{pending_job.id} has been changed.",
        ):

            async with storage.try_update_job(pending_job.id) as first_job:
                assert first_job.status == JobStatus.PENDING

                with pytest.not_raises(JobStorageTransactionError):

                    async with storage.try_update_job(pending_job.id) as second_job:
                        assert pending_job.status == JobStatus.PENDING
                        second_job.status = JobStatus.FAILED

                first_job.status = JobStatus.FAILED

        failed_job = await storage.get_job(pending_job.id)
        assert failed_job.status == JobStatus.FAILED
