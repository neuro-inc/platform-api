from datetime import datetime, timedelta
from itertools import islice
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Tuple

import aioredis
import pytest
from yarl import URL

from platform_api.orchestrator.job import (
    JobRecord,
    JobStatusHistory,
    JobStatusItem,
    current_datetime_factory,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    ContainerVolume,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import (
    JobFilter,
    JobStorageJobFoundError,
    JobStorageTransactionError,
    RedisJobsStorage,
)
from tests.conftest import not_raises, random_str


class TestRedisJobsStorage:
    def _create_job_request(self, with_gpu: bool = False) -> JobRequest:
        if with_gpu:
            resources = ContainerResources(
                cpu=0.1, memory_mb=256, gpu=1, gpu_model_id="nvidia-tesla-k80"
            )
        else:
            resources = ContainerResources(cpu=0.1, memory_mb=256)
        container = Container(image="ubuntu", command="sleep 5", resources=resources)
        return JobRequest.create(container)

    def _create_job(
        self, cluster_name: str = "test-cluster", **kwargs: Any
    ) -> JobRecord:
        return JobRecord.create(
            request=self._create_job_request(), cluster_name=cluster_name, **kwargs
        )

    def _create_pending_job(
        self, owner: str = "compute", job_name: Optional[str] = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(owner=owner, name=job_name, **kwargs)

    def _create_running_job(
        self, owner: str = "compute", job_name: Optional[str] = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(
            name=job_name, owner=owner, status=JobStatus.RUNNING, **kwargs
        )

    def _create_succeeded_job(
        self, owner: str = "compute", job_name: Optional[str] = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(
            name=job_name, status=JobStatus.SUCCEEDED, owner=owner, **kwargs
        )

    def _create_failed_job(
        self, owner: str = "compute", job_name: Optional[str] = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(
            name=job_name, status=JobStatus.FAILED, owner=owner, **kwargs
        )

    def _create_cancelled_job(
        self, owner: str = "compute", job_name: Optional[str] = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(
            name=job_name, status=JobStatus.CANCELLED, owner=owner, **kwargs
        )

    @pytest.mark.asyncio
    async def test_set_get(self, redis_client: aioredis.Redis) -> None:
        original_job = self._create_pending_job()
        storage = RedisJobsStorage(redis_client)
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__no_job_updated(
        self, redis_client: aioredis.Redis
    ) -> None:
        job = self._create_pending_job(owner="me", job_name="jobname1")
        storage = RedisJobsStorage(redis_client)
        job.status = JobStatus.RUNNING

        # do not put this job into redis

        # check that job exists in database:
        with pytest.raises(JobError, match=f"no such job {job.id}"):
            await storage.get_job(job.id)

        assert job.name
        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created is None

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__is_job_creation_false(
        self, redis_client: aioredis.Redis
    ) -> None:

        job = self._create_pending_job(owner="me", job_name="jobname1")
        storage = RedisJobsStorage(redis_client)
        job.status = JobStatus.RUNNING
        await storage.update_job_atomic(job, is_job_creation=False)

        # check that job exists in database:
        job_read = await storage.get_job(job.id)
        assert job_read.to_primitive() == job.to_primitive()

        assert job.name
        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created is None

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__is_job_creation_true(
        self, redis_client: aioredis.Redis
    ) -> None:
        job = self._create_pending_job(owner="me", job_name="jobname1")
        storage = RedisJobsStorage(redis_client)
        job.status = JobStatus.RUNNING
        await storage.update_job_atomic(job, is_job_creation=True)

        # check that job exists in database:
        job_read = await storage.get_job(job.id)
        assert job_read.to_primitive() == job.to_primitive()

        assert job.name
        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created == job.id

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__multiple_jobs_order_preserves(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        owner = "me"
        name = "jobnamename"

        job1 = self._create_pending_job(owner=owner, job_name=name)
        await storage.update_job_atomic(job1, is_job_creation=True)

        job2 = self._create_failed_job(owner=owner, job_name=name)
        await storage.update_job_atomic(job2, is_job_creation=True)

        job3 = self._create_succeeded_job(owner=owner, job_name=name)
        await storage.update_job_atomic(job3, is_job_creation=True)

        job_id_last_created = await storage.get_last_created_job_id(owner, name)
        assert job_id_last_created == job3.id

    @pytest.mark.asyncio
    async def test_try_create_job__no_name__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        pending_job = self._create_pending_job()
        async with storage.try_create_job(pending_job) as job:
            assert pending_job.status == JobStatus.PENDING
            assert job.id == pending_job.id
            job.status = JobStatus.RUNNING
        result_job = await storage.get_job(pending_job.id)
        assert result_job.status == JobStatus.RUNNING

        running_job = self._create_running_job()
        async with storage.try_create_job(running_job) as job:
            assert running_job.status == JobStatus.RUNNING
            assert job.id == running_job.id
            job.status = JobStatus.SUCCEEDED
        result_job = await storage.get_job(running_job.id)
        assert result_job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_try_create_job__no_name__job_changed_while_creation(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        job = self._create_pending_job()

        # process-1
        with pytest.raises(
            JobStorageTransactionError, match=f"Job {{id={job.id}}} has changed"
        ):
            async with storage.try_create_job(job) as first_job:
                # value in orchestrator: PENDING, value in redis: None
                assert first_job.status == JobStatus.PENDING
                first_job.status = JobStatus.SUCCEEDED
                # value in orchestrator: SUCCEEDED, value in redis: None

                # process-2
                with not_raises(JobStorageTransactionError):
                    async with storage.try_create_job(job) as second_job:
                        # value in orchestrator: succeeded, value in redis: None
                        assert second_job.status == JobStatus.SUCCEEDED
                        second_job.status = JobStatus.RUNNING
                        # value in orchestrator: FAILED, value in redis: None
                        # now status FAILED is written into the redis by process-2

                # now status SUCCEEDED fails to be written into the redis by process-1

                # now jobs-service catches transaction exception thrown
                # by process-1 and deletes the newly created by process-1 job.

        result_job = await storage.get_job(job.id)
        assert result_job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_try_create_job__different_name_same_owner__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        owner = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(job_name=job_name_1, owner=owner)
        job_2 = self._create_running_job(job_name=job_name_2, owner=owner)

        async with storage.try_create_job(job_1):
            pass
        async with storage.try_create_job(job_2):
            pass

        job = await storage.get_job(job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.PENDING

        job = await storage.get_job(job_2.id)
        assert job.id == job_2.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_try_create_job__same_name_different_owner__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        owner_1 = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        owner_2 = "test-user-2"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(job_name=job_name_1, owner=owner_1)
        job_2 = self._create_running_job(job_name=job_name_2, owner=owner_2)

        async with storage.try_create_job(job_1):
            pass
        async with storage.try_create_job(job_2):
            pass

        job = await storage.get_job(job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.PENDING

        job = await storage.get_job(job_2.id)
        assert job.id == job_2.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("first_job_status", [JobStatus.PENDING, JobStatus.RUNNING])
    async def test_try_create_job__same_name_with_an_active_job__conflict(
        self, redis_client: aioredis.Redis, first_job_status: JobStatus
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = self._create_job(
            name=job_name, status=first_job_status, owner=owner
        )
        async with storage.try_create_job(first_job):
            pass
        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        second_job = self._create_pending_job(job_name=job_name, owner=owner)
        with pytest.raises(
            JobStorageJobFoundError,
            match=f"job with name '{job_name}' and owner '{first_job.owner}'"
            f" already exists: '{first_job.id}'",
        ):
            async with storage.try_create_job(second_job):
                pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    async def test_try_create_job__same_name_with_a_terminated_job__ok(
        self, redis_client: aioredis.Redis, first_job_status: JobStatus
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = self._create_job(
            name=job_name, status=first_job_status, owner=owner
        )
        second_job = self._create_pending_job(owner=owner, job_name=job_name)

        # create first job:
        async with storage.try_create_job(first_job):
            pass
        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        # create second job:
        async with storage.try_create_job(second_job):
            pass
        job = await storage.get_job(second_job.id)
        assert job.id == second_job.id
        assert job.status == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_try_create_job_with_tags(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(redis_client)

        tags = ["tag1", "tag2"]
        job = self._create_job(tags=tags)
        async with storage.try_create_job(job) as job:
            assert job.id == job.id
            assert job.tags == tags

        result_job = await storage.get_job(job.id)
        assert result_job.tags == tags

    @pytest.mark.asyncio
    async def test_get_non_existent(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(redis_client)
        with pytest.raises(JobError, match="no such job unknown"):
            await storage.get_job("unknown")

    @pytest.mark.asyncio
    async def test_get_all_no_filter_empty_result(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        jobs = await storage.get_all_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_all_no_filter_single_job(
        self, redis_client: aioredis.Redis
    ) -> None:
        original_job = self._create_pending_job()
        storage = RedisJobsStorage(redis_client)
        await storage.set_job(original_job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == original_job.id
        assert job.status == original_job.status

    @pytest.mark.asyncio
    async def test_get_all_no_filter_multiple_jobs(
        self, redis_client: aioredis.Redis
    ) -> None:
        original_jobs = [
            self._create_pending_job(job_name="jobname1"),
            self._create_running_job(job_name="jobname2"),
            self._create_succeeded_job(job_name="jobname3"),
            self._create_failed_job(job_name="jobname3"),
        ]
        storage = RedisJobsStorage(redis_client)
        for job in original_jobs:
            async with storage.try_create_job(job):
                pass

        jobs = await storage.get_all_jobs()

        job_reprs = sorted(
            [job.to_primitive() for job in jobs], key=lambda job: job["id"]
        )
        original_job_reprs = sorted(
            [job.to_primitive() for job in original_jobs], key=lambda job: job["id"]
        )
        assert job_reprs == original_job_reprs

    @pytest.mark.asyncio
    async def test_get_all_filter_by_status(self, redis_client: aioredis.Redis) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        storage = RedisJobsStorage(client=redis_client)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_all_jobs()
        job_ids = [job.id for job in jobs]
        assert job_ids == [pending_job.id, running_job.id, succeeded_job.id]

        jobs = await storage.get_all_jobs(reverse=True)
        job_ids = [job.id for job in jobs]
        assert job_ids == [succeeded_job.id, running_job.id, pending_job.id]

        jobs = await storage.get_all_jobs(limit=2)
        job_ids = [job.id for job in jobs]
        assert job_ids == [pending_job.id, running_job.id]

        jobs = await storage.get_all_jobs(reverse=True, limit=2)
        job_ids = [job.id for job in jobs]
        assert job_ids == [succeeded_job.id, running_job.id]

        filters = JobFilter(statuses={JobStatus.FAILED})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == []

        filters = JobFilter(statuses={JobStatus.SUCCEEDED, JobStatus.RUNNING})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [running_job.id, succeeded_job.id]

        jobs = await storage.get_all_jobs(filters, reverse=True)
        job_ids = [job.id for job in jobs]
        assert job_ids == [succeeded_job.id, running_job.id]

        jobs = await storage.get_all_jobs(filters, limit=1)
        job_ids = [job.id for job in jobs]
        assert job_ids == [running_job.id]

        jobs = await storage.get_all_jobs(filters, reverse=True, limit=1)
        job_ids = [job.id for job in jobs]
        assert job_ids == [succeeded_job.id]

    @pytest.mark.asyncio
    async def test_get_all_filter_by_tags(self, redis_client: aioredis.Redis) -> None:
        job1 = self._create_job(tags=["t1"])
        job2 = self._create_job(tags=["t1", "t2"])
        job3 = self._create_job(tags=["t2"])
        job4 = self._create_job(tags=["t3"])

        storage = RedisJobsStorage(client=redis_client)
        await storage.set_job(job1)
        await storage.set_job(job2)
        await storage.set_job(job3)
        await storage.set_job(job4)

        jobs = await storage.get_all_jobs()
        job_ids = [job.id for job in jobs]
        assert job_ids == [job1.id, job2.id, job3.id, job4.id]

        filters = JobFilter(tags={"t1"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job1.id, job2.id]

        filters = JobFilter(tags={"t1", "t2"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job2.id]

        jobs = await storage.get_all_jobs(filters, limit=1)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job2.id]

        jobs = await storage.get_all_jobs(filters, limit=1, reverse=True)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job2.id]

        filters = JobFilter(tags={"t3"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job4.id]

        filters = JobFilter(tags={"t1", "t2", "t3"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert not job_ids

    @pytest.mark.asyncio
    @pytest.mark.parametrize("statuses", [(), (JobStatus.PENDING, JobStatus.RUNNING)])
    async def test_get_all_filter_by_date_range(
        self, statuses: Tuple[JobStatus, ...], redis_client: aioredis.Redis
    ) -> None:
        t1 = current_datetime_factory()
        job1 = self._create_job()
        t2 = current_datetime_factory()
        job2 = self._create_job()
        t3 = current_datetime_factory()
        job3 = self._create_job()
        t4 = current_datetime_factory()

        storage = RedisJobsStorage(client=redis_client)
        await storage.set_job(job1)
        await storage.set_job(job2)
        await storage.set_job(job3)

        job_filter = JobFilter(since=t1, until=t4)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job1.id, job2.id, job3.id]

        job_ids = [
            job.id for job in await storage.get_all_jobs(job_filter, reverse=True)
        ]
        assert job_ids == [job3.id, job2.id, job1.id]

        job_ids = [job.id for job in await storage.get_all_jobs(job_filter, limit=2)]
        assert job_ids == [job1.id, job2.id]

        job_ids = [
            job.id
            for job in await storage.get_all_jobs(job_filter, reverse=True, limit=2)
        ]
        assert job_ids == [job3.id, job2.id]

        job_filter = JobFilter(since=t2)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job2.id, job3.id]

        job_ids = [
            job.id for job in await storage.get_all_jobs(job_filter, reverse=True)
        ]
        assert job_ids == [job3.id, job2.id]

        job_ids = [job.id for job in await storage.get_all_jobs(job_filter, limit=1)]
        assert job_ids == [job2.id]

        job_ids = [
            job.id
            for job in await storage.get_all_jobs(job_filter, reverse=True, limit=1)
        ]
        assert job_ids == [job3.id]

        job_filter = JobFilter(until=t2)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job1.id]

        job_filter = JobFilter(since=t3)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job3.id]

        job_filter = JobFilter(until=t3)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job1.id, job2.id]

        job_ids = [
            job.id for job in await storage.get_all_jobs(job_filter, reverse=True)
        ]
        assert job_ids == [job2.id, job1.id]

        job_ids = [job.id for job in await storage.get_all_jobs(job_filter, limit=1)]
        assert job_ids == [job1.id]

        job_ids = [
            job.id
            for job in await storage.get_all_jobs(job_filter, reverse=True, limit=1)
        ]
        assert job_ids == [job2.id]

        job_filter = JobFilter(since=t2, until=t3)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job2.id]

        job_filter = JobFilter(since=t3, until=t2)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

    async def prepare_filtering_test(
        self, redis_client: aioredis.Redis
    ) -> Tuple[RedisJobsStorage, List[JobRecord]]:
        jobs = [
            # no name:
            self._create_pending_job(owner="user1", job_name=None),
            self._create_running_job(owner="user2", job_name=None),
            self._create_succeeded_job(owner="user3", job_name=None),
            self._create_failed_job(owner="user3", job_name=None),
            self._create_cancelled_job(owner="user3", job_name=None),
            # user1, jobname1:
            self._create_succeeded_job(owner="user1", job_name="jobname1"),
            self._create_failed_job(owner="user1", job_name="jobname1"),
            self._create_cancelled_job(owner="user1", job_name="jobname1"),
            self._create_pending_job(owner="user1", job_name="jobname1"),
            # user1, jobname2:
            self._create_succeeded_job(owner="user1", job_name="jobname2"),
            self._create_failed_job(owner="user1", job_name="jobname2"),
            self._create_cancelled_job(owner="user1", job_name="jobname2"),
            self._create_running_job(owner="user1", job_name="jobname2"),
            # user2, jobname2:
            self._create_succeeded_job(owner="user2", job_name="jobname2"),
            self._create_failed_job(owner="user2", job_name="jobname2"),
            self._create_cancelled_job(owner="user2", job_name="jobname2"),
            self._create_pending_job(owner="user2", job_name="jobname2"),
            # user2, jobname3:
            self._create_succeeded_job(owner="user2", job_name="jobname3"),
            self._create_failed_job(owner="user2", job_name="jobname3"),
            self._create_cancelled_job(owner="user2", job_name="jobname3"),
            self._create_running_job(owner="user2", job_name="jobname3"),
            # user3, jobname3:
            self._create_succeeded_job(owner="user3", job_name="jobname3"),
            self._create_failed_job(owner="user3", job_name="jobname3"),
            self._create_cancelled_job(owner="user3", job_name="jobname3"),
            self._create_pending_job(owner="user3", job_name="jobname3"),
        ]
        storage = RedisJobsStorage(client=redis_client)
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return storage, jobs

    @pytest.mark.asyncio
    async def test_get_all_filter_by_single_owner(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        owners = {"user1"}
        job_filter = JobFilter(owners=owners)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.owner in owners]
        assert expected
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_multiple_owners(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        owners = {"user1", "user3"}
        job_filter = JobFilter(owners=owners)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.owner in owners]
        assert expected
        assert job_ids == expected

    pend = JobStatus.PENDING
    runn = JobStatus.RUNNING
    succ = JobStatus.SUCCEEDED
    canc = JobStatus.CANCELLED
    fail = JobStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,owners,statuses",
        [
            (None, (), ()),
            (None, (), (pend,)),
            (None, (), (pend, runn)),
            (None, (), (succ, fail)),
            (None, (), (succ, fail, runn)),
            (None, (), (succ, fail, runn, pend)),
            (None, (), (succ, canc, fail, runn, pend)),
            (None, ("user1",), ()),
            (None, ("user1",), (pend,)),
            (None, ("user1",), (pend, runn)),
            (None, ("user1",), (succ, fail)),
            (None, ("user1",), (succ, fail, runn, pend)),
            (None, ("user1",), (succ, canc, fail, runn, pend)),
            (None, ("user1", "user2"), ()),
            (None, ("user1", "user2"), (pend,)),
            (None, ("user1", "user2"), (pend, runn)),
            (None, ("user1", "user2"), (succ, fail)),
            (None, ("user1", "user2"), (succ, fail, runn, pend)),
            (None, ("user1", "user2"), (succ, canc, fail, runn, pend)),
            ("jobname1", ("user1",), ()),
            ("jobname1", ("user1",), (pend,)),
            ("jobname1", ("user1",), (pend, runn)),
            ("jobname1", ("user1",), (succ, fail)),
            ("jobname1", ("user1",), (succ, fail, runn, pend)),
            ("jobname1", ("user1",), (succ, canc, fail, runn, pend)),
            ("jobname1", ("user1", "user2"), ()),
            ("jobname1", ("user1", "user2"), (pend,)),
            ("jobname1", ("user1", "user2"), (pend, runn)),
            ("jobname1", ("user1", "user2"), (succ, fail)),
            ("jobname1", ("user1", "user2"), (succ, fail, runn, pend)),
            ("jobname1", ("user1", "user2"), (succ, canc, fail, runn, pend)),
        ],
    )
    async def test_get_all_with_filters(
        self,
        owners: Tuple[str, ...],
        name: Optional[str],
        statuses: Tuple[JobStatus, ...],
        redis_client: aioredis.Redis,
    ) -> None:
        def sort_jobs_as_primitives(array: List[JobRecord]) -> List[Dict[str, Any]]:
            return sorted(
                [job.to_primitive() for job in array], key=lambda job: job["id"]
            )

        storage, jobs = await self.prepare_filtering_test(redis_client)
        job_filter = JobFilter(name=name, owners=set(owners), statuses=set(statuses))
        actual = sort_jobs_as_primitives(await storage.get_all_jobs(job_filter))
        expected = sort_jobs_as_primitives(
            [
                job
                for job in jobs
                if (not name or job.name == name)
                and (not owners or job.owner in owners)
                and (not statuses or job.status in statuses)
            ]
        )
        assert actual == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name,statuses",
        [
            ("jobname1", ()),
            ("jobname1", (pend,)),
            ("jobname1", (pend, runn)),
            ("jobname1", (succ, fail)),
            ("jobname1", (succ, fail, runn)),
            ("jobname1", (succ, fail, runn, pend)),
        ],
    )
    async def test_get_all_filter_by_name_with_no_owner(
        self,
        name: Optional[str],
        statuses: Tuple[JobStatus, ...],
        redis_client: aioredis.Redis,
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)
        job_filter = JobFilter(name=name, owners=set(), statuses=set(statuses))
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and (not statuses or job.status in statuses)
        ]
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_owner_and_name(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        name = "jobname2"
        owner = "user1"

        job_filter = JobFilter(name=name, owners={owner})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.name == name and job.owner == owner]
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_owner_name_and_status(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        name = "jobname2"
        owner = "user1"
        statuses = {JobStatus.RUNNING}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        ]
        assert job_ids == expected

        name = "jobname2"
        owner = "user1"
        statuses = {JobStatus.SUCCEEDED}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        ]
        assert job_ids == expected

        name = "jobname2"
        owner = "user1"
        statuses = {JobStatus.SUCCEEDED, JobStatus.RUNNING}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        ]
        assert job_ids == expected

        name = "jobname3"
        owner = "user2"
        statuses = {JobStatus.FAILED}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        ]
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_shared_by_name(self, redis_client: aioredis.Redis) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        job_filter = JobFilter(
            clusters={"test-cluster": {"user1": {"jobname2"}, "user2": {"jobname3"}}},
            owners={"user1", "user2"},
            statuses={JobStatus.FAILED},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.status == JobStatus.FAILED
            and (
                (job.owner == "user1" and job.name == "jobname2")
                or (job.owner == "user2" and job.name == "jobname3")
            )
        ]
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_hostname(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        job1, job2 = islice(jobs, 2)

        job_filter = JobFilter(ids={job1.id})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job1.id}

        job_filter = JobFilter(ids={job1.id, job2.id})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job1.id, job2.id}

    @pytest.mark.asyncio
    async def test_get_all_filter_by_hostname_and_status(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test(redis_client)

        running_job_ids = {
            job.id
            for job in islice(
                (job for job in jobs if job.status == JobStatus.RUNNING), 2
            )
        }
        succeeded_job_ids = {
            job.id
            for job in islice(
                (job for job in jobs if job.status == JobStatus.SUCCEEDED), 2
            )
        }

        statuses = {JobStatus.RUNNING}
        job_filter = JobFilter(ids=running_job_ids, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == running_job_ids

        statuses = {JobStatus.SUCCEEDED, JobStatus.RUNNING}
        job_filter = JobFilter(ids=running_job_ids, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == running_job_ids

        statuses = {JobStatus.SUCCEEDED}
        job_filter = JobFilter(ids=running_job_ids, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == set()

        statuses = {JobStatus.FAILED, JobStatus.RUNNING}
        job_filter = JobFilter(
            ids=running_job_ids | succeeded_job_ids, statuses=statuses
        )
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == running_job_ids

        statuses = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.RUNNING}
        job_filter = JobFilter(
            ids=running_job_ids | succeeded_job_ids, statuses=statuses
        )
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert set(job_ids) == running_job_ids | succeeded_job_ids

    async def prepare_filtering_test_different_clusters(
        self, redis_client: aioredis.Redis
    ) -> Tuple[RedisJobsStorage, List[JobRecord]]:
        jobs = [
            self._create_running_job(owner="user1", cluster_name="test-cluster"),
            self._create_succeeded_job(
                owner="user1", cluster_name="test-cluster", job_name="jobname1"
            ),
            self._create_failed_job(
                owner="user2", cluster_name="test-cluster", job_name="jobname1"
            ),
            self._create_succeeded_job(
                owner="user3", cluster_name="test-cluster", job_name="jobname1"
            ),
            self._create_succeeded_job(owner="user1", cluster_name="my-cluster"),
            self._create_failed_job(owner="user3", cluster_name="my-cluster"),
            self._create_failed_job(owner="user1", cluster_name="other-cluster"),
            self._create_succeeded_job(owner="user2", cluster_name="other-cluster"),
            self._create_running_job(owner="user3", cluster_name="other-cluster"),
        ]
        storage = RedisJobsStorage(client=redis_client)
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return storage, jobs

    @pytest.mark.asyncio
    async def test_get_all_filter_by_cluster(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test_different_clusters(
            redis_client
        )

        job_filter = JobFilter(clusters={"test-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job.id for job in jobs[:4]]

        job_filter = JobFilter(clusters={"test-cluster": {}, "my-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job.id for job in jobs[:6]]

        job_filter = JobFilter(clusters={"nonexisting-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

    @pytest.mark.asyncio
    async def test_get_all_filter_by_cluster_and_owner(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test_different_clusters(
            redis_client
        )

        job_filter = JobFilter(clusters={"test-cluster": {}}, owners={"user1"})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {}, "my-cluster": {}}, owners={"user1"}
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[4].id]

        job_filter = JobFilter(clusters={"test-cluster": {}}, owners={"user1", "user2"})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[2].id]

        job_filter = JobFilter(clusters={"my-cluster": {}}, owners={"user2"})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

        job_filter = JobFilter(
            clusters={
                "test-cluster": {"user1": set()},
                "other-cluster": {"user2": set()},
            },
            owners={"user1", "user2"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {"user1": set()}, "other-cluster": {}},
            owners={"user1", "user2"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[6].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={
                "test-cluster": {"user1": set(), "user2": set()},
                "my-cluster": {"user2": set(), "user3": set()},
                "other-cluster": {"user2": set()},
            },
            owners={"user1", "user2", "user3"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[2].id, jobs[5].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {"user1": set()}, "other-cluster": {}},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[6].id, jobs[7].id, jobs[8].id]

    @pytest.mark.asyncio
    async def test_get_all_filter_by_cluster_and_name(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test_different_clusters(
            redis_client
        )

        job_filter = JobFilter(
            clusters={"test-cluster": {}}, owners={"user1", "user2"}, name="jobname1"
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id, jobs[2].id]

        job_filter = JobFilter(clusters={"test-cluster": {}}, name="jobname1")
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id, jobs[2].id, jobs[3].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {}}, owners={"user1", "user2"}, name="jobname2"
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

        job_filter = JobFilter(clusters={"test-cluster": {}}, name="jobname2")
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

    @pytest.mark.asyncio
    async def test_get_all_filter_by_cluster_and_status(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage, jobs = await self.prepare_filtering_test_different_clusters(
            redis_client
        )

        job_filter = JobFilter(
            clusters={"test-cluster": {}}, statuses={JobStatus.SUCCEEDED}
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id, jobs[3].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {}, "my-cluster": {}},
            statuses={JobStatus.SUCCEEDED},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id, jobs[3].id, jobs[4].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {}},
            statuses={JobStatus.RUNNING, JobStatus.SUCCEEDED},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[3].id]

        job_filter = JobFilter(
            clusters={"my-cluster": {}}, statuses={JobStatus.RUNNING}
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

    @pytest.mark.asyncio
    async def test_get_running_empty(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(redis_client)

        jobs = await storage.get_running_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_running(self, redis_client: aioredis.Redis) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        storage = RedisJobsStorage(redis_client)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == running_job.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_get_unfinished_empty(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(redis_client)
        jobs = await storage.get_unfinished_jobs()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_unfinished(self, redis_client: aioredis.Redis) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        storage = RedisJobsStorage(redis_client)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_unfinished_jobs()
        assert len(jobs) == 2
        assert [job.id for job in jobs] == [pending_job.id, running_job.id]
        assert all([not job.is_finished for job in jobs])

    @pytest.mark.asyncio
    async def test_get_for_deletion_empty(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(redis_client)

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_for_deletion(self, redis_client: aioredis.Redis) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        deleted_job = self._create_succeeded_job(is_deleted=True)
        storage = RedisJobsStorage(redis_client)
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
    async def test_get_tags_empty(self, redis_client: aioredis.Redis) -> None:
        jobs_storage = RedisJobsStorage(redis_client)
        for job in [
            self._create_job(owner="u", tags=["b"]),
            self._create_job(owner="u", tags=["a"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("another")
        assert tags_u1 == []

    @pytest.mark.asyncio
    async def test_get_tags_single(self, redis_client: aioredis.Redis) -> None:
        jobs_storage = RedisJobsStorage(redis_client)
        f1 = lambda: datetime(year=2020, month=1, day=1, second=1)  # noqa
        f2 = lambda: datetime(year=2020, month=1, day=1, second=2)  # noqa
        f3 = lambda: datetime(year=2020, month=1, day=1, second=3)  # noqa

        for job in [
            self._create_job(owner="u", current_datetime_factory=f1, tags=["b"]),
            self._create_job(owner="u", current_datetime_factory=f2, tags=["a"]),
            self._create_job(owner="u", current_datetime_factory=f3, tags=["c"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["c", "a", "b"]

    @pytest.mark.asyncio
    async def test_get_tags_multiple(self, redis_client: aioredis.Redis) -> None:
        jobs_storage = RedisJobsStorage(redis_client)
        f1 = lambda: datetime(year=2020, month=1, day=1, second=1)  # noqa
        f2 = lambda: datetime(year=2020, month=1, day=1, second=2)  # noqa

        for job in [
            self._create_job(
                owner="u", current_datetime_factory=f1, tags=["b", "a", "c"]
            ),
            self._create_job(owner="u", current_datetime_factory=f2, tags=["d"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["d", "a", "b", "c"]

    @pytest.mark.asyncio
    async def test_get_tags_overwrite_single(
        self, redis_client: aioredis.Redis
    ) -> None:
        jobs_storage = RedisJobsStorage(redis_client)
        f1 = lambda: datetime(year=2020, month=1, day=1, second=1)  # noqa
        f2 = lambda: datetime(year=2020, month=1, day=1, second=2)  # noqa
        f3 = lambda: datetime(year=2020, month=1, day=1, second=3)  # noqa
        f4 = lambda: datetime(year=2020, month=1, day=1, second=4)  # noqa

        for job in [
            self._create_job(owner="u", current_datetime_factory=f1, tags=["a"]),
            self._create_job(owner="u", current_datetime_factory=f2, tags=["b"]),
            self._create_job(owner="u", current_datetime_factory=f3, tags=["a"]),
            self._create_job(owner="u", current_datetime_factory=f4, tags=["c"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["c", "a", "b"]

    @pytest.mark.asyncio
    async def test_get_tags_overwrite_multiple(
        self, redis_client: aioredis.Redis
    ) -> None:
        jobs_storage = RedisJobsStorage(redis_client)
        f1 = lambda: datetime(year=2020, month=1, day=1, second=1)  # noqa
        f2 = lambda: datetime(year=2020, month=1, day=1, second=2)  # noqa
        f3 = lambda: datetime(year=2020, month=1, day=1, second=3)  # noqa

        for job in [
            self._create_job(owner="u", current_datetime_factory=f1, tags=["a"]),
            self._create_job(owner="u", current_datetime_factory=f2, tags=["b"]),
            self._create_job(owner="u", current_datetime_factory=f3, tags=["c", "a"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["a", "c", "b"]

    @pytest.mark.asyncio
    async def test_job_lifecycle(self, redis_client: aioredis.Redis) -> None:
        job = self._create_pending_job()
        job_id = job.id
        storage = RedisJobsStorage(redis_client)
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
    async def test_try_update_job__no_name__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        pending_job = self._create_pending_job()
        await storage.set_job(pending_job)

        async with storage.try_update_job(pending_job.id) as job:
            assert pending_job.status == JobStatus.PENDING
            job.status = JobStatus.RUNNING

        running_job = await storage.get_job(pending_job.id)
        assert running_job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_try_update_job__not_found(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        pending_job = self._create_pending_job()

        with pytest.raises(JobError, match=f"no such job {pending_job.id}"):
            async with storage.try_update_job(pending_job.id):
                pass

    @pytest.mark.asyncio
    async def test_try_update_job__no_name__job_changed_while_creation(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        job = self._create_pending_job()
        await storage.set_job(job)

        # process-1
        with pytest.raises(
            JobStorageTransactionError, match=f"Job {{id={job.id}}} has changed"
        ):
            async with storage.try_update_job(job.id) as first_job:
                # value in orchestrator: PENDING, value in redis: None
                assert first_job.status == JobStatus.PENDING
                first_job.status = JobStatus.SUCCEEDED
                # value in orchestrator: SUCCEEDED, value in redis: None

                # process-2
                with not_raises(JobStorageTransactionError):
                    async with storage.try_update_job(job.id) as second_job:
                        # value in orchestrator: succeeded, value in redis: None
                        assert second_job.status == JobStatus.PENDING
                        second_job.status = JobStatus.RUNNING
                        # value in orchestrator: FAILED, value in redis: None
                        # now status FAILED is written into the redis by process-2

                # now status SUCCEEDED fails to be written into the redis by process-1

                # now jobs-service catches transaction exception thrown
                # by process-1 and deletes the newly created by process-1 job.

        result_job = await storage.get_job(job.id)
        assert result_job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_try_update_job__different_name_same_owner__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        owner = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(job_name=job_name_1, owner=owner)
        job_2 = self._create_running_job(job_name=job_name_2, owner=owner)

        await storage.set_job(job_1)
        await storage.set_job(job_2)

        async with storage.try_update_job(job_1.id) as job_1_current:
            async with storage.try_update_job(job_2.id) as job_2_current:
                job_2_current.status = JobStatus.FAILED
            job_1_current.status = JobStatus.SUCCEEDED

        job = await storage.get_job(job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.SUCCEEDED

        job = await storage.get_job(job_2.id)
        assert job.id == job_2.id
        assert job.status == JobStatus.FAILED

    @pytest.mark.asyncio
    async def test_try_update_job__same_name_different_owner__ok(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(redis_client)

        owner_1 = "test-user-1"
        owner_2 = "test-user-2"
        job_name = "some-test-job-name"

        job_1 = self._create_pending_job(job_name=job_name, owner=owner_1)
        job_2 = self._create_running_job(job_name=job_name, owner=owner_2)

        await storage.set_job(job_1)
        await storage.set_job(job_2)

        async with storage.try_update_job(job_1.id) as job_1_current:
            async with storage.try_update_job(job_2.id) as job_2_current:
                job_2_current.status = JobStatus.FAILED
            job_1_current.status = JobStatus.SUCCEEDED

        job = await storage.get_job(job_1.id)
        assert job.id == job_1.id
        assert job.status == JobStatus.SUCCEEDED

        job = await storage.get_job(job_2.id)
        assert job.id == job_2.id
        assert job.status == JobStatus.FAILED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    async def test_try_update_job__same_name_with_a_terminated_job__ok(
        self, redis_client: aioredis.Redis, first_job_status: JobStatus
    ) -> None:
        storage = RedisJobsStorage(redis_client)
        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = self._create_job(
            name=job_name, status=first_job_status, owner=owner
        )
        second_job = self._create_pending_job(owner=owner, job_name=job_name)

        # create first job:
        async with storage.try_create_job(first_job):
            pass
        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        # create second job:
        async with storage.try_create_job(second_job):
            pass
        job = await storage.get_job(second_job.id)
        assert job.id == second_job.id
        assert job.status == JobStatus.PENDING

        # update second job:
        async with storage.try_update_job(second_job.id) as job:
            job.status = JobStatus.RUNNING
        job = await storage.get_job(second_job.id)
        assert job.id == second_job.id
        assert job.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_migrate_no_jobs(self, redis_client: aioredis.Redis) -> None:
        storage = RedisJobsStorage(client=redis_client)

        jobs = await storage.get_all_jobs()
        assert not jobs

        assert await storage.migrate()
        assert not await storage.migrate()

    @pytest.mark.asyncio
    async def test_migrate_version_4(self, redis_client: aioredis.Redis) -> None:
        job = self._create_succeeded_job()

        storage = RedisJobsStorage(client=redis_client)
        async with storage.try_create_job(job, skip_index=True):
            pass

        jobs_for_deletion = await storage.get_jobs_for_deletion()
        assert not jobs_for_deletion

        await storage.migrate()

        jobs_for_deletion = await storage.get_jobs_for_deletion()
        assert jobs_for_deletion

        assert not await storage.migrate()

    @pytest.mark.asyncio
    async def test_migrate(self, redis_client: aioredis.Redis) -> None:
        first_job = self._create_pending_job(owner="testuser")
        second_job = self._create_running_job(owner="testuser")

        first_job.allow_empty_cluster_name = True
        first_job.cluster_name = ""
        second_job.allow_empty_cluster_name = True
        second_job.cluster_name = ""

        storage = RedisJobsStorage(client=redis_client)
        async with storage.try_create_job(first_job, skip_index=True):
            pass
        async with storage.try_create_job(second_job, skip_index=True):
            pass

        jobs = await storage.get_all_jobs()
        job_ids = [job.id for job in jobs]
        assert not job_ids

        filters = JobFilter(owners={"testuser"})

        jobs = await storage.get_all_jobs(filters)
        assert not jobs

        filters2 = JobFilter(clusters={"default": {}})

        jobs = await storage.get_all_jobs(filters2)
        assert not jobs

        assert await storage.migrate()

        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [first_job.id, second_job.id]
        for job in jobs:
            assert job.cluster_name == "default"

        jobs = await storage.get_all_jobs(filters2)
        job_ids = [job.id for job in jobs]
        assert job_ids == [first_job.id, second_job.id]

        assert not await storage.migrate()

    @pytest.mark.asyncio
    async def test_migrate_version_6(self, redis_client: aioredis.Redis) -> None:
        job = self._create_running_job(owner="testuser")
        volume1 = ContainerVolume(
            uri=URL("storage://path/to/dir"),
            src_path=PurePath("/tmp/path/to/dir"),
            dst_path=PurePath("/container/path"),
            read_only=True,
        )
        volume2 = ContainerVolume(
            uri=URL("storage://testuser/path/to/dir2"),
            src_path=PurePath("/tmp/path/to/dir2"),
            dst_path=PurePath("/container/path2"),
            read_only=True,
        )
        volume3 = ContainerVolume(
            uri=URL("storage://test-cluster/testuser/path/to/dir3"),
            src_path=PurePath("/tmp/path/to/dir3"),
            dst_path=PurePath("/container/path3"),
            read_only=True,
        )
        volume4 = ContainerVolume(
            uri=URL(""),
            src_path=PurePath("/tmp/path/to/dir4"),
            dst_path=PurePath("/container/path4"),
            read_only=True,
        )
        job.request.container.volumes.extend([volume1, volume2, volume3, volume4])

        storage = RedisJobsStorage(client=redis_client)
        async with storage.try_create_job(job):
            pass

        original_job = await storage.get_job(job.id)
        assert original_job.request.container.volumes == [
            volume1,
            volume2,
            volume3,
            volume4,
        ]

        assert await storage.migrate()

        migrated_job = await storage.get_job(job.id)
        migrated_volume1 = ContainerVolume(
            uri=URL("storage://test-cluster/path/to/dir"),
            src_path=PurePath("/tmp/path/to/dir"),
            dst_path=PurePath("/container/path"),
            read_only=True,
        )
        migrated_volume2 = ContainerVolume(
            uri=URL("storage://test-cluster/testuser/path/to/dir2"),
            src_path=PurePath("/tmp/path/to/dir2"),
            dst_path=PurePath("/container/path2"),
            read_only=True,
        )
        assert migrated_job.request.container.volumes == [
            migrated_volume1,
            migrated_volume2,
            volume3,
            volume4,
        ]

        assert not await storage.migrate()

    def test_migrate_storage_uri(self) -> None:
        convert = RedisJobsStorage._migrate_storage_uri
        assert convert(URL("storage://alice"), "test-cluster") == URL(
            "storage://test-cluster/alice"
        )
        assert convert(URL("storage://bob/data"), "test-cluster") == URL(
            "storage://test-cluster/bob/data"
        )
        assert convert(URL("storage://"), "test-cluster") == URL(
            "storage://test-cluster/"
        )
        assert convert(URL("storage:"), "test-cluster") == URL(
            "storage://test-cluster/"
        )
        with pytest.raises(AssertionError):
            convert(URL("storage://test-cluster/alice"), "test-cluster")
        with pytest.raises(AssertionError):
            convert(URL("image://alice"), "test-cluster")

    @pytest.mark.asyncio
    async def test_get_aggregated_run_time_for_user(
        self, redis_client: aioredis.Redis
    ) -> None:
        test_started_at = current_datetime_factory()
        owner = f"test-user-{random_str()}"

        job_started_delay = timedelta(hours=1)
        time_pending_delta = timedelta(minutes=3)
        time_running_delta = timedelta(minutes=20)

        job_started_at = current_datetime_factory() - job_started_delay
        job_running_at = job_started_at + time_pending_delta
        job_finished_at = job_running_at + time_running_delta

        expected_alive_job_runtime = job_started_delay - time_pending_delta
        expected_finished_job_runtime = time_running_delta

        def create_job(cluster_name: str, with_gpu: bool, finished: bool) -> JobRecord:
            status_history = [
                JobStatusItem.create(JobStatus.PENDING, transition_time=job_started_at),
                JobStatusItem.create(JobStatus.RUNNING, transition_time=job_running_at),
            ]
            if finished:
                status_history.append(
                    JobStatusItem.create(
                        JobStatus.SUCCEEDED, transition_time=job_finished_at
                    )
                )
            return JobRecord.create(
                owner=owner,
                request=self._create_job_request(with_gpu),
                cluster_name=cluster_name,
                status_history=JobStatusHistory(status_history),
            )

        jobs_with_gpu = [
            create_job(cluster_name="test-cluster1", with_gpu=True, finished=False),
            create_job(cluster_name="test-cluster1", with_gpu=True, finished=False),
            create_job(cluster_name="test-cluster1", with_gpu=True, finished=True),
            create_job(cluster_name="test-cluster1", with_gpu=True, finished=True),
            create_job(cluster_name="test-cluster2", with_gpu=True, finished=False),
            create_job(cluster_name="test-cluster2", with_gpu=True, finished=False),
            create_job(cluster_name="test-cluster2", with_gpu=True, finished=True),
            create_job(cluster_name="test-cluster2", with_gpu=True, finished=True),
        ]
        jobs_no_gpu = [
            create_job(cluster_name="test-cluster1", with_gpu=False, finished=False),
            create_job(cluster_name="test-cluster1", with_gpu=False, finished=False),
            create_job(cluster_name="test-cluster1", with_gpu=False, finished=True),
            create_job(cluster_name="test-cluster1", with_gpu=False, finished=True),
            create_job(cluster_name="test-cluster2", with_gpu=False, finished=False),
            create_job(cluster_name="test-cluster2", with_gpu=False, finished=False),
            create_job(cluster_name="test-cluster2", with_gpu=False, finished=True),
            create_job(cluster_name="test-cluster2", with_gpu=False, finished=True),
        ]
        storage = RedisJobsStorage(redis_client)
        for job in jobs_with_gpu + jobs_no_gpu:
            async with storage.try_create_job(job):
                pass

        job_filter = JobFilter(owners={owner})
        actual_run_time = await storage.get_aggregated_run_time(job_filter)
        actual_run_times = await storage.get_aggregated_run_time_by_clusters(job_filter)

        test_elapsed = current_datetime_factory() - test_started_at

        # 4x terminated GPU jobs, 4x GPU alive jobs
        expected = 4 * expected_alive_job_runtime + 4 * expected_finished_job_runtime
        actual_gpu = actual_run_time.total_gpu_run_time_delta
        actual_non_gpu = actual_run_time.total_non_gpu_run_time_delta

        actual_run_time1 = actual_run_times["test-cluster1"]
        expected1 = 2 * expected_alive_job_runtime + 2 * expected_finished_job_runtime
        actual_gpu1 = actual_run_time1.total_gpu_run_time_delta
        actual_non_gpu1 = actual_run_time1.total_non_gpu_run_time_delta

        actual_run_time2 = actual_run_times["test-cluster2"]
        expected2 = 2 * expected_alive_job_runtime + 2 * expected_finished_job_runtime
        actual_gpu2 = actual_run_time2.total_gpu_run_time_delta
        actual_non_gpu2 = actual_run_time2.total_non_gpu_run_time_delta

        # NOTE (ajuszkowski, 4-Apr-2019) Because we don't serialize all fields of `Job`
        # (specifically, `Job.current_datetime_factory`, see issue #560),
        # all deserialized `Job` instances get the default value of
        # `current_datetime_factory`, so we cannot assert exact value
        # of `Job.get_run_time()` in this test
        # 4x running jobs -> 4 * test_elapsed
        assert expected <= actual_gpu <= expected + 4 * test_elapsed
        assert expected <= actual_non_gpu <= expected + 4 * test_elapsed

        # 2x running jobs -> 2 * test_elapsed
        assert expected1 <= actual_gpu1 <= expected1 + 2 * test_elapsed
        assert expected1 <= actual_non_gpu1 <= expected1 + 2 * test_elapsed

        # 2x running jobs -> 2 * test_elapsed
        assert expected2 <= actual_gpu2 <= expected2 + 2 * test_elapsed
        assert expected2 <= actual_non_gpu2 <= expected2 + 2 * test_elapsed

    @pytest.mark.asyncio
    async def test_get_jobs_by_ids_missing_only(
        self, redis_client: aioredis.Redis
    ) -> None:
        storage = RedisJobsStorage(client=redis_client)

        jobs = await storage.get_jobs_by_ids({"missing"})
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_jobs_by_ids(self, redis_client: aioredis.Redis) -> None:
        first_job = self._create_pending_job(owner="testuser")
        second_job = self._create_running_job(owner="anotheruser")
        third_job = self._create_running_job(owner="testuser")
        storage = RedisJobsStorage(client=redis_client)
        for job in (first_job, second_job, third_job):
            async with storage.try_create_job(job):
                pass

        job_filter = JobFilter(statuses={JobStatus.PENDING}, owners={"testuser"})
        jobs = await storage.get_jobs_by_ids(
            {first_job.id, "missing", second_job.id, third_job.id},
            job_filter=job_filter,
        )
        job_ids = [job.id for job in jobs]
        assert job_ids == [first_job.id]
