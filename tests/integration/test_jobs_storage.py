from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from platform_api.orchestrator.job import Job, JobStatusHistory, JobStatusItem
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
    JobStorageJobFoundError,
    JobStorageTransactionError,
    RedisJobsStorage,
)
from tests.conftest import not_raises


class TestRedisJobsStorage:
    def _create_job_request(self, with_gpu: bool = False):
        if with_gpu:
            resources = ContainerResources(
                cpu=0.1, memory_mb=256, gpu=1, gpu_model_id="nvidia-tesla-k80"
            )
        else:
            resources = ContainerResources(cpu=0.1, memory_mb=256)
        container = Container(image="ubuntu", command="sleep 5", resources=resources)
        return JobRequest.create(container)

    def _create_pending_job(
        self, kube_orchestrator, owner: str = "", job_name: Optional[str] = None
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            owner=owner,
            name=job_name,
        )

    def _create_running_job(
        self, kube_orchestrator, owner: str = "", job_name: Optional[str] = None
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            owner=owner,
            status=JobStatus.RUNNING,
        )

    def _create_succeeded_job(
        self,
        kube_orchestrator,
        is_deleted=False,
        owner: str = "",
        job_name: Optional[str] = None,
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            status=JobStatus.SUCCEEDED,
            owner=owner,
            is_deleted=is_deleted,
        )

    def _create_failed_job(
        self,
        kube_orchestrator,
        is_deleted=False,
        owner: str = "",
        job_name: Optional[str] = None,
    ):
        return Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            status=JobStatus.FAILED,
            owner=owner,
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
    async def test_get_last_created_job_id__no_job_updated(
        self, redis_client, kube_orchestrator
    ):
        job = self._create_pending_job(kube_orchestrator, owner="me", job_name="job-1")
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        job.status = JobStatus.RUNNING

        # do not put this job into redis

        # check that job exists in database:
        with pytest.raises(JobError, match=f"no such job {job.id}"):
            await storage.get_job(job.id)

        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created is None

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__is_job_creation_false(
        self, redis_client, kube_orchestrator
    ):

        job = self._create_pending_job(kube_orchestrator, owner="me", job_name="job-1")
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        job.status = JobStatus.RUNNING
        await storage.update_job_atomic(job, is_job_creation=False)

        # check that job exists in database:
        job_read = await storage.get_job(job.id)
        assert job_read.to_primitive() == job.to_primitive()

        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created is None

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__is_job_creation_true(
        self, redis_client, kube_orchestrator
    ):
        job = self._create_pending_job(kube_orchestrator, owner="me", job_name="job-1")
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        job.status = JobStatus.RUNNING
        await storage.update_job_atomic(job, is_job_creation=True)

        # check that job exists in database:
        job_read = await storage.get_job(job.id)
        assert job_read.to_primitive() == job.to_primitive()

        job_id_last_created = await storage.get_last_created_job_id(job.owner, job.name)
        assert job_id_last_created == job.id

    @pytest.mark.asyncio
    async def test_get_last_created_job_id__multiple_jobs_order_preserves(
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        owner = "me"
        name = "job-name"

        job1 = self._create_pending_job(kube_orchestrator, owner=owner, job_name=name)
        await storage.update_job_atomic(job1, is_job_creation=True)

        job2 = self._create_failed_job(kube_orchestrator, owner=owner, job_name=name)
        await storage.update_job_atomic(job2, is_job_creation=True)

        job3 = self._create_succeeded_job(kube_orchestrator, owner=owner, job_name=name)
        await storage.update_job_atomic(job3, is_job_creation=True)

        job_id_last_created = await storage.get_last_created_job_id(owner, name)
        assert job_id_last_created == job3.id

    @pytest.mark.asyncio
    async def test_try_create_job__no_name__ok(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        pending_job = self._create_pending_job(kube_orchestrator)
        async with storage.try_create_job(pending_job) as job:
            assert pending_job.status == JobStatus.PENDING
            assert job.id == pending_job.id
            job.status = JobStatus.RUNNING
        result_job = await storage.get_job(pending_job.id)
        assert result_job.status == JobStatus.RUNNING

        running_job = self._create_running_job(kube_orchestrator)
        async with storage.try_create_job(running_job) as job:
            assert running_job.status == JobStatus.RUNNING
            assert job.id == running_job.id
            job.status = JobStatus.SUCCEEDED
        result_job = await storage.get_job(running_job.id)
        assert result_job.status == JobStatus.SUCCEEDED

    @pytest.mark.asyncio
    async def test_try_create_job__no_name__job_changed_while_creation(
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        job = self._create_pending_job(kube_orchestrator)

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
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        owner = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(
            kube_orchestrator, job_name=job_name_1, owner=owner
        )
        job_2 = self._create_running_job(
            kube_orchestrator, job_name=job_name_2, owner=owner
        )

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
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        owner_1 = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        owner_2 = "test-user-2"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(
            kube_orchestrator, job_name=job_name_1, owner=owner_1
        )
        job_2 = self._create_running_job(
            kube_orchestrator, job_name=job_name_2, owner=owner_2
        )

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
        self, redis_client, kube_orchestrator, first_job_status
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            status=first_job_status,
            owner=owner,
            is_deleted=False,
        )
        async with storage.try_create_job(first_job):
            pass
        job = await storage.get_job(first_job.id)
        assert job.id == first_job.id
        assert job.status == first_job_status

        second_job = self._create_pending_job(
            kube_orchestrator, job_name=job_name, owner=owner
        )
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
        self, redis_client, kube_orchestrator, first_job_status
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            status=first_job_status,
            owner=owner,
            is_deleted=False,
        )
        second_job = self._create_pending_job(
            kube_orchestrator, owner=owner, job_name=job_name
        )

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
    async def test_get_all_no_filter_multiple_jobs(
        self, redis_client, kube_orchestrator
    ):
        original_jobs = [
            self._create_pending_job(kube_orchestrator, job_name="job-1"),
            self._create_running_job(kube_orchestrator, job_name="job-2"),
            self._create_succeeded_job(kube_orchestrator, job_name="job-3"),
            self._create_failed_job(kube_orchestrator, job_name="job-3"),
        ]
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
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

    async def prepare_filtering_test(self, redis_client, kube_orchestrator):
        ko = kube_orchestrator
        jobs = [
            # no name:
            self._create_pending_job(ko, owner="user1", job_name=None),
            self._create_running_job(ko, owner="user2", job_name=None),
            self._create_succeeded_job(ko, owner="user3", job_name=None),
            self._create_failed_job(ko, owner="user3", job_name=None),
            # user1, job-1:
            self._create_succeeded_job(ko, owner="user1", job_name="job-1"),
            self._create_failed_job(ko, owner="user1", job_name="job-1"),
            self._create_pending_job(ko, owner="user1", job_name="job-1"),
            # user1, job-2:
            self._create_succeeded_job(ko, owner="user1", job_name="job-2"),
            self._create_failed_job(ko, owner="user1", job_name="job-2"),
            self._create_running_job(ko, owner="user1", job_name="job-2"),
            # user2, job-2:
            self._create_succeeded_job(ko, owner="user2", job_name="job-2"),
            self._create_failed_job(ko, owner="user2", job_name="job-2"),
            self._create_pending_job(ko, owner="user2", job_name="job-2"),
            # user2, job-3:
            self._create_succeeded_job(ko, owner="user2", job_name="job-3"),
            self._create_failed_job(ko, owner="user2", job_name="job-3"),
            self._create_running_job(ko, owner="user2", job_name="job-3"),
            # user3, job-3:
            self._create_succeeded_job(ko, owner="user3", job_name="job-3"),
            self._create_failed_job(ko, owner="user3", job_name="job-3"),
            self._create_pending_job(ko, owner="user3", job_name="job-3"),
        ]
        storage = RedisJobsStorage(client=redis_client, orchestrator_config=ko.config)
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return storage, jobs

    @pytest.mark.asyncio
    async def test_get_all_filter_by_single_owner(
        self, redis_client, kube_orchestrator
    ):
        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )

        owners = {"user1"}
        job_filter = JobFilter(owners=owners)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {job.id for job in jobs if job.owner in owners}
        assert expected
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_multiple_owners(
        self, redis_client, kube_orchestrator
    ):
        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )

        owners = {"user1", "user3"}
        job_filter = JobFilter(owners=owners)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {job.id for job in jobs if job.owner in owners}
        assert expected
        assert job_ids == expected

    pend = JobStatus.PENDING
    runn = JobStatus.RUNNING
    succ = JobStatus.SUCCEEDED
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
            (None, ("user1",), ()),
            (None, ("user1",), (pend,)),
            (None, ("user1",), (pend, runn)),
            (None, ("user1",), (succ, fail)),
            (None, ("user1",), (succ, fail, runn, pend)),
            (None, ("user1", "user2"), ()),
            (None, ("user1", "user2"), (pend,)),
            (None, ("user1", "user2"), (pend, runn)),
            (None, ("user1", "user2"), (succ, fail)),
            (None, ("user1", "user2"), (succ, fail, runn, pend)),
            ("job-1", ("user1",), ()),
            ("job-1", ("user1",), (pend,)),
            ("job-1", ("user1",), (pend, runn)),
            ("job-1", ("user1",), (succ, fail)),
            ("job-1", ("user1",), (succ, fail, runn, pend)),
            ("job-1", ("user1", "user2"), ()),
            ("job-1", ("user1", "user2"), (pend,)),
            ("job-1", ("user1", "user2"), (pend, runn)),
            ("job-1", ("user1", "user2"), (succ, fail)),
            ("job-1", ("user1", "user2"), (succ, fail, runn, pend)),
        ],
    )
    async def test_get_all_with_filters(
        self, owners, name, statuses, redis_client, kube_orchestrator
    ):
        def sort_jobs_as_primitives(array: List[Job]) -> List[Dict[str, Any]]:
            return sorted(
                [job.to_primitive() for job in array], key=lambda job: job["id"]
            )

        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )
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
        "name,owners,statuses",
        [
            ("job-1", (), ()),
            ("job-1", (), (pend,)),
            ("job-1", (), (pend, runn)),
            ("job-1", (), (succ, fail)),
            ("job-1", (), (succ, fail, runn)),
            ("job-1", (), (succ, fail, runn, pend)),
        ],
    )
    async def test_get_all_filter_by_name_with_no_owner_fail(
        self, owners, name, statuses, redis_client, kube_orchestrator
    ):
        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )
        job_filter = JobFilter(name=name, owners=set(owners), statuses=set(statuses))
        invalid_operation_error = (
            "filtering jobs by name is allowed only together with owner"
        )
        with pytest.raises(JobsStorageException, match=invalid_operation_error):
            job_filter = JobFilter(name="job-first")
            await storage.get_all_jobs(job_filter)

    @pytest.mark.asyncio
    async def test_get_all_filter_by_owner_and_name(
        self, redis_client, kube_orchestrator
    ):
        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )

        name = "job-first"
        owner = "user1"

        job_filter = JobFilter(name=name, owners={owner})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {job.id for job in jobs if job.name == name and job.owner == owner}
        assert job_ids == expected

    @pytest.mark.asyncio
    async def test_get_all_filter_by_owner_name_and_status(
        self, redis_client, kube_orchestrator
    ):
        storage, jobs = await self.prepare_filtering_test(
            redis_client, kube_orchestrator
        )

        name = "job-first"
        owner = "user1"
        statuses = {JobStatus.RUNNING}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        }
        assert job_ids == expected

        name = "job-first"
        owner = "user1"
        statuses = {JobStatus.SUCCEEDED}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        }
        assert job_ids == expected

        name = "job-first"
        owner = "user1"
        statuses = {JobStatus.SUCCEEDED, JobStatus.RUNNING}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        }
        assert job_ids == expected

        name = "job-second"
        owner = "user3"
        statuses = {JobStatus.FAILED}
        job_filter = JobFilter(name=name, owners={owner}, statuses=statuses)
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        expected = {
            job.id
            for job in jobs
            if job.name == name and job.owner == owner and job.status in statuses
        }
        assert job_ids == expected

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
    async def test_try_update_job__no_name__ok(self, redis_client, kube_orchestrator):
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
    async def test_try_update_job__not_found(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        pending_job = self._create_pending_job(kube_orchestrator)

        with pytest.raises(JobError, match=f"no such job {pending_job.id}"):
            async with storage.try_update_job(pending_job.id):
                pass

    @pytest.mark.asyncio
    async def test_try_update_job__no_name__job_changed_while_creation(
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        job = self._create_pending_job(kube_orchestrator)
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
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        owner = "test-user-1"
        job_name_1 = "some-test-job-name-1"
        job_name_2 = "some-test-job-name-2"

        job_1 = self._create_pending_job(
            kube_orchestrator, job_name=job_name_1, owner=owner
        )
        job_2 = self._create_running_job(
            kube_orchestrator, job_name=job_name_2, owner=owner
        )

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
        self, redis_client, kube_orchestrator
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )

        owner_1 = "test-user-1"
        owner_2 = "test-user-2"
        job_name = "some-test-job-name"

        job_1 = self._create_pending_job(
            kube_orchestrator, job_name=job_name, owner=owner_1
        )
        job_2 = self._create_running_job(
            kube_orchestrator, job_name=job_name, owner=owner_2
        )

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
        self, redis_client, kube_orchestrator, first_job_status
    ):
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = Job(
            kube_orchestrator.config,
            job_request=self._create_job_request(),
            name=job_name,
            status=first_job_status,
            owner=owner,
            is_deleted=False,
        )
        second_job = self._create_pending_job(
            kube_orchestrator, owner=owner, job_name=job_name
        )

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
    async def test_reindex_job_owners_no_jobs(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            client=redis_client, orchestrator_config=kube_orchestrator.config
        )

        jobs = await storage.get_all_jobs()
        assert not jobs

        number_reindexed = await storage.reindex_job_owners()
        assert number_reindexed == 0

    @pytest.mark.asyncio
    async def test_reindex_job_owners(self, redis_client, kube_orchestrator):
        first_job = self._create_pending_job(kube_orchestrator, owner="testuser")
        second_job = self._create_running_job(kube_orchestrator, owner="testuser")
        storage = RedisJobsStorage(
            client=redis_client, orchestrator_config=kube_orchestrator.config
        )
        async with storage.try_create_job(first_job, skip_owner_index=True):
            pass
        async with storage.try_create_job(second_job, skip_owner_index=True):
            pass

        jobs = await storage.get_all_jobs()
        job_ids = {job.id for job in jobs}
        assert job_ids == {first_job.id, second_job.id}

        filters = JobFilter(owners={"testuser"})

        jobs = await storage.get_all_jobs(filters)
        assert not jobs

        number_reindexed = await storage.reindex_job_owners()
        assert number_reindexed == 2

        jobs = await storage.get_all_jobs(filters)
        job_ids = {job.id for job in jobs}
        assert job_ids == {first_job.id, second_job.id}

        number_reindexed = await storage.reindex_job_owners()
        assert number_reindexed == 0

    @pytest.mark.asyncio
    async def test_get_aggregated_run_time_for_user(
        self, redis_client, kube_orchestrator
    ):
        def current_time():
            return datetime.now(tz=timezone.utc)

        job_started_at = current_time()
        job_finished_at = datetime(year=2099, month=1, day=1, tzinfo=timezone.utc)

        owner = f"test-user-{uuid4()}"

        def create_job(status: JobStatus, with_gpu: bool, finished: bool) -> Job:
            status_history = [
                JobStatusItem.create(JobStatus.PENDING, transition_time=job_started_at)
            ]
            if finished:
                status_history.append(
                    JobStatusItem.create(
                        JobStatus.SUCCEEDED, transition_time=job_finished_at
                    )
                )
            return Job(
                kube_orchestrator.config,
                owner=owner,
                job_request=self._create_job_request(with_gpu),
                status=status,
                status_history=JobStatusHistory(status_history),
            )

        jobs_with_gpu = [
            create_job(JobStatus.PENDING, with_gpu=True, finished=False),
            create_job(JobStatus.RUNNING, with_gpu=True, finished=False),
            create_job(JobStatus.SUCCEEDED, with_gpu=True, finished=True),
            create_job(JobStatus.FAILED, with_gpu=True, finished=True),
        ]
        jobs_no_gpu = [
            create_job(JobStatus.PENDING, with_gpu=False, finished=False),
            create_job(JobStatus.RUNNING, with_gpu=False, finished=False),
            create_job(JobStatus.SUCCEEDED, with_gpu=False, finished=True),
            create_job(JobStatus.FAILED, with_gpu=False, finished=True),
        ]
        storage = RedisJobsStorage(
            redis_client, orchestrator_config=kube_orchestrator.config
        )
        for job in jobs_with_gpu + jobs_no_gpu:
            async with storage.try_create_job(job):
                pass

        compute_expected_time_started_at = current_time()
        expected_gpu_time = timedelta()
        for job in jobs_with_gpu:
            expected_gpu_time += job.get_run_time()
        expected_non_gpu_time = timedelta()
        for job in jobs_no_gpu:
            expected_non_gpu_time += job.get_run_time()
        compute_expected_time_finished_at = current_time()
        compute_expected_time_elapsed = (
            compute_expected_time_finished_at - compute_expected_time_started_at
        )

        job_filter = JobFilter(owners={owner})
        actual_run_time = await storage.get_aggregated_run_time(job_filter)

        # NOTE (ajuszkowski, 4-Apr-2019) Because we don't serialize all fields of `Job`
        # (specifically, `Job.current_datetime_factory`, see issue #560),
        # all deserialized `Job` instances get the default value of
        # `current_datetime_factory`, so we cannot assert exact value
        # of `Job.get_run_time()` in this test

        expected_gpu_approx = expected_gpu_time + compute_expected_time_elapsed
        assert actual_run_time.total_gpu_run_time_delta >= expected_gpu_approx

        expected_non_gpu_approx = expected_non_gpu_time + compute_expected_time_elapsed
        assert actual_run_time.total_non_gpu_run_time_delta >= expected_non_gpu_approx

    @pytest.mark.asyncio
    async def test_get_jobs_by_ids_missing_only(self, redis_client, kube_orchestrator):
        storage = RedisJobsStorage(
            client=redis_client, orchestrator_config=kube_orchestrator.config
        )

        jobs = await storage.get_jobs_by_ids({"missing"})
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_jobs_by_ids(self, redis_client, kube_orchestrator):
        first_job = self._create_pending_job(kube_orchestrator, owner="testuser")
        second_job = self._create_running_job(kube_orchestrator, owner="anotheruser")
        third_job = self._create_running_job(kube_orchestrator, owner="testuser")
        storage = RedisJobsStorage(
            client=redis_client, orchestrator_config=kube_orchestrator.config
        )
        for job in (first_job, second_job, third_job):
            async with storage.try_create_job(job):
                pass

        job_filter = JobFilter(statuses={JobStatus.PENDING}, owners={"testuser"})
        jobs = await storage.get_jobs_by_ids(
            {first_job.id, "missing", second_job.id, third_job.id},
            job_filter=job_filter,
        )
        job_ids = {job.id for job in jobs}
        assert job_ids == {first_job.id}
