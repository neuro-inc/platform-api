from datetime import timedelta
from itertools import islice
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.orchestrator.job import JobRecord, current_datetime_factory
from platform_api.orchestrator.job_request import (
    Container,
    ContainerResources,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import (
    JobFilter,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)
from platform_api.orchestrator.jobs_storage.postgres import PostgresJobsStorage
from tests.conftest import not_raises


class TestJobsStorage:
    @pytest.fixture(params=["postgres"])
    def storage(self, request: Any, sqalchemy_engine: AsyncEngine) -> JobsStorage:
        if request.param == "postgres":
            return PostgresJobsStorage(sqalchemy_engine)
        raise Exception(f"Unknown job storage engine {request.param}.")

    def _create_job_request(self, with_gpu: bool = False) -> JobRequest:
        if with_gpu:
            resources = ContainerResources(
                cpu=0.1,
                memory=256 * 10**6,
                nvidia_gpu=1,
                nvidia_gpu_model="nvidia-tesla-k80",
            )
        else:
            resources = ContainerResources(cpu=0.1, memory=256 * 10**6)
        container = Container(
            image="ubuntu:20.10", command="sleep 5", resources=resources
        )
        return JobRequest.create(container)

    def _create_job(
        self, cluster_name: str = "test-cluster", **kwargs: Any
    ) -> JobRecord:
        return JobRecord.create(
            request=self._create_job_request(), cluster_name=cluster_name, **kwargs
        )

    def _create_pending_job(
        self, owner: str = "compute", job_name: str | None = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(owner=owner, name=job_name, **kwargs)

    def _create_running_job(
        self, owner: str = "compute", job_name: str | None = None, **kwargs: Any
    ) -> JobRecord:
        kwargs.setdefault("materialized", True)
        return self._create_job(
            name=job_name, owner=owner, status=JobStatus.RUNNING, **kwargs
        )

    def _create_succeeded_job(
        self, owner: str = "compute", job_name: str | None = None, **kwargs: Any
    ) -> JobRecord:
        kwargs.setdefault("materialized", True)
        return self._create_job(
            name=job_name, status=JobStatus.SUCCEEDED, owner=owner, **kwargs
        )

    def _create_failed_job(
        self, owner: str = "compute", job_name: str | None = None, **kwargs: Any
    ) -> JobRecord:
        kwargs.setdefault("materialized", True)
        return self._create_job(
            name=job_name, status=JobStatus.FAILED, owner=owner, **kwargs
        )

    def _create_cancelled_job(
        self, owner: str = "compute", job_name: str | None = None, **kwargs: Any
    ) -> JobRecord:
        return self._create_job(
            name=job_name, status=JobStatus.CANCELLED, owner=owner, **kwargs
        )

    async def test_set_get(self, storage: JobsStorage) -> None:
        original_job = self._create_pending_job()
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id
        assert job.status == original_job.status

    async def test_drop_job(self, storage: JobsStorage) -> None:
        original_job = self._create_pending_job()
        await storage.set_job(original_job)

        job = await storage.get_job(original_job.id)
        assert job.id == original_job.id

        await storage.drop_job(original_job.id)
        with pytest.raises(JobError):
            await storage.get_job(original_job.id)

    async def test_drop_unexisting_job(self, storage: JobsStorage) -> None:
        original_job = self._create_pending_job()
        with pytest.raises(JobError):
            await storage.drop_job(original_job.id)

    async def test_try_create_job__no_name__ok(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        async with storage.try_create_job(pending_job) as job:
            assert pending_job.status == JobStatus.PENDING
            assert job.id == pending_job.id
            assert job.project_name == "compute"
            job.status = JobStatus.RUNNING
        result_job = await storage.get_job(pending_job.id)
        assert result_job.project_name == "compute"
        assert result_job.status == JobStatus.RUNNING

        running_job = self._create_running_job()
        async with storage.try_create_job(running_job) as job:
            assert running_job.status == JobStatus.RUNNING
            assert job.id == running_job.id
            job.status = JobStatus.SUCCEEDED
        result_job = await storage.get_job(running_job.id)
        assert result_job.status == JobStatus.SUCCEEDED

    async def test_try_create_job__project_name__ok(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job(project_name="project")
        async with storage.try_create_job(pending_job) as job:
            assert pending_job.status == JobStatus.PENDING
            assert job.id == pending_job.id
            assert job.project_name == "project"
            job.status = JobStatus.RUNNING
        result_job = await storage.get_job(pending_job.id)
        assert result_job.project_name == "project"
        assert result_job.status == JobStatus.RUNNING

    async def test_try_create_job__no_name__job_changed_while_creation(
        self, storage: JobsStorage
    ) -> None:
        job = self._create_pending_job()

        # process-1
        with pytest.raises(
            JobStorageTransactionError, match=f"Job {{id={job.id}}} has changed"
        ):
            async with storage.try_create_job(job) as first_job:
                # value in orchestrator: PENDING, value in db: None
                assert first_job.status == JobStatus.PENDING
                first_job.status = JobStatus.RUNNING
                # value in orchestrator: SUCCEEDED, value in db: None

                # process-2
                with not_raises(JobStorageTransactionError):
                    async with storage.try_create_job(job) as second_job:
                        # value in orchestrator: succeeded, value in db: None
                        assert second_job.status == JobStatus.RUNNING
                        second_job.status = JobStatus.SUCCEEDED
                        # value in orchestrator: FAILED, value in db: None
                        # now status FAILED is written into the db by process-2

                # now status SUCCEEDED fails to be written into the db by process-1

                # now jobs-service catches transaction exception thrown
                # by process-1 and deletes the newly created by process-1 job.

        result_job = await storage.get_job(job.id)
        assert result_job.status == JobStatus.SUCCEEDED

    async def test_try_create_job__different_name_same_owner__ok(
        self, storage: JobsStorage
    ) -> None:
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

    async def test_try_create_job__same_name_different_owner__ok(
        self, storage: JobsStorage
    ) -> None:
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

    @pytest.mark.parametrize("first_job_status", [JobStatus.PENDING, JobStatus.RUNNING])
    async def test_try_create_job__same_name_with_an_active_job__conflict(
        self, storage: JobsStorage, first_job_status: JobStatus
    ) -> None:
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
            match=f"job with name '{job_name}' and project '{first_job.project_name}'"
            f" already exists: '{first_job.id}'",
        ):
            async with storage.try_create_job(second_job):
                pass

    @pytest.mark.parametrize("first_job_status", [JobStatus.PENDING, JobStatus.RUNNING])
    async def test_try_create_job__same_name_and_base_owner_with_active_job__conflict(
        self, storage: JobsStorage, first_job_status: JobStatus
    ) -> None:
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

        second_job = self._create_pending_job(
            job_name=job_name, owner=f"{owner}/anything"
        )
        with pytest.raises(
            JobStorageJobFoundError,
            match=f"job with name '{job_name}' and project '{first_job.project_name}'"
            f" already exists: '{first_job.id}'",
        ):
            async with storage.try_create_job(second_job):
                pass

    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    async def test_try_create_job__same_name_with_a_terminated_job__ok(
        self, storage: JobsStorage, first_job_status: JobStatus
    ) -> None:
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

    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    async def test_try_create_job__same_name_and_base_owner_with_a_terminated_job__ok(
        self, storage: JobsStorage, first_job_status: JobStatus
    ) -> None:
        owner = "test-user"
        job_name = "some-test-job-name"

        first_job = self._create_job(
            name=job_name, status=first_job_status, owner=owner
        )
        second_job = self._create_pending_job(
            owner=f"{owner}/anything", job_name=job_name
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

    async def test_try_create_job_with_tags(self, storage: JobsStorage) -> None:
        tags = ["tag1", "tag2"]
        job = self._create_job(tags=tags)
        async with storage.try_create_job(job) as job:
            assert job.id == job.id
            assert job.tags == tags

        result_job = await storage.get_job(job.id)
        assert result_job.tags == tags

    async def test_get_non_existent(self, storage: JobsStorage) -> None:
        with pytest.raises(JobError, match="no such job unknown"):
            await storage.get_job("unknown")

    async def test_get_all_no_filter_empty_result(self, storage: JobsStorage) -> None:
        jobs = await storage.get_all_jobs()
        assert not jobs

    async def test_get_all_no_filter_single_job(self, storage: JobsStorage) -> None:
        original_job = self._create_pending_job()
        await storage.set_job(original_job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == original_job.id
        assert job.status == original_job.status

    async def test_get_all_no_filter_multiple_jobs(self, storage: JobsStorage) -> None:
        original_jobs = [
            self._create_pending_job(job_name="jobname1"),
            self._create_running_job(job_name="jobname2"),
            self._create_succeeded_job(job_name="jobname3"),
            self._create_failed_job(job_name="jobname3"),
        ]
        for job in original_jobs:
            async with storage.try_create_job(job):
                pass

        jobs = await storage.get_all_jobs()

        job_reprs = sorted(
            (job.to_primitive() for job in jobs), key=lambda job: job["id"]
        )
        original_job_reprs = sorted(
            (job.to_primitive() for job in original_jobs), key=lambda job: job["id"]
        )
        assert job_reprs == original_job_reprs

    async def test_get_all_filter_by_status(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
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

    async def test_get_all_filter_by_tags(self, storage: JobsStorage) -> None:
        job1 = self._create_job(tags=["t1"])
        job2 = self._create_job(tags=["t1", "t2"])
        job3 = self._create_job(tags=["t2"])
        job4 = self._create_job(tags=["t3"])
        job5 = self._create_job(tags=["weird---tag////%%%$$$"])

        await storage.set_job(job1)
        await storage.set_job(job2)
        await storage.set_job(job3)
        await storage.set_job(job4)
        await storage.set_job(job5)

        jobs = await storage.get_all_jobs()
        job_ids = [job.id for job in jobs]
        assert job_ids == [job1.id, job2.id, job3.id, job4.id, job5.id]

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

        filters = JobFilter(tags={"weird---tag////%%%$$$"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job5.id]

        filters = JobFilter(tags={"t1", "t2", "t3"})
        jobs = await storage.get_all_jobs(filters)
        job_ids = [job.id for job in jobs]
        assert not job_ids

    @pytest.mark.parametrize("statuses", [(), (JobStatus.PENDING, JobStatus.RUNNING)])
    async def test_get_all_filter_by_date_range(
        self, statuses: tuple[JobStatus, ...], storage: JobsStorage
    ) -> None:
        t1 = current_datetime_factory()
        job1 = self._create_job()
        t2 = current_datetime_factory()
        job2 = self._create_job()
        t3 = current_datetime_factory()
        job3 = self._create_job()
        t4 = current_datetime_factory()

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

    async def prepare_filtering_test(self, storage: JobsStorage) -> list[JobRecord]:
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
            # user4/service-accounts/test, jobname3:
            self._create_succeeded_job(
                owner="user4/service-accounts/test", job_name="jobname4"
            ),
            self._create_failed_job(
                owner="user4/service-accounts/test", job_name="jobname4"
            ),
            self._create_cancelled_job(
                owner="user4/service-accounts/test", job_name="jobname4"
            ),
            self._create_pending_job(
                owner="user4/service-accounts/test", job_name="jobname4"
            ),
            # project1, user1, jobname3:
            self._create_succeeded_job(
                project_name="project1", owner="user1", job_name="jobname3"
            ),
            self._create_failed_job(
                project_name="project1", owner="user1", job_name="jobname3"
            ),
            self._create_cancelled_job(
                project_name="project1", owner="user1", job_name="jobname3"
            ),
            self._create_pending_job(
                project_name="project1", owner="user1", job_name="jobname3"
            ),
            # project2, user1, jobname3:
            self._create_succeeded_job(
                project_name="project2", owner="user1", job_name="jobname4"
            ),
            self._create_failed_job(
                project_name="project2", owner="user1", job_name="jobname4"
            ),
            self._create_cancelled_job(
                project_name="project2", owner="user1", job_name="jobname4"
            ),
            self._create_pending_job(
                project_name="project2", owner="user1", job_name="jobname4"
            ),
        ]
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return jobs

    async def test_get_all_filter_by_single_owner(self, storage: JobsStorage) -> None:
        jobs = await self.prepare_filtering_test(storage)

        owners = {"user1"}
        job_filter = JobFilter(owners=owners)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.owner in owners]
        assert expected
        assert job_ids == expected

    async def test_get_all_filter_by_single_base_owner(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

        base_owners = {"user4"}
        job_filter = JobFilter(base_owners=base_owners)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.base_owner in base_owners]
        assert expected
        assert job_ids == expected

    async def test_get_all_filter_by_multiple_owners(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

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
        owners: tuple[str, ...],
        name: str | None,
        statuses: tuple[JobStatus, ...],
        storage: JobsStorage,
    ) -> None:
        def sort_jobs_as_primitives(array: list[JobRecord]) -> list[dict[str, Any]]:
            return sorted(
                (job.to_primitive() for job in array), key=lambda job: job["id"]
            )

        jobs = await self.prepare_filtering_test(storage)
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

    @pytest.mark.parametrize(
        "projects,statuses",
        [
            ((), ()),
            ((), (pend,)),
            ((), (pend, runn)),
            ((), (succ, fail)),
            ((), (succ, fail, runn)),
            ((), (succ, fail, runn, pend)),
            ((), (succ, canc, fail, runn, pend)),
            (("user1",), ()),
            (("user1",), (pend,)),
            (("user1",), (pend, runn)),
            (("user1",), (succ, fail)),
            (("user1",), (succ, fail, runn, pend)),
            (("user1",), (succ, canc, fail, runn, pend)),
            (("user1", "user2"), ()),
            (("user1", "user2"), (pend,)),
            (("user1", "user2"), (pend, runn)),
            (("user1", "user2"), (succ, fail)),
            (("user1", "user2"), (succ, fail, runn, pend)),
            (("user1", "user2"), (succ, canc, fail, runn, pend)),
            (("project1",), ()),
            (("project1",), (pend,)),
            (("project1",), (pend, runn)),
            (("project1",), (succ, fail)),
            (("project1",), (succ, fail, runn, pend)),
            (("project1",), (succ, canc, fail, runn, pend)),
            (("project1", "project2"), ()),
            (("project1", "project2"), (pend,)),
            (("project1", "project2"), (pend, runn)),
            (("project1", "project2"), (succ, fail)),
            (("project1", "project2"), (succ, fail, runn, pend)),
            (("project1", "project2"), (succ, canc, fail, runn, pend)),
        ],
    )
    async def test_get_all__filter_by_projects_and_statuses(
        self,
        projects: tuple[str, ...],
        statuses: tuple[JobStatus, ...],
        storage: JobsStorage,
    ) -> None:
        def sort_jobs_as_primitives(array: list[JobRecord]) -> list[dict[str, Any]]:
            return sorted(
                (job.to_primitive() for job in array), key=lambda job: job["id"]
            )

        jobs = await self.prepare_filtering_test(storage)
        job_filter = JobFilter(projects=set(projects), statuses=set(statuses))
        actual = sort_jobs_as_primitives(await storage.get_all_jobs(job_filter))
        expected = sort_jobs_as_primitives(
            [
                job
                for job in jobs
                if (not projects or job.project_name in projects)
                and (not statuses or job.status in statuses)
            ]
        )
        assert actual == expected

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
        name: str | None,
        statuses: tuple[JobStatus, ...],
        storage: JobsStorage,
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)
        job_filter = JobFilter(name=name, owners=set(), statuses=set(statuses))
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [
            job.id
            for job in jobs
            if job.name == name and (not statuses or job.status in statuses)
        ]
        assert job_ids == expected

    async def test_get_all_filter_by_owner_and_name(
        self,
        storage: JobsStorage,
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

        name = "jobname2"
        owner = "user1"

        job_filter = JobFilter(name=name, owners={owner})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        expected = [job.id for job in jobs if job.name == name and job.owner == owner]
        assert job_ids == expected

    async def test_get_all_filter_by_owner_name_and_status(
        self,
        storage: JobsStorage,
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

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

    async def test_get_all_shared_by_name(self, storage: JobsStorage) -> None:
        jobs = await self.prepare_filtering_test(storage)

        job_filter = JobFilter(
            clusters={
                "test-cluster": {None: {"user1": {"jobname2"}, "user2": {"jobname3"}}}
            },
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

    async def test_get_all_filter_by_hostname(
        self,
        storage: JobsStorage,
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

        job1, job2 = islice(jobs, 2)

        job_filter = JobFilter(ids={job1.id})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job1.id}

        job_filter = JobFilter(ids={job1.id, job2.id})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job1.id, job2.id}

    async def test_get_all_filter_by_hostname_and_status(
        self,
        storage: JobsStorage,
    ) -> None:
        jobs = await self.prepare_filtering_test(storage)

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
        self, storage: JobsStorage
    ) -> list[JobRecord]:
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
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return jobs

    async def prepare_filtering_test_different_clusters_orgs(
        self, storage: JobsStorage
    ) -> list[JobRecord]:
        jobs = [
            self._create_running_job(
                owner="user1", cluster_name="test-cluster", org_name=None
            ),
            self._create_succeeded_job(
                owner="user1",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name=None,
            ),
            self._create_failed_job(
                owner="user2",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name="test-org",
            ),
            self._create_succeeded_job(
                owner="user3",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name="test-org",
            ),
            self._create_succeeded_job(
                owner="user1", cluster_name="my-cluster", org_name="test-org"
            ),
            self._create_failed_job(
                owner="user3",
                cluster_name="my-cluster",
                org_name="test-org",
            ),
            self._create_failed_job(
                owner="user1", cluster_name="other-cluster", org_name=None
            ),
            self._create_succeeded_job(
                owner="user2",
                cluster_name="other-cluster",
                org_name="test-org",
            ),
            self._create_running_job(
                owner="user3",
                cluster_name="other-cluster",
                org_name="test-org",
            ),
        ]
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return jobs

    async def test_get_all_filter_by_cluster(self, storage: JobsStorage) -> None:
        jobs = await self.prepare_filtering_test_different_clusters(storage)

        job_filter = JobFilter(clusters={"test-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job.id for job in jobs[:4]]

        job_filter = JobFilter(clusters={"test-cluster": {}, "my-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [job.id for job in jobs[:6]]

        job_filter = JobFilter(clusters={"nonexisting-cluster": {}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == []

    async def prepare_filtering_test_different_orgs(
        self, storage: JobsStorage
    ) -> list[JobRecord]:
        jobs = [
            self._create_running_job(
                owner="user1", cluster_name="test-cluster", org_name=None
            ),
            self._create_succeeded_job(
                owner="user1",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name=None,
            ),
            self._create_failed_job(
                owner="user2",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name=None,
            ),
            self._create_succeeded_job(
                owner="user3",
                cluster_name="test-cluster",
                job_name="jobname1",
                org_name="test-org",
            ),
            self._create_succeeded_job(
                owner="user1", cluster_name="my-cluster", org_name="test-org"
            ),
            self._create_failed_job(
                owner="user3", cluster_name="my-cluster", org_name="test-org"
            ),
            self._create_failed_job(
                owner="user1", cluster_name="other-cluster", org_name="test-org"
            ),
            self._create_succeeded_job(
                owner="user2", cluster_name="other-cluster", org_name="other-org"
            ),
            self._create_running_job(
                owner="user3", cluster_name="other-cluster", org_name="other-org"
            ),
        ]
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        return jobs

    async def test_get_all_filter_by_org(self, storage: JobsStorage) -> None:
        jobs = await self.prepare_filtering_test_different_orgs(storage)

        job_filter = JobFilter(orgs={"other-org"})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job.id for job in jobs[7:]}

        job_filter = JobFilter(orgs={None})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job.id for job in jobs[:3]}

        job_filter = JobFilter(orgs={None, "test-org"})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == {job.id for job in jobs[:7]}

        job_filter = JobFilter(orgs={"nonexisting-org"})
        job_ids = {job.id for job in await storage.get_all_jobs(job_filter)}
        assert job_ids == set()

    async def test_get_all_filter_by_being_dropped(self, storage: JobsStorage) -> None:
        jobs = [
            self._create_job(being_dropped=True),
            self._create_job(being_dropped=False),
            self._create_job(being_dropped=True),
        ]
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        job_filter = JobFilter(being_dropped=True)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[2].id]

        job_filter = JobFilter(being_dropped=False)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id]

    async def test_get_all_filter_by_logs_removed(self, storage: JobsStorage) -> None:
        jobs = [
            self._create_job(logs_removed=True),
            self._create_job(logs_removed=False),
            self._create_job(logs_removed=True),
        ]
        for job in jobs:
            async with storage.try_create_job(job):
                pass
        job_filter = JobFilter(logs_removed=True)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[2].id]

        job_filter = JobFilter(logs_removed=False)
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[1].id]

    async def test_get_all_filter_by_cluster_and_owner(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test_different_clusters(storage)

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
                "test-cluster": {None: {"user1": set()}},
                "other-cluster": {None: {"user2": set()}},
            },
            owners={"user1", "user2"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {None: {"user1": set()}}, "other-cluster": {}},
            owners={"user1", "user2"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[6].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={
                "test-cluster": {None: {"user1": set(), "user2": set()}},
                "my-cluster": {None: {"user2": set(), "user3": set()}},
                "other-cluster": {None: {"user2": set()}},
            },
            owners={"user1", "user2", "user3"},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[2].id, jobs[5].id, jobs[7].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {None: {"user1": set()}}, "other-cluster": {}},
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[0].id, jobs[1].id, jobs[6].id, jobs[7].id, jobs[8].id]

    async def test_get_all_filter_by_cluster_and_org(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test_different_clusters_orgs(storage)

        job_filter = JobFilter(clusters={"test-cluster": {"test-org": {}}})
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[2].id, jobs[3].id]

        job_filter = JobFilter(
            clusters={"test-cluster": {"test-org": {"user2": set()}}}
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[2].id]

        job_filter = JobFilter(
            clusters={"my-cluster": {}, "other-cluster": {"test-org": {}}}
        )
        job_ids = [job.id for job in await storage.get_all_jobs(job_filter)]
        assert job_ids == [jobs[4].id, jobs[5].id, jobs[7].id, jobs[8].id]

    async def test_get_all_filter_by_cluster_and_name(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test_different_clusters(storage)

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

    async def test_get_all_filter_by_cluster_and_status(
        self, storage: JobsStorage
    ) -> None:
        jobs = await self.prepare_filtering_test_different_clusters(storage)

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

    async def test_get_running_empty(self, storage: JobsStorage) -> None:
        jobs = await storage.get_running_jobs()
        assert not jobs

    async def test_get_running(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_running_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == running_job.id
        assert job.status == JobStatus.RUNNING

    async def test_get_unfinished_empty(self, storage: JobsStorage) -> None:
        jobs = await storage.get_unfinished_jobs()
        assert not jobs

    async def test_get_unfinished(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)

        jobs = await storage.get_unfinished_jobs()
        assert len(jobs) == 2
        assert [job.id for job in jobs] == [pending_job.id, running_job.id]
        assert all(not job.is_finished for job in jobs)

    async def test_get_for_deletion_empty(self, storage: JobsStorage) -> None:
        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

    async def test_get_for_deletion(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()
        deleted_job = self._create_succeeded_job(materialized=False)
        await storage.set_job(pending_job)
        await storage.set_job(running_job)
        await storage.set_job(succeeded_job)
        await storage.set_job(deleted_job)

        jobs = await storage.get_jobs_for_deletion()
        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == succeeded_job.id
        assert job.status == JobStatus.SUCCEEDED
        assert job.materialized

    async def test_get_for_drop_empty(self, storage: JobsStorage) -> None:
        jobs = await storage.get_jobs_for_drop()
        assert not jobs

    async def test_get_for_drop(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        running_job = self._create_running_job()
        succeeded_job = self._create_succeeded_job()

        now = current_datetime_factory()
        now_f = lambda: now  # noqa
        f1 = lambda: now - timedelta(days=1)  # noqa
        f2 = lambda: now - timedelta(days=2)  # noqa

        deleted_job_1 = self._create_succeeded_job(
            materialized=False,
            current_datetime_factory=f1,
        )
        deleted_job_2 = self._create_succeeded_job(
            materialized=False,
            current_datetime_factory=f2,
        )
        for job in [
            pending_job,
            running_job,
            succeeded_job,
            deleted_job_1,
            deleted_job_2,
        ]:
            await storage.set_job(job)

        jobs = await storage.get_jobs_for_drop(delay=timedelta(days=2))
        assert len(jobs) == 1
        assert {deleted_job_2.id} == {job.id for job in jobs}

        jobs = await storage.get_jobs_for_drop(delay=timedelta(hours=1))
        assert len(jobs) == 2
        assert {deleted_job_1.id, deleted_job_2.id} == {job.id for job in jobs}

        jobs = await storage.get_jobs_for_drop(delay=timedelta(days=1), limit=1)
        assert len(jobs) == 1
        assert {deleted_job_1.id, deleted_job_2.id}.issuperset({job.id for job in jobs})

    async def test_job_lifecycle(self, storage: JobsStorage) -> None:
        job = self._create_pending_job()
        job_id = job.id
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

        job.materialized = True
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
        assert job.materialized

        job.materialized = False
        await storage.set_job(job)

        jobs = await storage.get_all_jobs()
        assert len(jobs) == 1

        jobs = await storage.get_running_jobs()
        assert not jobs

        jobs = await storage.get_jobs_for_deletion()
        assert not jobs

    async def test_try_update_job__no_name__ok(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()
        await storage.set_job(pending_job)

        async with storage.try_update_job(pending_job.id) as job:
            assert pending_job.status == JobStatus.PENDING
            job.status = JobStatus.RUNNING

        running_job = await storage.get_job(pending_job.id)
        assert running_job.status == JobStatus.RUNNING

    async def test_try_update_job__not_found(self, storage: JobsStorage) -> None:
        pending_job = self._create_pending_job()

        with pytest.raises(JobError, match=f"no such job {pending_job.id}"):
            async with storage.try_update_job(pending_job.id):
                pass

    async def test_try_update_job__no_name__job_changed_while_creation(
        self, storage: JobsStorage
    ) -> None:
        job = self._create_pending_job()
        await storage.set_job(job)

        # process-1
        with pytest.raises(
            JobStorageTransactionError, match=f"Job {{id={job.id}}} has changed"
        ):
            async with storage.try_update_job(job.id) as first_job:
                # value in orchestrator: PENDING, value in db: None
                assert first_job.status == JobStatus.PENDING
                first_job.status = JobStatus.SUCCEEDED
                # value in orchestrator: SUCCEEDED, value in db: None

                # process-2
                with not_raises(JobStorageTransactionError):
                    async with storage.try_update_job(job.id) as second_job:
                        # value in orchestrator: succeeded, value in db: None
                        assert second_job.status == JobStatus.PENDING
                        second_job.status = JobStatus.RUNNING
                        # value in orchestrator: FAILED, value in db: None
                        # now status FAILED is written into the db by process-2

                # now status SUCCEEDED fails to be written into the db by process-1

                # now jobs-service catches transaction exception thrown
                # by process-1 and deletes the newly created by process-1 job.

        result_job = await storage.get_job(job.id)
        assert result_job.status == JobStatus.RUNNING

    async def test_try_update_job__different_name_same_owner__ok(
        self, storage: JobsStorage
    ) -> None:
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

    async def test_try_update_job__same_name_different_owner__ok(
        self, storage: JobsStorage
    ) -> None:
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

    @pytest.mark.parametrize(
        "first_job_status", [JobStatus.SUCCEEDED, JobStatus.FAILED]
    )
    async def test_try_update_job__same_name_with_a_terminated_job__ok(
        self, storage: JobsStorage, first_job_status: JobStatus
    ) -> None:
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

    async def test_get_jobs_by_ids_missing_only(self, storage: JobsStorage) -> None:
        jobs = await storage.get_jobs_by_ids({"missing"})
        assert not jobs

    async def test_get_jobs_by_ids(self, storage: JobsStorage) -> None:
        first_job = self._create_pending_job(owner="testuser")
        second_job = self._create_running_job(owner="anotheruser")
        third_job = self._create_running_job(owner="testuser")
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

        jobs = await storage.get_jobs_by_ids(
            set(),
            job_filter=job_filter,
        )
        assert jobs == []

    async def test_get_jobs_by_org_project_hash(self, storage: JobsStorage) -> None:
        job1 = self._create_job(project_name="project1")
        job2 = self._create_job(project_name="project2")
        for job in (job1, job2):
            async with storage.try_create_job(job):
                pass

        job_filter = JobFilter(org_project_hash=job1.org_project_hash)
        jobs = await storage.get_jobs_by_ids({job1.id}, job_filter=job_filter)
        job_ids = [job.id for job in jobs]
        assert job_ids == [job1.id]

        job_filter = JobFilter(org_project_hash=job1.org_project_hash)
        jobs = await storage.get_jobs_by_ids(set(), job_filter=job_filter)
        assert jobs == []
