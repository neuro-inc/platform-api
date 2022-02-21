from datetime import timedelta
from typing import Any

import pytest

from platform_api.orchestrator.job import (
    JobRecord,
    JobRequest,
    JobStatus,
    current_datetime_factory,
)
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.jobs_storage import (
    InMemoryJobsStorage,
    JobFilter,
    JobStorageJobFoundError,
)


class TestInMemoryJobsStorage:
    async def test_get_all_jobs_empty(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        jobs = await jobs_storage.get_all_jobs()
        assert not jobs

    def _create_job_request(self, is_gpu_job: bool = False) -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage",
                resources=ContainerResources(
                    cpu=1, memory_mb=128, gpu=1, gpu_model_id="nvidia-tesla-k80"
                )
                if is_gpu_job
                else ContainerResources(cpu=1, memory_mb=128),
            )
        )

    def _create_job(
        self, cluster_name: str = "test-cluster", **kwargs: Any
    ) -> JobRecord:
        return JobRecord.create(
            request=self._create_job_request(), cluster_name=cluster_name, **kwargs
        )

    def _create_finished_job(
        self,
        run_time: timedelta,
        is_gpu_job: bool = False,
        job_status: JobStatus = JobStatus.SUCCEEDED,
        cluster_name: str = "test-cluster",
        **kwargs: Any,
    ) -> JobRecord:
        job = JobRecord.create(
            request=self._create_job_request(is_gpu_job=is_gpu_job),
            cluster_name=cluster_name,
            **kwargs,
        )
        current_time = current_datetime_factory()
        job.set_status(
            JobStatus.RUNNING, current_datetime_factory=lambda: current_time - run_time
        )
        job.set_status(job_status, current_datetime_factory=lambda: current_time)
        return job

    async def test_set_get_job(self) -> None:
        jobs_storage = InMemoryJobsStorage()

        pending_job = self._create_job()
        await jobs_storage.set_job(pending_job)

        running_job = self._create_job(status=JobStatus.RUNNING, materialized=True)
        await jobs_storage.set_job(running_job)

        succeeded_job = self._create_job(status=JobStatus.SUCCEEDED, materialized=True)
        await jobs_storage.set_job(succeeded_job)

        job = await jobs_storage.get_job(pending_job.id)
        assert job.id == pending_job.id
        assert job.request == pending_job.request

        jobs = await jobs_storage.get_all_jobs()
        assert {job.id for job in jobs} == {
            pending_job.id,
            running_job.id,
            succeeded_job.id,
        }

        job_filter = JobFilter(statuses={JobStatus.PENDING, JobStatus.RUNNING})
        jobs = await jobs_storage.get_all_jobs(job_filter)
        assert {job.id for job in jobs} == {running_job.id, pending_job.id}

        jobs = await jobs_storage.get_running_jobs()
        assert {job.id for job in jobs} == {running_job.id}

        jobs = await jobs_storage.get_unfinished_jobs()
        assert {job.id for job in jobs} == {pending_job.id, running_job.id}

        jobs = await jobs_storage.get_jobs_for_deletion()
        assert {job.id for job in jobs} == {succeeded_job.id}

    async def test_try_create_job(self) -> None:
        jobs_storage = InMemoryJobsStorage()

        job = self._create_job(name="job-name")

        async with jobs_storage.try_create_job(job):
            pass

        retrieved_job = await jobs_storage.get_job(job.id)
        assert retrieved_job.id == job.id

        with pytest.raises(JobStorageJobFoundError):
            async with jobs_storage.try_create_job(job):
                pass


class TestJobFilter:
    def _create_job_request(self) -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    def _create_job(
        self, cluster_name: str = "test-cluster", **kwargs: Any
    ) -> JobRecord:
        return JobRecord.create(
            request=self._create_job_request(), cluster_name=cluster_name, **kwargs
        )

    def test_check_empty_filter(self) -> None:
        job = self._create_job(owner="testuser")
        assert JobFilter().check(job)

    def test_check_statuses(self) -> None:
        job = self._create_job(owner="testuser", status=JobStatus.PENDING)
        assert not JobFilter(statuses={JobStatus.RUNNING}).check(job)

    def test_check_tags_job_zero_filter_zero(self) -> None:
        job = self._create_job(owner="testuser", status=JobStatus.PENDING)
        filt = JobFilter()
        assert filt.check(job)

    def test_check_tags_job_all_fileter_all(self) -> None:
        job = self._create_job(
            owner="testuser", status=JobStatus.PENDING, tags=["t1", "t2", "t3"]
        )
        filt = JobFilter(tags={"t1", "t2", "t3"})
        assert filt.check(job)

    def test_check_tags_job_zero_filter_all(self) -> None:
        job = self._create_job(owner="testuser", status=JobStatus.PENDING)
        filt = JobFilter(tags={"t1", "t2", "t3"})
        assert not filt.check(job)

    def test_check_tags_job_all_filter_zero(self) -> None:
        job = self._create_job(
            owner="testuser", status=JobStatus.PENDING, tags=["t1", "t2", "t3"]
        )
        filt = JobFilter()
        assert filt.check(job)

    def test_check_tags_job_less_filter_more(self) -> None:
        job = self._create_job(owner="testuser", status=JobStatus.PENDING, tags=["t1"])
        filt = JobFilter(tags={"t1", "t2", "t3"})
        assert not filt.check(job)

    def test_check_tags_job_more_filter_less(self) -> None:
        job = self._create_job(
            owner="testuser", status=JobStatus.PENDING, tags=["t1", "t2", "t3"]
        )
        filt = JobFilter(tags={"t1"})
        assert filt.check(job)

    def test_check_tags_intersect(self) -> None:
        job = self._create_job(
            owner="testuser", status=JobStatus.PENDING, tags=["t1", "t2"]
        )
        filt = JobFilter(tags={"t2", "t3"})
        assert not filt.check(job)

    def test_check_tags_disjoint(self) -> None:
        job = self._create_job(
            owner="testuser", status=JobStatus.PENDING, tags=["t1", "t2"]
        )
        filt = JobFilter(tags={"t3", "t4"})
        assert not filt.check(job)

    def test_check_owners(self) -> None:
        job = self._create_job(owner="testuser")
        assert not JobFilter(owners={"anotheruser"}).check(job)

    def test_check_name(self) -> None:
        job = self._create_job(owner="testuser", name="testname")
        assert not JobFilter(name="anothername").check(job)

    def test_check_cluster_names(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(),
            owner="testuser",
            cluster_name="my-cluster",
        )
        assert not JobFilter(clusters={"test-cluster": {}}).check(job)
        assert JobFilter(clusters={"my-cluster": {}}).check(job)

    def test_check_ids(self) -> None:
        job = self._create_job(owner="testuser", name="testname")
        job2 = self._create_job(owner="testuser")
        assert JobFilter(ids={job.id}).check(job)
        assert JobFilter(ids={job2.id}).check(job2)
        assert not JobFilter(ids={job.id}).check(job2)
        assert not JobFilter(ids={job2.id}).check(job)
        assert JobFilter(ids={job.id, job2.id}).check(job)
        assert JobFilter(ids={job.id, job2.id}).check(job2)

    def test_check_ids_status(self) -> None:
        job = self._create_job(
            owner="testuser", name="testname", status=JobStatus.PENDING
        )
        assert JobFilter(ids={job.id}, statuses={JobStatus.PENDING}).check(job)
        assert not JobFilter(ids={job.id}, statuses={JobStatus.RUNNING}).check(job)

    def test_check_all(self) -> None:
        job = self._create_job(
            status=JobStatus.PENDING, owner="testuser", name="testname"
        )
        assert JobFilter(
            statuses={JobStatus.PENDING},
            owners={"testuser"},
            name="testname",
            clusters={"test-cluster": {}},
        ).check(job)

    def test_check_clusters_and_owners(self) -> None:
        filter = JobFilter(
            clusters={
                "cluster1": {None: {"user2": set()}},
                "cluster2": {None: {"user1": set()}},
            },
            owners={"user1", "user2"},
        )
        found = []
        for cluster_name in ("cluster1", "cluster2", "cluster3"):
            for owner in ("user1", "user2", "user3"):
                job = self._create_job(cluster_name=cluster_name, owner=owner)
                if filter.check(job):
                    found.append((cluster_name, owner))
        assert found == [("cluster1", "user2"), ("cluster2", "user1")]

    def test_check_clusters_and_owners2(self) -> None:
        filter = JobFilter(
            clusters={"cluster1": {}, "cluster2": {None: {"user2": set()}}},
            owners={"user1", "user2"},
        )
        found = []
        for cluster_name in ("cluster1", "cluster2", "cluster3"):
            for owner in ("user1", "user2", "user3"):
                job = self._create_job(cluster_name=cluster_name, owner=owner)
                if filter.check(job):
                    found.append((cluster_name, owner))
        assert found == [
            ("cluster1", "user1"),
            ("cluster1", "user2"),
            ("cluster2", "user2"),
        ]

    def test_check_clusters_and_owners3(self) -> None:
        filter = JobFilter(
            clusters={"cluster1": {}, "cluster2": {None: {"user2": set()}}}
        )
        found = []
        for cluster_name in ("cluster1", "cluster2", "cluster3"):
            for owner in ("user1", "user2", "user3"):
                job = self._create_job(cluster_name=cluster_name, owner=owner)
                if filter.check(job):
                    found.append((cluster_name, owner))
        assert found == [
            ("cluster1", "user1"),
            ("cluster1", "user2"),
            ("cluster1", "user3"),
            ("cluster2", "user2"),
        ]

    def test_check_clusters_and_owners4(self) -> None:
        filter = JobFilter(
            clusters={
                "cluster1": {None: {"user1": set()}},
                "cluster2": {None: {"user1": set(), "user2": set()}},
                "cluster3": {None: {"user1": set(), "user3": set()}},
            },
            owners={"user1", "user2", "user3"},
        )
        found = []
        for cluster_name in ("cluster1", "cluster2", "cluster3", "cluster4"):
            for owner in ("user1", "user2", "user3", "user4"):
                job = self._create_job(cluster_name=cluster_name, owner=owner)
                if filter.check(job):
                    found.append((cluster_name, owner))
        assert found == [
            ("cluster1", "user1"),
            ("cluster2", "user1"),
            ("cluster2", "user2"),
            ("cluster3", "user1"),
            ("cluster3", "user3"),
        ]

    def test_check_owners_and_names(self) -> None:
        filter = JobFilter(
            clusters={"test-cluster": {None: {"user1": {"name1"}, "user2": {"name2"}}}},
            owners={"user1", "user2"},
        )
        found = []
        for owner in ("user1", "user2", "user3"):
            for name in ("name1", "name2", "name3", None):
                job = self._create_job(owner=owner, name=name)
                if filter.check(job):
                    found.append((owner, name))
        assert found == [("user1", "name1"), ("user2", "name2")]

    def test_check_owners_and_names2(self) -> None:
        filter = JobFilter(
            clusters={"test-cluster": {None: {"user1": set(), "user2": {"name2"}}}},
            owners={"user1", "user2"},
        )
        found = []
        for owner in ("user1", "user2", "user3"):
            for name in ("name1", "name2", None):
                job = self._create_job(owner=owner, name=name)
                if filter.check(job):
                    found.append((owner, name))
        assert found == [
            ("user1", "name1"),
            ("user1", "name2"),
            ("user1", None),
            ("user2", "name2"),
        ]
