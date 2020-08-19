import asyncio
import copy
import itertools
from asyncio import Future
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, AsyncIterator, Iterable, cast
from unittest.mock import Mock

import pytest

from platform_api.orchestrator.job import (
    AggregatedRunTime,
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
from platform_api.orchestrator.jobs_storage.proxy import ProxyJobStorage


class TestInMemoryJobsStorage:
    @pytest.mark.asyncio
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
        **kwargs: Any
    ) -> JobRecord:
        job = JobRecord.create(
            request=self._create_job_request(is_gpu_job=is_gpu_job),
            cluster_name=cluster_name,
            **kwargs
        )
        current_time = current_datetime_factory()
        job.set_status(
            JobStatus.RUNNING, current_datetime_factory=lambda: current_time - run_time
        )
        job.set_status(job_status, current_datetime_factory=lambda: current_time)
        return job

    @pytest.mark.asyncio
    async def test_set_get_job(self) -> None:
        jobs_storage = InMemoryJobsStorage()

        pending_job = self._create_job()
        await jobs_storage.set_job(pending_job)

        running_job = self._create_job(status=JobStatus.RUNNING)
        await jobs_storage.set_job(running_job)

        succeeded_job = self._create_job(status=JobStatus.SUCCEEDED)
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

    @pytest.mark.asyncio
    async def test_get_tags_empty(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        for job in [
            self._create_job(owner="u", tags=["b"]),
            self._create_job(owner="u", tags=["a"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("another")
        assert tags_u1 == []

    @pytest.mark.asyncio
    async def test_get_tags_single(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        for job in [
            self._create_job(owner="u", tags=["b"]),
            self._create_job(owner="u", tags=["a"]),
            self._create_job(owner="u", tags=["c"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["c", "a", "b"]

    @pytest.mark.asyncio
    async def test_get_tags_multiple(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        for job in [
            self._create_job(owner="u", tags=["b", "a", "c"]),
            self._create_job(owner="u", tags=["d"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["d", "a", "b", "c"]

    @pytest.mark.asyncio
    async def test_get_tags_overwrite_single(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        for job in [
            self._create_job(owner="u", tags=["a"]),
            self._create_job(owner="u", tags=["b"]),
            self._create_job(owner="u", tags=["a"]),
            self._create_job(owner="u", tags=["c"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["c", "a", "b"]

    @pytest.mark.asyncio
    async def test_get_tags_overwrite_multiple(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        for job in [
            self._create_job(owner="u", tags=["a"]),
            self._create_job(owner="u", tags=["b"]),
            self._create_job(owner="u", tags=["c", "a"]),
        ]:
            async with jobs_storage.try_create_job(job):
                pass

        tags_u1 = await jobs_storage.get_tags("u")
        assert tags_u1 == ["a", "c", "b"]

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
    async def test_get_aggregated_run_time(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=False,
                run_time=timedelta(minutes=1),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=False,
                run_time=timedelta(minutes=2),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=True,
                run_time=timedelta(minutes=3),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=True,
                run_time=timedelta(minutes=4),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-2", run_time=timedelta(minutes=5),
            )
        )
        result = await jobs_storage.get_aggregated_run_time(JobFilter())

        assert result == AggregatedRunTime(
            total_gpu_run_time_delta=timedelta(minutes=7),
            total_non_gpu_run_time_delta=timedelta(minutes=8),
        )

    @pytest.mark.asyncio
    async def test_get_aggregated_run_time_by_clusters(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=False,
                run_time=timedelta(minutes=1),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=False,
                run_time=timedelta(minutes=2),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=True,
                run_time=timedelta(minutes=3),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-1",
                is_gpu_job=True,
                run_time=timedelta(minutes=4),
            )
        )
        await jobs_storage.set_job(
            self._create_finished_job(
                cluster_name="test-cluster-2", run_time=timedelta(minutes=5),
            )
        )
        result = await jobs_storage.get_aggregated_run_time_by_clusters(JobFilter())

        assert result == {
            "test-cluster-1": AggregatedRunTime(
                total_gpu_run_time_delta=timedelta(minutes=7),
                total_non_gpu_run_time_delta=timedelta(minutes=3),
            ),
            "test-cluster-2": AggregatedRunTime(
                total_gpu_run_time_delta=timedelta(),
                total_non_gpu_run_time_delta=timedelta(minutes=5),
            ),
        }


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
            clusters={"cluster1": {"user2": set()}, "cluster2": {"user1": set()}},
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
            clusters={"cluster1": {}, "cluster2": {"user2": set()}},
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
        filter = JobFilter(clusters={"cluster1": {}, "cluster2": {"user2": set()}})
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
                "cluster1": {"user1": set()},
                "cluster2": {"user1": set(), "user2": set()},
                "cluster3": {"user1": set(), "user3": set()},
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
            clusters={"test-cluster": {"user1": {"name1"}, "user2": {"name2"}}},
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
            clusters={"test-cluster": {"user1": set(), "user2": {"name2"}}},
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


@dataclass
class ProxyJobStorageSetup:
    proxy: ProxyJobStorage
    primary: Mock
    secondary: Iterable[Mock]

    @property
    def all_mocks(self) -> Iterable[Mock]:
        return itertools.chain((self.primary,), self.secondary)


class TestProxyJobStorage:
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

    @pytest.fixture()
    def proxy_setup(self) -> ProxyJobStorageSetup:
        primary = Mock()
        secondary = (Mock(), Mock())
        proxy = ProxyJobStorage(primary, secondary)
        return ProxyJobStorageSetup(proxy=proxy, primary=primary, secondary=secondary)

    @pytest.mark.asyncio
    async def test_pass_through_get_job(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.get_job.return_value = Future()
        proxy_setup.primary.get_job.return_value.set_result(42)
        assert await proxy_setup.proxy.get_job("foo") == 42
        for mock in proxy_setup.all_mocks:
            mock.get_job.assert_called_with("foo")

    @pytest.mark.asyncio
    async def test_pass_through_set_job(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.set_job.return_value = Future()
        proxy_setup.primary.set_job.return_value.set_result(None)
        fake_job = cast(JobRecord, Mock())
        assert await proxy_setup.proxy.set_job(fake_job) is None
        for mock in proxy_setup.all_mocks:
            mock.set_job.assert_called_with(fake_job)

    @pytest.mark.asyncio
    async def test_pass_through_get_jobs_by_ids(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.get_jobs_by_ids.return_value = Future()
        proxy_setup.primary.get_jobs_by_ids.return_value.set_result(42)
        assert await proxy_setup.proxy.get_jobs_by_ids(("foo", "bar"), None) == 42
        for mock in proxy_setup.all_mocks:
            mock.get_jobs_by_ids.assert_called_with(("foo", "bar"), None)

    @pytest.mark.asyncio
    async def test_pass_through_get_jobs_for_deletion(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.get_jobs_for_deletion.return_value = Future()
        proxy_setup.primary.get_jobs_for_deletion.return_value.set_result(42)
        delay = timedelta(days=2)
        assert await proxy_setup.proxy.get_jobs_for_deletion(delay=delay) == 42
        for mock in proxy_setup.all_mocks:
            mock.get_jobs_for_deletion.assert_called_with(delay=delay)

    @pytest.mark.asyncio
    async def test_pass_through_get_tags(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.get_tags.return_value = Future()
        proxy_setup.primary.get_tags.return_value.set_result(42)
        assert await proxy_setup.proxy.get_tags("foo") == 42
        for mock in proxy_setup.all_mocks:
            mock.get_tags.assert_called_with("foo")

    @pytest.mark.asyncio
    async def test_pass_through_get_aggregated_run_time_by_clusters(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        proxy_setup.primary.get_aggregated_run_time_by_clusters.return_value = Future()
        proxy_setup.primary.get_aggregated_run_time_by_clusters.return_value.set_result(
            42
        )
        fake_filter = cast(JobFilter, Mock())
        assert (
            await proxy_setup.proxy.get_aggregated_run_time_by_clusters(fake_filter)
            == 42
        )
        for mock in proxy_setup.all_mocks:
            mock.get_aggregated_run_time_by_clusters.assert_called_with(fake_filter)

    @pytest.mark.asyncio
    async def test_pass_through_iter_all_jobs(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        call_count = 0

        async def aiter() -> AsyncIterator[int]:
            nonlocal call_count
            for num in range(10):
                yield num
                call_count += 1

        for mock in proxy_setup.all_mocks:
            mock.iter_all_jobs.return_value = aiter()

        results = []
        async for item in proxy_setup.proxy.iter_all_jobs(
            None, reverse=False, limit=None
        ):
            results.append(item)

        await asyncio.sleep(0.1)

        assert results == list(range(10))
        assert call_count == 10 * len(list(proxy_setup.all_mocks))

        for mock in proxy_setup.all_mocks:
            mock.iter_all_jobs.assert_called_with(None, reverse=False, limit=None)

    @pytest.mark.asyncio
    async def test_pass_through_try_create_job(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        records = []

        @asynccontextmanager
        async def amanager(job: JobRecord) -> AsyncIterator[JobRecord]:
            job_copy = copy.deepcopy(job)
            records.append(job_copy)
            yield job_copy

        job = self._create_job()
        for mock in proxy_setup.all_mocks:
            mock.try_create_job.side_effect = amanager

        async with proxy_setup.proxy.try_create_job(job) as new_job:
            new_job.owner = "42"

        assert len(records) == len(list(proxy_setup.all_mocks))
        for recorded_job in records:
            assert recorded_job.owner == "42"

        for mock in proxy_setup.all_mocks:
            mock.try_create_job.assert_called_with(job)

    @pytest.mark.asyncio
    async def test_pass_through_try_update_job(
        self, proxy_setup: ProxyJobStorageSetup
    ) -> None:
        records = []

        @asynccontextmanager
        async def amanager(job_id: str) -> AsyncIterator[JobRecord]:
            job = self._create_job()
            records.append(job)
            yield job

        for mock in proxy_setup.all_mocks:
            mock.try_update_job.side_effect = amanager

        async with proxy_setup.proxy.try_update_job("job_id") as job_to_update:
            job_to_update.owner = "42"

        assert len(records) == len(list(proxy_setup.all_mocks))
        for recorded_job in records:
            assert recorded_job.owner == "42"

        for mock in proxy_setup.all_mocks:
            mock.try_update_job.assert_called_with("job_id")
