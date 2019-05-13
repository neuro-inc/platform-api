import pytest

from platform_api.orchestrator.job import JobRecord, JobRequest, JobStatus
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.jobs_storage import (
    InMemoryJobsStorage,
    JobFilter,
    JobStorageJobFoundError,
)


class TestInMemoryJobsStorage:
    @pytest.mark.asyncio
    async def test_get_all_jobs_empty(self) -> None:
        jobs_storage = InMemoryJobsStorage()
        jobs = await jobs_storage.get_all_jobs()
        assert not jobs

    def _create_job_request(self) -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            )
        )

    def _create_job_request_with_description(self) -> JobRequest:
        return JobRequest.create(
            Container(
                image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
            ),
            description="test test description",
        )

    @pytest.mark.asyncio
    async def test_set_get_job(self) -> None:
        jobs_storage = InMemoryJobsStorage()

        pending_job = JobRecord.create(request=self._create_job_request())
        await jobs_storage.set_job(pending_job)

        running_job = JobRecord.create(
            request=self._create_job_request(), status=JobStatus.RUNNING
        )
        await jobs_storage.set_job(running_job)

        succeeded_job = JobRecord.create(
            request=self._create_job_request(), status=JobStatus.SUCCEEDED
        )
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
    async def test_try_create_job(self) -> None:
        jobs_storage = InMemoryJobsStorage()

        job = JobRecord.create(request=self._create_job_request(), name="job-name")

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

    def test_check_empty_filter(self) -> None:
        job = JobRecord.create(request=self._create_job_request(), owner="testuser")
        assert JobFilter().check(job)

    def test_check_statuses(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(),
            owner="testuser",
            status=JobStatus.PENDING,
        )
        assert not JobFilter(statuses={JobStatus.RUNNING}).check(job)

    def test_check_owners(self) -> None:
        job = JobRecord.create(request=self._create_job_request(), owner="testuser")
        assert not JobFilter(owners={"anotheruser"}).check(job)

    def test_check_name(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(), owner="testuser", name="testname"
        )
        assert not JobFilter(name="anothername").check(job)

    def test_check_all(self) -> None:
        job = JobRecord.create(
            request=self._create_job_request(),
            status=JobStatus.PENDING,
            owner="testuser",
            name="testname",
        )
        assert JobFilter(
            statuses={JobStatus.PENDING}, owners={"testuser"}, name="testname"
        ).check(job)
