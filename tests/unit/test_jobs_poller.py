import asyncio
from typing import Any, AsyncIterator, Callable

import pytest

from platform_api.orchestrator.job_request import JobRequest, JobStatus
from platform_api.orchestrator.jobs_poller import JobsPoller
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.user import User

from .conftest import MockOrchestrator


class TestJobsPoller:
    async def wait_for_job_status(
        self, jobs_service: JobsService, num: int = 10, interval: int = 1
    ) -> None:
        for _ in range(num):
            all_jobs = await jobs_service.get_all_jobs()
            if all(job.status == JobStatus.SUCCEEDED for job in all_jobs):
                break
            else:
                await asyncio.sleep(interval)
        else:
            pytest.fail("Not all jobs have succeeded")

    @pytest.fixture
    async def jobs_poller(self, jobs_service: JobsService) -> AsyncIterator[JobsPoller]:
        poller = JobsPoller(jobs_service=jobs_service, interval_s=1)
        await poller.start()
        yield poller
        await poller.stop()

    @pytest.mark.asyncio
    async def test_polling(
        self,
        jobs_poller: JobsPoller,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = User(name="testuser", token="")
        await jobs_service.create_job(job_request_factory(), user=user)
        await jobs_service.create_job(job_request_factory(), user=user)

        all_jobs = await jobs_service.get_all_jobs()
        assert all(job.status == JobStatus.PENDING for job in all_jobs)

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await self.wait_for_job_status(jobs_service=jobs_service)

    @pytest.mark.asyncio
    async def test_polling_exception(
        self,
        jobs_poller: JobsPoller,
        jobs_service: JobsService,
        mock_orchestrator: MockOrchestrator,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        user = User(name="testuser", token="")
        await jobs_service.create_job(job_request_factory(), user=user)
        await jobs_service.create_job(job_request_factory(), user=user)

        all_jobs = await jobs_service.get_all_jobs()
        assert all(job.status == JobStatus.PENDING for job in all_jobs)

        def update_jobs_statuses() -> Any:
            print("update_jobs_status with error")
            raise ValueError("some unknown error")

        assert jobs_service.update_jobs_statuses
        # jobs_service.update_jobs_statuses = update_jobs_statuses  # type: ignore

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await self.wait_for_job_status(jobs_service=jobs_service)

    @pytest.mark.asyncio
    async def test_cluster_removal(self, jobs_service: JobsService) -> None:
        # create 2 clusters
        # create several jobs in each
        # mock consecutive failures in jobs_service
        # assert failed cluster is removed
        pass
