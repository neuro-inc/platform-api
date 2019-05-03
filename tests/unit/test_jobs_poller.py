import asyncio
from typing import Any, Callable, Tuple

import pytest

from platform_api.config import JobsConfig
from platform_api.orchestrator import (
    JobRequest,
    JobsService,
    JobsStatusPooling,
    JobStatus,
)
from platform_api.user import User

from .conftest import MockJobsStorage, MockOrchestrator


class TestJobsStatusPooling:
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

    async def create_job_pooling(
        self, mock_orchestrator: MockOrchestrator, mock_jobs_storage: MockJobsStorage
    ) -> Tuple[JobsStatusPooling, JobsService]:
        jobs_service = JobsService(
            orchestrator=mock_orchestrator,
            jobs_storage=mock_jobs_storage,
            jobs_config=JobsConfig(),
        )
        jobs_status_pooling = JobsStatusPooling(jobs_service=jobs_service, interval_s=1)
        await jobs_status_pooling.start()
        return jobs_status_pooling, jobs_service

    @pytest.mark.asyncio
    async def test_polling(
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_status_pooling, jobs_service = await self.create_job_pooling(
            mock_orchestrator, mock_jobs_storage
        )

        user = User(name="testuser", token="")
        await jobs_service.create_job(job_request_factory(), user=user)
        await jobs_service.create_job(job_request_factory(), user=user)

        all_jobs = await jobs_service.get_all_jobs()
        assert all(job.status == JobStatus.PENDING for job in all_jobs)

        mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
        await self.wait_for_job_status(jobs_service=jobs_service)
        await jobs_status_pooling.stop()

    @pytest.mark.asyncio
    async def test_polling_exception(
        self,
        mock_orchestrator: MockOrchestrator,
        mock_jobs_storage: MockJobsStorage,
        job_request_factory: Callable[[], JobRequest],
    ) -> None:
        jobs_status_pooling, jobs_service = await self.create_job_pooling(
            mock_orchestrator, mock_jobs_storage
        )

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
        await jobs_status_pooling.stop()
