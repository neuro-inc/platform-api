import logging
from typing import List, Optional, Tuple

from .base import LogReader, Orchestrator
from .job import Job
from .job_request import JobRequest, JobStatus
from .jobs_storage import InMemoryJobsStorage, JobsStorage
from .status import Status

logger = logging.getLogger(__file__)


class JobsService:
    def __init__(
            self, orchestrator: Orchestrator,
            jobs_storage: Optional[JobsStorage] = None) -> None:
        self._jobs_storage = jobs_storage or InMemoryJobsStorage(
            orchestrator=orchestrator)
        self._orchestrator = orchestrator

    async def update_jobs_statuses(self):
        for job in await self._jobs_storage.get_unfinished_jobs():
            await self._update_job_status(job)

        for job in await self._jobs_storage.get_jobs_for_deletion():
            await self._delete_job(job)

    async def _update_job_status(self, job: Job) -> None:
        logger.info('Updating job %s', job.id)
        assert not job.is_finished

        old_status = job.status
        job.status = await self._orchestrator.status_job(job.id)
        if old_status != job.status:
            logger.info(
                'Job %s transitioned from %s to %s', job.id,
                old_status.name, job.status.name)

        await self._jobs_storage.set_job(job)

    async def create_job(self, job_request: JobRequest) -> Tuple[Job, Status]:
        job = Job(
            orchestrator_config=self._orchestrator.config,
            job_request=job_request)
        await self._orchestrator.start_job(job)
        status = Status.create(job.status)
        await self._jobs_storage.set_job(job=job)
        return job, status

    async def get_job_status(self, job_id: str) -> JobStatus:
        job = await self._jobs_storage.get_job(job_id)
        return job.status

    async def get_job(self, job_id: str) -> Job:
        return await self._jobs_storage.get_job(job_id)

    async def get_job_log_reader(self, job_id: str) -> LogReader:
        job = await self.get_job(job_id)
        return await self._orchestrator.get_job_log_reader(job)

    async def _delete_job(self, job: Job) -> None:
        logger.info('Deleting job %s', job.id)
        await self._orchestrator.delete_job(job)
        if not job.is_finished:
            # explicitly setting the job status as succeeded due to manual
            # deletion of a still running job
            job.status = JobStatus.SUCCEEDED
        job.is_deleted = True
        await self._jobs_storage.set_job(job)

    async def delete_job(self, job_id: str) -> None:
        job = await self._jobs_storage.get_job(job_id)
        if not job.is_finished:
            await self._delete_job(job)

    async def get_all_jobs(self) -> List[Job]:
        return await self._jobs_storage.get_all_jobs()
