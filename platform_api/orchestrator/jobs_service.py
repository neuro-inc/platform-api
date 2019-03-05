import logging
from typing import List, Optional, Tuple

from platform_api.user import User

from .base import LogReader, Orchestrator, Telemetry
from .job import Job, JobStatusItem
from .job_request import JobException, JobNotFoundException, JobRequest, JobStatus
from .jobs_storage import (
    InMemoryJobsStorage,
    JobFilter,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)
from .status import Status


logger = logging.getLogger(__file__)


class JobsServiceException(Exception):
    pass


class JobsService:
    def __init__(
        self, orchestrator: Orchestrator, jobs_storage: Optional[JobsStorage] = None
    ) -> None:
        self._jobs_storage = jobs_storage or InMemoryJobsStorage(
            orchestrator_config=orchestrator.config
        )
        self._orchestrator = orchestrator

        self._max_deletion_attempts = 3

    async def update_jobs_statuses(self):
        # TODO (A Danshyn 02/17/19): instead of returning `Job` objects,
        # it makes sense to just return their IDs.

        for job in await self._jobs_storage.get_unfinished_jobs():
            try:
                async with self._jobs_storage.try_update_job(job.id) as job:
                    await self._update_job_status(job)
            except JobStorageTransactionError:
                # intentionally ignoring any transaction failures here because
                # the job may have been changed and a retry is needed.
                pass

        for job in await self._jobs_storage.get_jobs_for_deletion():
            # finished, but not yet deleted jobs
            # assert job.is_finished and not job.is_deleted
            try:
                async with self._jobs_storage.try_update_job(job.id) as job:
                    await self._delete_job(job)
            except JobStorageTransactionError:
                # intentionally ignoring any transaction failures here because
                # the job may have been changed and a retry is needed.
                pass

    async def _update_job_status(self, job: Job) -> None:
        if job.is_finished:
            logger.warning("Ignoring an attempt to update a finished job %s", job.id)
            return

        logger.info("Updating job %s", job.id)

        old_status_item = job.status_history.current

        try:
            status_item = await self._orchestrator.get_job_status(job)
            # TODO: In case job is found, but container is not in state Pending
            # We shall go and check for the events assigned to the pod
            # "pod didn't trigger scale-up (it wouldn't fit if a new node is added)"
            # this is the sign that we KILL the job.
            # Event details
            # Additional details: NotTriggerScaleUp, Nov 2, 2018, 3:00:53 PM,
            # 	Nov 2, 2018, 3:51:06 PM	178
        except JobNotFoundException as exc:
            logger.warning("Failed to get job %s status. Reason: %s", job.id, exc)
            status_item = JobStatusItem.create(
                JobStatus.FAILED,
                reason="Missing",
                description=("The job could not be scheduled or was preempted."),
            )
            job.is_deleted = True

        if old_status_item != status_item:
            job.status_history.current = status_item
            logger.info(
                "Job %s transitioned from %s to %s",
                job.id,
                old_status_item.status.name,
                status_item.status.name,
            )

    async def create_job(
        self,
        job_request: JobRequest,
        user: User,
        job_name: Optional[str] = None,
        is_preemptible: bool = False,
    ) -> Tuple[Job, Status]:
        job = Job(
            orchestrator_config=self._orchestrator.config,
            job_request=job_request,
            owner=user.name,
            name=job_name,
            is_preemptible=is_preemptible,
        )
        job_id = job_request.job_id
        try:
            async with self._jobs_storage.try_create_job(job) as saved_job:
                await self._orchestrator.start_job(saved_job, user.token)
            return saved_job, Status.create(job.status)

        except JobStorageJobFoundError as name_conflict_err:
            logger.error(f"Failed to create job {job_id}: {name_conflict_err}")
            raise JobsServiceException(f"Failed to create job: {name_conflict_err}")

        except JobStorageTransactionError as transaction_err:
            logger.error(f"Failed to create job {job_id}: {transaction_err}")
            try:
                await self._orchestrator.delete_job(job)
            except Exception as cleanup_exc:
                # ignore exception
                logger.info(
                    f"Failed to cleanup job {job_id} during unsuccessful "
                    f"creation: {cleanup_exc}"
                )
            raise JobsServiceException(f"Failed to create job: {transaction_err}")

    async def get_job_status(self, job_id: str) -> JobStatus:
        job = await self._jobs_storage.get_job(job_id)
        return job.status

    async def get_job(self, job_id: str) -> Job:
        return await self._jobs_storage.get_job(job_id)

    async def get_job_log_reader(self, job_id: str) -> LogReader:
        job = await self.get_job(job_id)
        return await self._orchestrator.get_job_log_reader(job)

    async def get_job_telemetry(self, job_id: str) -> Telemetry:
        job = await self.get_job(job_id)
        return await self._orchestrator.get_job_telemetry(job)

    async def _delete_job(self, job: Job) -> None:
        logger.info("Deleting job %s", job.id)
        try:
            await self._orchestrator.delete_job(job)
        except JobException as exc:
            # if the job is missing, we still want to mark it as deleted
            logger.warning("Could not delete job %s. Reason: %s", job.id, exc)
        if not job.is_finished:
            # explicitly setting the job status as succeeded due to manual
            # deletion of a still running job
            job.status = JobStatus.SUCCEEDED
        job.is_deleted = True

    async def delete_job(self, job_id: str) -> None:
        for _ in range(self._max_deletion_attempts):
            try:
                async with self._jobs_storage.try_update_job(job_id) as job:
                    if not job.is_finished:
                        await self._delete_job(job)
                return
            except JobStorageTransactionError:
                logger.warning("Failed to mark a job %s as deleted. Retrying.", job.id)
        logger.warning("Failed to mark a job %s as deleted. Giving up.", job.id)

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        return await self._jobs_storage.get_all_jobs(job_filter)
