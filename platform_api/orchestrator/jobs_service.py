import logging
from typing import AsyncIterator, Iterable, List, Optional, Sequence, Tuple

from async_generator import asynccontextmanager
from notifications_client import (
    Client as NotificationsClient,
    JobCannotStartQuotaReached,
    JobTransition,
)

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterNotAvailable,
    ClusterNotFound,
    ClusterRegistry,
)
from platform_api.config import JobsConfig
from platform_api.user import User, UserCluster

from .base import Orchestrator
from .job import Job, JobRecord, JobStatusHistory, JobStatusItem, JobStatusReason
from .job_request import (
    JobError,
    JobException,
    JobNotFoundException,
    JobRequest,
    JobStatus,
)
from .jobs_storage import (
    JobFilter,
    JobsStorage,
    JobsStorageException,
    JobStorageTransactionError,
)
from .status import Status


logger = logging.getLogger(__file__)


class JobsServiceException(Exception):
    pass


class QuotaException(JobsServiceException):
    pass


class GpuQuotaExceededError(QuotaException):
    def __init__(self, user: str) -> None:
        super().__init__(f"GPU quota exceeded for user '{user}'")


class NonGpuQuotaExceededError(QuotaException):
    def __init__(self, user: str) -> None:
        super().__init__(f"non-GPU quota exceeded for user '{user}'")


class JobsService:
    def __init__(
        self,
        cluster_registry: ClusterRegistry,
        jobs_storage: JobsStorage,
        jobs_config: JobsConfig,
        notifications_client: NotificationsClient,
    ) -> None:
        self._cluster_registry = cluster_registry
        self._jobs_storage = jobs_storage
        self._jobs_config = jobs_config
        self._notifications_client = notifications_client

        self._max_deletion_attempts = 10

    def get_cluster_name(self, job: Job) -> str:
        return self._get_cluster_name(job.cluster_name)

    def _get_cluster_name(self, cluster_name: str) -> str:
        return cluster_name or self._jobs_config.default_cluster_name

    @asynccontextmanager
    async def _get_cluster(
        self, name: str, tolerate_unavailable: bool = False
    ) -> AsyncIterator[Cluster]:
        async with self._cluster_registry.get(
            self._get_cluster_name(name), skip_circuit_breaker=tolerate_unavailable
        ) as cluster:
            yield cluster

    async def update_jobs_statuses(self) -> None:
        # TODO (A Danshyn 02/17/19): instead of returning `Job` objects,
        # it makes sense to just return their IDs.

        for record in await self._jobs_storage.get_unfinished_jobs():
            await self._update_job_status_by_id(record.id)

        for record in await self._jobs_storage.get_jobs_for_deletion(
            delay=self._jobs_config.deletion_delay
        ):
            # finished, but not yet deleted jobs
            # assert job.is_finished and not job.is_deleted
            await self._delete_job_by_id(record.id)

    async def _update_job_status_by_id(self, job_id: str) -> None:
        try:
            async with self._update_job_in_storage(job_id) as record:
                try:
                    async with self._get_cluster(record.cluster_name) as cluster:
                        job = Job(
                            storage_config=cluster.config.storage,
                            orchestrator_config=cluster.orchestrator.config,
                            record=record,
                        )
                        await self._update_job_status(cluster.orchestrator, job)
                        job.collect_if_needed()
                except ClusterNotFound as cluster_err:
                    # marking PENDING/RUNNING job as FAILED
                    logger.warning(
                        "Failed to get job '%s' status. Reason: %s",
                        record.id,
                        cluster_err,
                    )
                    record.status_history.current = JobStatusItem.create(
                        JobStatus.FAILED,
                        reason=JobStatusReason.CLUSTER_NOT_FOUND,
                        description=str(cluster_err),
                    )
                    record.is_deleted = True
                except ClusterNotAvailable:
                    # skipping job status update
                    pass
        except JobStorageTransactionError:
            # intentionally ignoring any transaction failures here because
            # the job may have been changed and a retry is needed.
            pass

    async def _update_job_status(self, orchestrator: Orchestrator, job: Job) -> None:
        if job.is_finished:
            logger.warning("Ignoring an attempt to update a finished job %s", job.id)
            return

        logger.info("Updating job %s", job.id)

        old_status_item = job.status_history.current

        if job.is_creating:
            try:
                await orchestrator.start_job(job)
                status_item = job.status_history.current
            except JobError as exc:
                logger.exception("Failed to start job %s. Reason: %s", job.id, exc)
                status_item = JobStatusItem.create(
                    JobStatus.FAILED,
                    reason=str(exc),
                    description="The job could not be started.",
                )
                job.is_deleted = True
        else:
            try:
                status_item = await orchestrator.get_job_status(job)
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
                    reason=JobStatusReason.NOT_FOUND,
                    description="The job could not be scheduled or was preempted.",
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

    async def _delete_job_by_id(self, job_id: str) -> None:
        try:
            async with self._update_job_in_storage(job_id) as record:
                await self._delete_cluster_job(record)

                # marking finished job as deleted
                record.is_deleted = True
        except JobStorageTransactionError:
            # intentionally ignoring any transaction failures here because
            # the job may have been changed and a retry is needed.
            pass

    async def _raise_for_run_time_quota(
        self, user: User, user_cluster: UserCluster, gpu_requested: bool
    ) -> None:
        if not user_cluster.has_quota():
            return
        quota = user_cluster.quota
        run_time_filter = JobFilter(owners={user.name}, clusters={user_cluster.name})
        run_times = await self._jobs_storage.get_aggregated_run_time_by_clusters(
            run_time_filter
        )
        run_time = run_times.get(user_cluster.name, ZERO_RUN_TIME)
        # Even GPU jobs require CPU, so always check CPU quota
        if run_time.total_non_gpu_run_time_delta >= quota.total_non_gpu_run_time_delta:
            raise NonGpuQuotaExceededError(user.name)
        if (
            gpu_requested
            and run_time.total_gpu_run_time_delta >= quota.total_gpu_run_time_delta
        ):
            raise GpuQuotaExceededError(user.name)

    async def create_job(
        self,
        job_request: JobRequest,
        user: User,
        *,
        cluster_name: str,
        job_name: Optional[str] = None,
        is_preemptible: bool = False,
        schedule_timeout: Optional[float] = None,
        max_run_time_minutes: Optional[int] = None,
    ) -> Tuple[Job, Status]:
        user_cluster = user.get_cluster(cluster_name)
        assert user_cluster

        try:
            await self._raise_for_run_time_quota(
                user,
                user_cluster,
                gpu_requested=bool(job_request.container.resources.gpu),
            )
        except QuotaException:
            await self._notifications_client.notify(
                JobCannotStartQuotaReached(user.name)
            )
            raise
        record = JobRecord.create(
            request=job_request,
            owner=user.name,
            cluster_name=cluster_name,
            status_history=JobStatusHistory(
                [
                    JobStatusItem.create(
                        JobStatus.PENDING, reason=JobStatusReason.CREATING
                    )
                ]
            ),
            name=job_name,
            is_preemptible=is_preemptible,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
        )
        job_id = job_request.job_id
        try:
            async with self._create_job_in_storage(record) as record:
                async with self._get_cluster(record.cluster_name) as cluster:
                    job = Job(
                        storage_config=cluster.config.storage,
                        orchestrator_config=cluster.orchestrator.config,
                        record=record,
                    )
                    await cluster.orchestrator.prepare_job(job)
            return job, Status.create(job.status)

        except ClusterNotFound as cluster_err:
            # NOTE: this will result in 400 HTTP response which may not be
            # what we want to convey really
            raise JobsServiceException(
                f"Cluster '{record.cluster_name}' not found"
            ) from cluster_err
        except JobsStorageException as transaction_err:
            logger.error(f"Failed to create job {job_id}: {transaction_err}")
            try:
                await self._delete_cluster_job(record)
            except Exception as cleanup_exc:
                # ignore exceptions
                logger.warning(
                    f"Failed to cleanup job {job_id} during unsuccessful "
                    f"creation: {cleanup_exc}"
                )
            raise JobsServiceException(f"Failed to create job: {transaction_err}")

    async def get_job_status(self, job_id: str) -> JobStatus:
        job = await self._jobs_storage.get_job(job_id)
        return job.status

    async def set_job_status(self, job_id: str, status_item: JobStatusItem) -> None:
        async with self._update_job_in_storage(job_id) as record:
            old_status_item = record.status_history.current
            if old_status_item != status_item:
                record.status_history.current = status_item
                logger.info(
                    "Job %s transitioned from %s to %s",
                    record.request.job_id,
                    old_status_item.status.name,
                    status_item.status.name,
                )

    async def _get_cluster_job(self, record: JobRecord) -> Job:
        try:
            async with self._get_cluster(
                record.cluster_name, tolerate_unavailable=True
            ) as cluster:
                return Job(
                    storage_config=cluster.config.storage,
                    orchestrator_config=cluster.orchestrator.config,
                    record=record,
                )
        except ClusterNotFound:
            # in case the cluster is missing, we still want to return the job
            # to be able to render a proper HTTP response, therefore we have
            # the fallback logic that uses the default cluster instead.
            logger.warning(
                "Falling back to cluster '%s' to retrieve job '%s'",
                self._jobs_config.default_cluster_name,
                record.id,
            )
            # NOTE: we may rather want to fall back to some dummy
            # OrchestratorConfig instead.
            async with self._get_cluster(
                self._jobs_config.default_cluster_name
            ) as cluster:
                return Job(
                    storage_config=cluster.config.storage,
                    orchestrator_config=cluster.orchestrator.config,
                    record=record,
                )

    async def get_job(self, job_id: str) -> Job:
        record = await self._jobs_storage.get_job(job_id)
        return await self._get_cluster_job(record)

    async def _delete_cluster_job(self, record: JobRecord) -> None:
        try:
            async with self._get_cluster(record.cluster_name) as cluster:
                job = Job(
                    storage_config=cluster.config.storage,
                    orchestrator_config=cluster.orchestrator.config,
                    record=record,
                )
                try:
                    await cluster.orchestrator.delete_job(job)
                except JobException as exc:
                    # if the job is missing, we still want to mark
                    # the job as deleted. suppressing.
                    logger.warning(
                        "Could not delete job '%s'. Reason: '%s'", record.id, exc
                    )
        except (ClusterNotFound, ClusterNotAvailable) as exc:
            # if the cluster is unavailable or missing, we still want to mark
            # the job as deleted. suppressing.
            logger.warning("Could not delete job '%s'. Reason: '%s'", record.id, exc)

    async def delete_job(self, job_id: str) -> None:
        for _ in range(self._max_deletion_attempts):
            try:
                async with self._update_job_in_storage(job_id) as record:
                    if record.is_finished:
                        # the job has already finished. nothing to do here.
                        return

                    logger.info("Deleting job %s", job_id)
                    await self._delete_cluster_job(record)

                    record.status = JobStatus.SUCCEEDED
                    record.is_deleted = True
                return
            except JobStorageTransactionError:
                logger.warning("Failed to mark a job %s as deleted. Retrying.", job_id)
        logger.warning("Failed to mark a job %s as deleted. Giving up.", job_id)

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        records = await self._jobs_storage.get_all_jobs(job_filter)
        return [await self._get_cluster_job(record) for record in records]

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[Job]:
        records = await self._jobs_storage.get_jobs_by_ids(
            job_ids, job_filter=job_filter
        )
        return [await self._get_cluster_job(record) for record in records]

    async def get_available_gpu_models(self, name: str) -> Sequence[str]:
        async with self._get_cluster(name) as cluster:
            return await cluster.orchestrator.get_available_gpu_models()

    async def get_cluster_config(self, name: str) -> ClusterConfig:
        async with self._get_cluster(name) as cluster:
            return cluster.config

    async def get_available_cluster_configs(self, user: User) -> List[ClusterConfig]:
        return [await self.get_cluster_config(c.name) for c in user.clusters]

    @asynccontextmanager
    async def _create_job_in_storage(
        self, record: JobRecord
    ) -> AsyncIterator[JobRecord]:
        """
        Wrapper around self._jobs_storage.try_create_job() with notification
        """
        async with self._jobs_storage.try_create_job(record) as record:
            yield record
        await self._notifications_client.notify(
            JobTransition(
                job_id=record.id,
                status=record.status,
                transition_time=record.status_history.current.transition_time,
                reason=record.status_history.current.reason,
            )
        )

    @asynccontextmanager
    async def _update_job_in_storage(self, job_id: str) -> AsyncIterator[JobRecord]:
        """
        Wrapper around self._jobs_storage.try_update_job() with notification
        """
        async with self._jobs_storage.try_update_job(job_id) as record:
            initial_status = record.status_history.current
            yield record
        if initial_status != record.status_history.current:
            await self._notifications_client.notify(
                JobTransition(
                    job_id=record.id,
                    status=record.status_history.current.status,
                    transition_time=record.status_history.current.transition_time,
                    reason=record.status_history.current.reason,
                    description=record.status_history.current.description,
                    exit_code=record.status_history.current.exit_code,
                    prev_status=initial_status.status,
                    prev_transition_time=initial_status.transition_time,
                )
            )

    @property
    def jobs_storage(self) -> JobsStorage:
        return self._jobs_storage
