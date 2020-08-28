import logging
from pathlib import PurePath
from typing import AsyncIterator, Iterable, List, Optional, Sequence, Tuple

from async_generator import asynccontextmanager
from notifications_client import (
    Client as NotificationsClient,
    JobCannotStartQuotaReached,
    JobTransition,
    QuotaResourceType,
)
from yarl import URL

from platform_api.cluster import (
    Cluster,
    ClusterConfig,
    ClusterNotAvailable,
    ClusterNotFound,
    ClusterRegistry,
)
from platform_api.cluster_config import OrchestratorConfig, StorageConfig
from platform_api.config import JobsConfig
from platform_api.user import User, UserCluster

from .base import Orchestrator
from .job import (
    ZERO_RUN_TIME,
    Job,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
    JobStatusReason,
    maybe_job_id,
)
from .job_request import (
    JobAlreadyExistsException,
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

        self._dummy_cluster_storage_config = StorageConfig(
            host_mount_path=PurePath("/<dummy>")
        )
        self._dummy_cluster_orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.missing-cluster",
            ssh_auth_server="missing-cluster:22",
            resource_pool_types=(),
        )

    @asynccontextmanager
    async def _get_cluster(
        self, name: str, tolerate_unavailable: bool = False
    ) -> AsyncIterator[Cluster]:
        async with self._cluster_registry.get(
            name, skip_circuit_breaker=tolerate_unavailable
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
            except JobAlreadyExistsException:
                logger.info(f"Job '{job.id}' already exists.")
                # Terminate the transaction. The exception will be ignored.
                raise JobStorageTransactionError
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
        run_time_filter = JobFilter(
            owners={user.name}, clusters={user_cluster.name: {}}
        )
        run_times = await self._jobs_storage.get_aggregated_run_time_by_clusters(
            run_time_filter
        )
        run_time = run_times.get(user_cluster.name, ZERO_RUN_TIME)
        if (
            not gpu_requested
            and run_time.total_non_gpu_run_time_delta
            >= quota.total_non_gpu_run_time_delta
        ):
            raise NonGpuQuotaExceededError(user.name)
        if (
            gpu_requested
            and run_time.total_gpu_run_time_delta >= quota.total_gpu_run_time_delta
        ):
            raise GpuQuotaExceededError(user.name)

    def _get_secret_name(self, secret_uri: URL) -> str:
        parts = PurePath(secret_uri.path).parts
        assert len(parts) == 3, parts
        return parts[2]

    async def create_job(
        self,
        job_request: JobRequest,
        user: User,
        *,
        cluster_name: Optional[str] = None,
        job_name: Optional[str] = None,
        tags: Sequence[str] = (),
        is_preemptible: bool = False,
        schedule_timeout: Optional[float] = None,
        max_run_time_minutes: Optional[int] = None,
        restart_policy: JobRestartPolicy = JobRestartPolicy.NEVER,
    ) -> Tuple[Job, Status]:
        if cluster_name:
            user_cluster = user.get_cluster(cluster_name)
            assert user_cluster
        else:
            # NOTE: left this for backward compatibility with existing tests
            user_cluster = user.clusters[0]
        cluster_name = user_cluster.name

        if job_name is not None and maybe_job_id(job_name):
            raise JobsServiceException(
                "Failed to create job: job name cannot start with 'job-' prefix."
            )
        try:
            await self._raise_for_run_time_quota(
                user,
                user_cluster,
                gpu_requested=bool(job_request.container.resources.gpu),
            )
        except GpuQuotaExceededError:
            quota = user_cluster.quota.total_gpu_run_time_delta
            await self._notifications_client.notify(
                JobCannotStartQuotaReached(
                    user_id=user.name,
                    resource=QuotaResourceType.GPU,
                    quota=quota.total_seconds(),
                    cluster_name=cluster_name,
                )
            )
            raise
        except NonGpuQuotaExceededError:
            quota = user_cluster.quota.total_non_gpu_run_time_delta
            await self._notifications_client.notify(
                JobCannotStartQuotaReached(
                    user_id=user.name,
                    resource=QuotaResourceType.NON_GPU,
                    quota=quota.total_seconds(),
                    cluster_name=cluster_name,
                )
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
            tags=tags,
            is_preemptible=is_preemptible,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
            restart_policy=restart_policy,
        )
        job_id = job_request.job_id

        job_secrets = [
            self._get_secret_name(uri)
            for uri in job_request.container.get_secret_uris()
        ]
        if job_secrets:
            async with self._get_cluster(cluster_name) as cluster:
                # Warning: contextmanager '_get_cluster' suppresses all exceptions
                missing = await cluster.orchestrator.get_missing_secrets(
                    user.name, job_secrets
                )
            if missing:
                details = ", ".join(f"'{s}'" for s in sorted(missing))
                raise JobsServiceException(f"Missing secrets: {details}")

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
                "Falling back to dummy cluster config to retrieve job '%s'", record.id
            )
            # NOTE: we may rather want to fall back to some dummy
            # OrchestratorConfig instead.
            return Job(
                storage_config=self._dummy_cluster_storage_config,
                orchestrator_config=self._dummy_cluster_orchestrator_config,
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

                    record.status = JobStatus.CANCELLED
                    record.is_deleted = True
                return
            except JobStorageTransactionError:
                logger.warning("Failed to mark a job %s as deleted. Retrying.", job_id)
        logger.warning("Failed to mark a job %s as deleted. Giving up.", job_id)

    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[Job]:
        async for record in self._jobs_storage.iter_all_jobs(
            job_filter, reverse=reverse, limit=limit
        ):
            yield await self._get_cluster_job(record)

    # Only used in tests
    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None, *, reverse: bool = False
    ) -> List[Job]:
        return [job async for job in self.iter_all_jobs(job_filter, reverse=reverse)]

    async def get_job_by_name(self, job_name: str, owner: User) -> Job:
        job_filter = JobFilter(owners={owner.name}, name=job_name)
        async for record in self._jobs_storage.iter_all_jobs(
            job_filter, reverse=True, limit=1
        ):
            return await self._get_cluster_job(record)
        raise JobError(f"no such job {job_name}")

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

    async def get_user_cluster_configs(self, user: User) -> List[ClusterConfig]:
        configs = []
        for user_cluster in user.clusters:
            try:
                async with self._get_cluster(
                    user_cluster.name, tolerate_unavailable=True
                ) as cluster:
                    configs.append(cluster.config)
            except ClusterNotFound:
                pass
        return configs

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
