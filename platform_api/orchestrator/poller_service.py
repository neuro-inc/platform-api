import abc
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import AsyncIterator, Callable, Dict, List, Optional, Tuple, Union

from aiohttp import ClientResponseError
from neuro_admin_client import AdminClient
from neuro_auth_client import AuthClient

from platform_api.cluster import (
    Cluster,
    ClusterHolder,
    ClusterNotAvailable,
    ClusterNotFound,
)
from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import JobsConfig, JobsSchedulerConfig

from ..utils.asyncio import run_and_log_exceptions
from ..utils.retry import retries
from .base import Orchestrator
from .job import Job, JobRecord, JobStatusItem, JobStatusReason
from .job_request import (
    JobAlreadyExistsException,
    JobError,
    JobException,
    JobNotFoundException,
    JobStatus,
)
from .jobs_storage import JobStorageTransactionError


logger = logging.getLogger(__file__)


@dataclass(frozen=True)
class SchedulingResult:
    jobs_to_update: List[JobRecord]
    jobs_to_suspend: List[JobRecord]


current_datetime_factory = partial(datetime.now, timezone.utc)


class JobsScheduler:
    def __init__(
        self,
        config: JobsSchedulerConfig,
        admin_client: AdminClient,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> None:
        self._config = config
        self._admin_client = admin_client
        self._current_datetime_factory = current_datetime_factory

    async def _get_user_running_jobs_quota(
        self, username: str, cluster: str
    ) -> Optional[int]:
        try:
            base_name = username.split("/", 1)[0]  # SA same quota as user
            cluster_user = await self._admin_client.get_cluster_user(
                cluster_name=cluster, user_name=base_name
            )
        except ClientResponseError as e:
            if e.status == 404:
                # User has no access to this cluster
                return 0
            raise
        else:
            return cluster_user.quota.total_running_jobs

    async def _enforce_running_job_quota(
        self, raw_result: SchedulingResult
    ) -> SchedulingResult:
        jobs_to_update: List[JobRecord] = []

        # Grouping by (username, cluster_name):
        grouped_jobs: Dict[Tuple[str, str], List[JobRecord]] = defaultdict(list)
        for record in raw_result.jobs_to_update:
            grouped_jobs[(record.owner, record.cluster_name)].append(record)

        # Filter jobs
        for (username, cluster), jobs in grouped_jobs.items():
            quota = await self._get_user_running_jobs_quota(username, cluster)
            if quota is not None:
                materialized_jobs = [job for job in jobs if job.materialized]
                not_materialized = [job for job in jobs if not job.materialized]
                jobs_to_update.extend(materialized_jobs)
                free_places = quota - len(materialized_jobs)
                if free_places > 0:
                    not_materialized = sorted(
                        not_materialized, key=lambda job: job.status_history.created_at
                    )
                    jobs_to_update.extend(not_materialized[:free_places])
            else:
                jobs_to_update.extend(jobs)

        return SchedulingResult(
            jobs_to_update=jobs_to_update,
            jobs_to_suspend=raw_result.jobs_to_suspend,
        )

    async def schedule(self, unfinished: List[JobRecord]) -> SchedulingResult:
        jobs_to_update: List[JobRecord] = []
        jobs_to_suspend: List[JobRecord] = []
        now = self._current_datetime_factory()

        # Always start/update not scheduled jobs
        jobs_to_update.extend(job for job in unfinished if not job.scheduler_enabled)

        scheduled = [job for job in unfinished if job.scheduler_enabled]

        not_materialized = sorted(
            (job for job in scheduled if not job.materialized),
            key=lambda job: job.status_history.current.transition_time,
        )
        materialized_not_running = [
            job
            for job in scheduled
            if job.status != JobStatus.RUNNING and job.materialized
        ]
        running = [job for job in scheduled if job.status == JobStatus.RUNNING]

        waiting_exists = any(
            now - job.status_history.current.transition_time
            >= self._config.is_waiting_min_time
            for job in materialized_not_running
        )

        # not_materialized processing

        for job in not_materialized:
            suspended_at = job.status_history.current.transition_time
            if (
                not waiting_exists
                or now - suspended_at >= self._config.max_suspended_time
            ):
                jobs_to_update.append(job)

        # materialized_not_running processing

        jobs_to_update.extend(materialized_not_running)

        # running processing

        for job in running:
            continued_at = job.status_history.continued_at
            assert continued_at, "Running job should have continued_at"
            if now - continued_at >= self._config.run_quantum and waiting_exists:
                jobs_to_suspend.append(job)
            else:
                jobs_to_update.append(job)

        result = SchedulingResult(
            jobs_to_update=jobs_to_update,
            jobs_to_suspend=jobs_to_suspend,
        )
        return await self._enforce_running_job_quota(result)


class JobsPollerApi(abc.ABC):
    async def get_unfinished_jobs(self) -> List[JobRecord]:
        raise NotImplementedError

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> List[JobRecord]:
        raise NotImplementedError

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        raise NotImplementedError

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        raise NotImplementedError


async def _revoke_pass_config(
    auth_client: AuthClient, job: Union[JobRecord, Job]
) -> None:
    if job.pass_config:
        token_uri = f"token://{job.cluster_name}/job/{job.id}"
        try:
            await auth_client.revoke_user_permissions(job.owner, [token_uri])
        except ClientResponseError as e:
            if e.status == 400 and e.message == "Operation has no effect":
                # Token permission was already revoked
                return
            raise


class JobsPollerService:
    def __init__(
        self,
        cluster_holder: ClusterHolder,
        jobs_config: JobsConfig,
        scheduler: JobsScheduler,
        auth_client: AuthClient,
        api: JobsPollerApi,
    ) -> None:
        self._cluster_holder = cluster_holder
        self._jobs_config = jobs_config
        self._scheduler = scheduler
        self._api = api

        self._max_deletion_attempts = 10

        self._dummy_cluster_orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.missing-cluster",
            jobs_internal_domain_name_template="{job_id}.missing-cluster",
            resource_pool_types=(),
        )
        self._auth_client = auth_client

    async def _check_secrets(self, job: Job, orchestrator: Orchestrator) -> None:
        grouped_secrets = job.request.container.get_user_secrets()
        if not grouped_secrets:
            return

        missing = []
        for user_name, user_secrets in grouped_secrets.items():
            missing.extend(
                await orchestrator.get_missing_secrets(
                    user_name, [secret.secret_key for secret in user_secrets]
                )
            )
        if missing:
            details = ", ".join(f"'{s}'" for s in sorted(missing))
            raise JobError(f"Missing secrets: {details}")

    async def _check_disks(self, job: Job, orchestrator: Orchestrator) -> None:
        job_disks = [
            disk_volume.disk for disk_volume in job.request.container.disk_volumes
        ]
        if job_disks:
            missing = await orchestrator.get_missing_disks(job_disks)
            if missing:
                details = ", ".join(f"'{disk.disk_id}'" for disk in missing)
                raise JobError(f"Missing disks: {details}")

    @asynccontextmanager
    async def _get_cluster(self, name: str) -> AsyncIterator[Cluster]:
        async with self._cluster_holder.get() as cluster:
            assert cluster.name == name, "Poller tried to access different cluster"
            yield cluster

    async def update_jobs_statuses(
        self,
    ) -> None:
        unfinished = await self._api.get_unfinished_jobs()
        result = await self._scheduler.schedule(unfinished)

        await run_and_log_exceptions(
            self._update_job_status_wrapper(record) for record in result.jobs_to_update
        )

        await run_and_log_exceptions(
            self._suspend_job_wrapper(record) for record in result.jobs_to_suspend
        )

        await run_and_log_exceptions(
            self._delete_job_wrapper(record)
            for record in await self._api.get_jobs_for_deletion(
                delay=self._jobs_config.deletion_delay
            )
        )

    def _make_job(self, record: JobRecord, cluster: Optional[Cluster] = None) -> Job:
        if cluster is not None:
            orchestrator_config = cluster.config.orchestrator
        else:
            orchestrator_config = self._dummy_cluster_orchestrator_config
        return Job(
            orchestrator_config=orchestrator_config,
            record=record,
            image_pull_error_delay=self._jobs_config.image_pull_error_delay,
        )

    async def _update_job_status_wrapper(self, job_record: JobRecord) -> None:
        try:
            async with self._update_job(job_record) as record:
                try:
                    async with self._get_cluster(record.cluster_name) as cluster:
                        job = self._make_job(record, cluster)
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
                    record.materialized = False
                    await self._revoke_pass_config(record)
                except ClusterNotAvailable:
                    # skipping job status update
                    pass
        except JobStorageTransactionError:
            # intentionally ignoring any transaction failures here because
            # the job may have been changed and a retry is needed.
            pass

    async def _suspend_job_wrapper(self, job_record: JobRecord) -> None:
        try:
            async with self._update_job(job_record) as record:
                await self._delete_cluster_job(record)

                record.status = JobStatus.SUSPENDED
                record.materialized = False
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

        if not job.materialized:
            try:
                await self._check_secrets(job, orchestrator)
                await self._check_disks(job, orchestrator)
                await orchestrator.start_job(job)
                status_item = job.status_history.current
                job.materialized = True
            except JobAlreadyExistsException:
                logger.info(f"Job '{job.id}' already exists.")
                status_item = job.status_history.current
                job.materialized = True
            except JobError as exc:
                logger.exception("Failed to start job %s. Reason: %s", job.id, exc)
                status_item = JobStatusItem.create(
                    JobStatus.FAILED,
                    reason=str(exc),
                    description="The job could not be started.",
                )
                job.materialized = False
                await self._revoke_pass_config(job)
        else:
            try:
                for retry in retries("Fail to fetch a job", (JobNotFoundException,)):
                    async with retry:
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
                job.materialized = False
                await self._revoke_pass_config(job)

        if old_status_item != status_item:
            job.status_history.current = status_item
            logger.info(
                "Job %s transitioned from %s to %s",
                job.id,
                old_status_item.status.name,
                status_item.status.name,
            )

    async def _delete_job_wrapper(self, job_record: JobRecord) -> None:
        try:
            async with self._update_job(job_record) as record:
                await self._delete_cluster_job(record)

                # marking finished job as deleted
                record.materialized = False
                await self._revoke_pass_config(record)
        except JobStorageTransactionError:
            # intentionally ignoring any transaction failures here because
            # the job may have been changed and a retry is needed.
            pass

    async def _revoke_pass_config(self, job: Union[JobRecord, Job]) -> None:
        await _revoke_pass_config(self._auth_client, job)

    async def _delete_cluster_job(self, record: JobRecord) -> None:
        try:
            async with self._get_cluster(record.cluster_name) as cluster:
                job = self._make_job(record, cluster)
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

    @asynccontextmanager
    async def _update_job(self, record: JobRecord) -> AsyncIterator[JobRecord]:
        status_cnt = len(record.status_history.all)
        initial_materialized = record.materialized
        yield record
        for status in record.status_history.all[status_cnt:]:
            await self._api.push_status(record.id, status)
        if initial_materialized != record.materialized:
            await self._api.set_materialized(record.id, record.materialized)
