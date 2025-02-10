import abc
import itertools
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import partial

from aiohttp import ClientResponseError
from neuro_admin_client import AdminClient
from neuro_auth_client import AuthClient

from platform_api.cluster import (
    Cluster,
    ClusterHolder,
    ClusterNotAvailable,
    ClusterNotFound,
)
from platform_api.cluster_config import ClusterConfig, OrchestratorConfig
from platform_api.config import JobsConfig, JobsSchedulerConfig

from ..utils.asyncio import run_and_log_exceptions
from ..utils.retry import retries
from .base import Orchestrator
from .job import Job, JobPriority, JobRecord, JobStatusItem, JobStatusReason
from .job_request import (
    JobAlreadyExistsException,
    JobError,
    JobException,
    JobNotFoundException,
    JobStatus,
    JobUnschedulableException,
)
from .jobs_storage import JobStorageTransactionError

logger = logging.getLogger(__file__)


@dataclass(frozen=True)
class SchedulingResult:
    jobs_to_start: list[JobRecord] = field(default_factory=list)
    jobs_to_update: list[JobRecord] = field(default_factory=list)
    # `jobs_to_replace` is a list of jobs that may be replaced by `jobs_to_start` in
    # case of a congested cluster.
    jobs_to_replace: list[JobRecord] = field(default_factory=list)
    jobs_to_suspend: list[JobRecord] = field(default_factory=list)


current_datetime_factory = partial(datetime.now, UTC)


class JobsScheduler:
    def __init__(
        self,
        *,
        config: JobsSchedulerConfig,
        admin_client: AdminClient,
        cluster_holder: ClusterHolder,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> None:
        self._config = config
        self._admin_client = admin_client
        self._cluster_holder = cluster_holder
        self._current_datetime_factory = current_datetime_factory

    async def _get_user_running_jobs_quota(
        self, username: str, cluster: str, org_name: str | None
    ) -> int | None:
        try:
            base_name = username.split("/", 1)[0]  # SA same quota as user
            cluster_user = await self._admin_client.get_cluster_user(
                cluster_name=cluster, user_name=base_name, org_name=org_name
            )
        except ClientResponseError as e:
            if e.status == 404:
                # User has no access to this cluster
                return 0
            raise
        else:
            return cluster_user.quota.total_running_jobs

    async def _get_org_running_jobs_quota(
        self, cluster: str, org_name: str | None
    ) -> int | None:
        if org_name is None:
            return None
        try:
            org_cluster = await self._admin_client.get_org_cluster(
                cluster_name=cluster, org_name=org_name
            )
        except ClientResponseError as e:
            if e.status == 404:
                # User has no access to this cluster
                return 0
            raise
        else:
            return org_cluster.quota.total_running_jobs

    async def _enforce_running_job_quota(
        self, unfinished: list[JobRecord]
    ) -> list[JobRecord]:
        if not unfinished:
            return unfinished

        jobs_to_update: list[JobRecord] = []

        # Grouping by (username, cluster_name, org_name):
        grouped_jobs: dict[tuple[str, str, str | None], list[JobRecord]] = defaultdict(
            list
        )
        for record in unfinished:
            grouped_jobs[(record.owner, record.cluster_name, record.org_name)].append(
                record
            )

        def _filter_our_for_quota(
            quota: int | None, jobs: list[JobRecord]
        ) -> list[JobRecord]:
            if quota is not None:
                materialized_jobs = [job for job in jobs if job.materialized]
                not_materialized = [job for job in jobs if not job.materialized]
                free_places = quota - len(materialized_jobs)
                result = materialized_jobs
                if free_places > 0:
                    not_materialized = sorted(
                        not_materialized, key=lambda job: job.status_history.created_at
                    )
                    result += not_materialized[:free_places]
                return result
            return jobs

        # Filter jobs by user quota
        for (username, cluster, org_name), jobs in grouped_jobs.items():
            quota = await self._get_user_running_jobs_quota(username, cluster, org_name)
            jobs_to_update.extend(_filter_our_for_quota(quota, jobs))

        # Grouping by (cluster_name, org_name):
        grouped_by_org_jobs: dict[tuple[str, str | None], list[JobRecord]] = (
            defaultdict(list)
        )
        for record in jobs_to_update:
            grouped_by_org_jobs[(record.cluster_name, record.org_name)].append(record)
        jobs_to_update = []

        # Filter jobs by org quota
        for (cluster, org_name), jobs in grouped_by_org_jobs.items():
            quota = await self._get_org_running_jobs_quota(cluster, org_name)
            jobs_to_update.extend(_filter_our_for_quota(quota, jobs))

        return jobs_to_update

    async def _get_cluster_config(self) -> ClusterConfig | None:
        try:
            async with self._cluster_holder.get() as cluster:
                return cluster.config
        except (ClusterNotFound, ClusterNotAvailable):
            return None

    def _check_energy_schedule(
        self,
        *,
        job: JobRecord,
        cluster_config: ClusterConfig | None,
        current_time: datetime,
    ) -> bool:
        if (
            not cluster_config
            or not cluster_config.orchestrator.allow_scheduler_enabled_job
            or not job.scheduler_enabled
        ):
            return True
        schedule = cluster_config.energy.get_schedule(job.energy_schedule_name)
        return schedule.check_time(current_time)

    async def schedule(self, unfinished: list[JobRecord]) -> SchedulingResult:
        now = self._current_datetime_factory()
        cluster_config = await self._get_cluster_config()
        unfinished = await self._enforce_running_job_quota(unfinished)

        if not unfinished:
            return SchedulingResult()

        jobs_to_start: list[JobRecord] = []
        jobs_to_update: list[JobRecord] = []
        jobs_to_replace: list[JobRecord] = []
        jobs_to_suspend: list[JobRecord] = []

        # Process pending jobs
        for job in unfinished:
            if not job.status.is_pending:
                continue
            if not self._check_energy_schedule(
                job=job, cluster_config=cluster_config, current_time=now
            ):
                continue
            jobs_to_start.append(job)

        max_job_to_start_priority = JobPriority.LOW
        if jobs_to_start:
            max_job_to_start_priority = max(job.priority for job in jobs_to_start)

        # Process running jobs
        for job in unfinished:
            if not job.status.is_running:
                continue
            if not self._check_energy_schedule(
                job=job, cluster_config=cluster_config, current_time=now
            ):
                jobs_to_suspend.append(job)
                continue
            if job.scheduler_enabled:
                continued_at = job.status_history.continued_at
                assert continued_at, "Running job should have continued_at"
                if now - continued_at < self._config.run_quantum:
                    jobs_to_update.append(job)
                else:
                    jobs_to_replace.append(job)
            else:
                jobs_to_update.append(job)

        # Process suspended jobs
        for job in unfinished:
            if not job.status.is_suspended:
                continue
            fits_energy_schedule = self._check_energy_schedule(
                job=job, cluster_config=cluster_config, current_time=now
            )
            suspended_at = job.status_history.current.transition_time
            if fits_energy_schedule and (
                not jobs_to_start
                or job.priority > max_job_to_start_priority
                or now - suspended_at >= self._config.max_suspended_time
            ):
                jobs_to_start.append(job)

        # Always give priority to materialized jobs
        # as they have already been created in orchestrator
        jobs_to_start.sort(
            key=lambda job: (
                job.materialized,
                job.priority,
                now - job.status_history.current.transition_time,
            ),
            reverse=True,
        )

        return SchedulingResult(
            jobs_to_start=jobs_to_start,
            jobs_to_update=jobs_to_update,
            jobs_to_replace=jobs_to_replace,
            jobs_to_suspend=jobs_to_suspend,
        )


class JobsPollerApi(abc.ABC):
    async def get_unfinished_jobs(self) -> list[JobRecord]:
        raise NotImplementedError

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> list[JobRecord]:
        raise NotImplementedError

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        raise NotImplementedError

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        raise NotImplementedError


async def _revoke_pass_config(auth_client: AuthClient, job: JobRecord | Job) -> None:
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
            presets=(),
        )
        self._auth_client = auth_client

    async def _check_secrets(self, job: Job, orchestrator: Orchestrator) -> None:
        grouped_secrets = job.request.container.get_path_to_secrets()
        if not grouped_secrets:
            return

        missing = []
        for secret_path, path_secrets in grouped_secrets.items():
            missing.extend(
                await orchestrator.get_missing_secrets(
                    secret_path, [secret.secret_key for secret in path_secrets]
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

    async def update_jobs_statuses(self) -> None:
        unfinished = await self._api.get_unfinished_jobs()
        result = await self._scheduler.schedule(unfinished)

        await run_and_log_exceptions(
            self._start_jobs_wrapper(
                result.jobs_to_start, result.jobs_to_replace, result.jobs_to_suspend
            )
        )

        await run_and_log_exceptions(
            self._update_job_status_wrapper(record) for record in result.jobs_to_update
        )

        await run_and_log_exceptions(
            self._delete_job_wrapper(record)
            for record in await self._api.get_jobs_for_deletion(
                delay=self._jobs_config.deletion_delay
            )
        )

    def _make_job(self, record: JobRecord, cluster: Cluster | None = None) -> Job:
        if cluster is not None:
            orchestrator_config = cluster.config.orchestrator
        else:
            orchestrator_config = self._dummy_cluster_orchestrator_config
        return Job(
            orchestrator_config=orchestrator_config,
            record=record,
            image_pull_error_delay=self._jobs_config.image_pull_error_delay,
        )

    async def _start_jobs_wrapper(
        self,
        records_to_start: list[JobRecord],
        records_to_replace: list[JobRecord],
        records_to_suspend: list[JobRecord],
    ) -> None:
        try:
            async with AsyncExitStack() as stack:
                for record in itertools.chain(
                    records_to_start, records_to_replace, records_to_suspend
                ):
                    await stack.enter_async_context(self._update_job(record))

                try:
                    async with self._cluster_holder.get() as cluster:
                        await self._suspend_jobs(
                            cluster=cluster, records=records_to_suspend
                        )
                        await self._start_jobs(
                            cluster, records_to_start, records_to_replace
                        )
                except ClusterNotFound as cluster_err:
                    for record in itertools.chain(
                        records_to_start, records_to_replace, records_to_suspend
                    ):
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

    async def _start_jobs(
        self,
        cluster: Cluster,
        records_to_start: list[JobRecord],
        records_to_suspend: list[JobRecord],
    ) -> None:
        try:
            if not records_to_start:
                return

            if (
                not cluster.config.orchestrator.allow_scheduler_enabled_job
                and not cluster.config.orchestrator.allow_job_priority
            ):
                # Clusters without job scheduler_enabled presets
                # and priorities. We can start all jobs at once.
                for record in records_to_start:
                    job = self._make_job(record, cluster)
                    await self._update_job_status(cluster.orchestrator, job)
                return

            jobs_to_start = [self._make_job(r, cluster) for r in records_to_start]
            jobs_to_suspend = [self._make_job(r, cluster) for r in records_to_suspend]
            scheduled_jobs = await cluster.orchestrator.get_scheduled_jobs(
                jobs_to_start
            )
            scheduled_job_ids = {r.id for r in scheduled_jobs}
            schedulable_jobs = await cluster.orchestrator.get_schedulable_jobs(
                jobs_to_start
            )
            schedulable_job_ids = {r.id for r in schedulable_jobs}
            stop_materializing = False

            for job in jobs_to_start:
                if job.materialized and job.id in scheduled_job_ids:
                    await self._update_job_status(cluster.orchestrator, job)
                    continue
                if not job.materialized and stop_materializing:
                    break
                if job.id in schedulable_job_ids:
                    await self._update_job_status(cluster.orchestrator, job)
                    # Do not materialize next jobs until job is scheduled
                    stop_materializing = True
                    continue
                suspended_jobs = await self._replace_jobs(
                    cluster.orchestrator, job, jobs_to_suspend
                )
                if suspended_jobs:
                    await self._update_job_status(cluster.orchestrator, job)
                    # Do not materialize next jobs until job is scheduled
                    stop_materializing = True
                    continue
                # For some reason there are no resources for the job, it can
                # be skipped until other jobs finish and free some resources.

                # Even if there are no free resources job could be materialized
                # during previous poller cycles because we expected that there are
                # enough resources.
                if job.materialized:
                    await cluster.orchestrator.delete_job(job)
                    await self._revoke_pass_config(job)
                    job.materialized = False

            # `_replace_jobs` may have suspended some but not all jobs.
            # If jobs were not suspended, we either want to start them, or update their
            # statuses.
            for job in jobs_to_suspend:
                if job.status != JobStatus.SUSPENDED:
                    await self._update_job_status(cluster.orchestrator, job)
        except JobException as exc:
            # if the job is missing, we still want to mark
            # the job as deleted. suppressing.
            logger.warning("Could not start jobs. Reason: '%s'", exc)

    async def _replace_jobs(
        self,
        orchestrator: Orchestrator,
        job_to_start: Job,
        jobs_to_suspend: list[Job],
    ) -> list[Job]:
        try:
            jobs_to_suspend = [
                job for job in jobs_to_suspend if job_to_start.priority >= job.priority
            ]
            suspended_jobs = await orchestrator.preempt_jobs(
                [job_to_start], jobs_to_suspend
            )
            for job in suspended_jobs:
                job.status = JobStatus.SUSPENDED
                job.materialized = False
            return suspended_jobs
        except JobError as exc:
            logger.info(
                "Failed to suspend jobs for job %r. Reason: %s", job_to_start.id, exc
            )
            job_to_start.status_history.current = JobStatusItem.create(
                JobStatus.FAILED,
                reason=str(exc),
                description="The job could not be started.",
            )
            await self._revoke_pass_config(job_to_start)
            return []

    async def _suspend_jobs(
        self, *, cluster: Cluster, records: list[JobRecord]
    ) -> None:
        if not records:
            return
        try:
            for record in records:
                job = self._make_job(record, cluster)
                await cluster.orchestrator.delete_job(job)
                job.status = JobStatus.SUSPENDED
                job.materialized = False
        except JobException as exc:
            logger.info("Failed to suspend jobs. Reason: %s", exc)

    async def _update_job_status_wrapper(self, job_record: JobRecord) -> None:
        try:
            async with self._update_job(job_record) as record:
                try:
                    async with self._get_cluster(record.cluster_name) as cluster:
                        job = self._make_job(record, cluster)
                        await self._update_job_status(cluster.orchestrator, job)
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
                if not isinstance(exc, JobUnschedulableException):
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

        job.collect_if_needed()

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

    async def _revoke_pass_config(self, job: JobRecord | Job) -> None:
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
