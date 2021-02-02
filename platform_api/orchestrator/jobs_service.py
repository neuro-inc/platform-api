import abc
import base64
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import (
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from async_generator import asynccontextmanager
from neuro_auth_client import AuthClient, Permission
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
from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import JobsConfig, JobsSchedulerConfig
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


NEURO_PASSED_CONFIG = "NEURO_PASSED_CONFIG"


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


class RunningJobsQuotaExceededError(QuotaException):
    def __init__(self, user: str) -> None:
        super().__init__(f"jobs limit quota exceeded for user '{user}'")


@dataclass(frozen=True)
class SchedulingResult:
    jobs_to_update: List[JobRecord]
    jobs_to_suspend: List[JobRecord]


current_datetime_factory = partial(datetime.now, timezone.utc)


class JobsScheduler:
    def __init__(
        self,
        config: JobsSchedulerConfig,
        auth_client: AuthClient,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> None:
        self._config = config
        self._auth_client = auth_client
        self._current_datetime_factory = current_datetime_factory

    async def _get_user_running_jobs_quota(
        self, username: str, cluster: str
    ) -> Optional[int]:
        auth_user = await self._auth_client.get_user(username)
        user = User.create_from_auth_user(auth_user)
        user_cluster = user.get_cluster(cluster)
        if user_cluster:
            return user_cluster.jobs_quota
        return 0  # User has no access to this cluster

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


class JobsService:
    def __init__(
        self,
        cluster_registry: ClusterRegistry,
        jobs_storage: JobsStorage,
        jobs_config: JobsConfig,
        notifications_client: NotificationsClient,
        scheduler: JobsScheduler,
        auth_client: AuthClient,
        api_base_url: URL,
    ) -> None:
        self._cluster_registry = cluster_registry
        self._jobs_storage = jobs_storage
        self._jobs_config = jobs_config
        self._notifications_client = notifications_client
        self._scheduler = scheduler

        self._max_deletion_attempts = 10

        self._dummy_cluster_orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.missing-cluster",
            resource_pool_types=(),
        )
        self._auth_client = auth_client
        self._api_base_url = api_base_url

    @asynccontextmanager
    async def _get_cluster(
        self, name: str, tolerate_unavailable: bool = False
    ) -> AsyncIterator[Cluster]:
        async with self._cluster_registry.get(
            name, skip_circuit_breaker=tolerate_unavailable
        ) as cluster:
            yield cluster

    def _make_job(self, record: JobRecord, cluster: Optional[Cluster] = None) -> Job:
        if cluster is not None:
            orchestrator_config = cluster.orchestrator.config
        else:
            orchestrator_config = self._dummy_cluster_orchestrator_config
        return Job(
            orchestrator_config=orchestrator_config,
            record=record,
            image_pull_error_delay=self._jobs_config.image_pull_error_delay,
        )

    async def _raise_for_run_time_quota(
        self, user: User, user_cluster: UserCluster, gpu_requested: bool
    ) -> None:
        if not user_cluster.has_quota():
            return
        quota = user_cluster.runtime_quota
        run_times = await self._jobs_storage.get_aggregated_run_time_by_clusters(
            user.name
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

    async def _raise_for_running_jobs_quota(
        self, user: User, user_cluster: UserCluster
    ) -> None:
        if user_cluster.jobs_quota is None:
            return
        running_filter = JobFilter(
            owners={user.name},
            clusters={user_cluster.name: {}},
            statuses={JobStatus(value) for value in JobStatus.active_values()},
        )
        running_count = len(await self._jobs_storage.get_all_jobs(running_filter))
        if running_count >= user_cluster.jobs_quota:
            raise RunningJobsQuotaExceededError(user.name)

    async def _check_secrets(self, cluster_name: str, job_request: JobRequest) -> None:
        grouped_secrets = job_request.container.get_user_secrets()
        if not grouped_secrets:
            return

        missing = []
        async with self._get_cluster(cluster_name) as cluster:
            # Warning: contextmanager '_get_cluster' suppresses all exceptions
            for user_name, user_secrets in grouped_secrets.items():
                missing.extend(
                    await cluster.orchestrator.get_missing_secrets(
                        user_name, [secret.secret_key for secret in user_secrets]
                    )
                )
        if missing:
            details = ", ".join(f"'{s}'" for s in sorted(missing))
            raise JobsServiceException(f"Missing secrets: {details}")

    async def _make_pass_config_token(self, username: str, job_id: str) -> str:
        token_uri = f"token://job/{job_id}"
        await self._auth_client.grant_user_permissions(
            username, [Permission(uri=token_uri, action="read")]
        )
        return await self._auth_client.get_user_token(username, new_token_uri=token_uri)

    async def _setup_pass_config(
        self, user: User, cluster_name: str, job_request: JobRequest
    ) -> JobRequest:
        if NEURO_PASSED_CONFIG in job_request.container.env:
            raise JobsServiceException(
                f"Cannot pass config: ENV '{NEURO_PASSED_CONFIG}' " "already specified"
            )
        token = await self._make_pass_config_token(user.name, job_request.job_id)
        pass_config_data = base64.b64encode(
            json.dumps(
                {
                    "token": token,
                    "cluster": cluster_name,
                    "url": str(self._api_base_url),
                }
            ).encode()
        ).decode()
        new_env = {
            **job_request.container.env,
            NEURO_PASSED_CONFIG: pass_config_data,
        }
        new_container = replace(job_request.container, env=new_env)
        return replace(job_request, container=new_container)

    async def create_job(
        self,
        job_request: JobRequest,
        user: User,
        *,
        cluster_name: Optional[str] = None,
        job_name: Optional[str] = None,
        preset_name: Optional[str] = None,
        tags: Sequence[str] = (),
        scheduler_enabled: bool = False,
        preemptible_node: bool = False,
        pass_config: bool = False,
        wait_for_jobs_quota: bool = False,
        privileged: bool = False,
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
            quota = user_cluster.runtime_quota.total_gpu_run_time_delta
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
            quota = user_cluster.runtime_quota.total_non_gpu_run_time_delta
            await self._notifications_client.notify(
                JobCannotStartQuotaReached(
                    user_id=user.name,
                    resource=QuotaResourceType.NON_GPU,
                    quota=quota.total_seconds(),
                    cluster_name=cluster_name,
                )
            )
            raise
        if not wait_for_jobs_quota:
            await self._raise_for_running_jobs_quota(user, user_cluster)
        if pass_config:
            job_request = await self._setup_pass_config(user, cluster_name, job_request)

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
            preset_name=preset_name,
            tags=tags,
            scheduler_enabled=scheduler_enabled,
            preemptible_node=preemptible_node,
            pass_config=pass_config,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
            restart_policy=restart_policy,
            privileged=privileged,
        )
        job_id = job_request.job_id

        await self._check_secrets(cluster_name, job_request)

        job_disks = [
            disk_volume.disk for disk_volume in job_request.container.disk_volumes
        ]
        if job_disks:
            async with self._get_cluster(cluster_name) as cluster:
                # Warning: contextmanager '_get_cluster' suppresses all exceptions
                missing = await cluster.orchestrator.get_missing_disks(job_disks)
            if missing:
                details = ", ".join(f"'{disk.disk_id}'" for disk in sorted(missing))
                raise JobsServiceException(f"Missing disks: {details}")

        if record.privileged:
            async with self._get_cluster(cluster_name) as cluster:
                # Warning: contextmanager '_get_cluster' suppresses all exceptions,
                privileged_mode_allowed = (
                    cluster.config.orchestrator.allow_privileged_mode
                )
            if not privileged_mode_allowed:
                raise JobsServiceException(
                    f"Cluster {cluster_name} does not allow privileged jobs"
                )

        try:
            async with self._create_job_in_storage(record) as record:
                async with self._get_cluster(record.cluster_name) as cluster:
                    job = self._make_job(record, cluster)
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

    async def set_job_materialized(self, job_id: str, materialized: bool) -> None:
        async with self._update_job_in_storage(job_id) as record:
            record.materialized = materialized

    async def _get_cluster_job(self, record: JobRecord) -> Job:
        try:
            async with self._get_cluster(
                record.cluster_name, tolerate_unavailable=True
            ) as cluster:
                return self._make_job(record, cluster)
        except ClusterNotFound:
            # in case the cluster is missing, we still want to return the job
            # to be able to render a proper HTTP response, therefore we have
            # the fallback logic that uses the dummy cluster instead.
            logger.warning(
                "Falling back to dummy cluster config to retrieve job '%s'", record.id
            )
            return self._make_job(record)

    async def get_job(self, job_id: str) -> Job:
        record = await self._jobs_storage.get_job(job_id)
        return await self._get_cluster_job(record)

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

    async def cancel_job(self, job_id: str) -> None:
        for _ in range(self._max_deletion_attempts):
            try:
                async with self._update_job_in_storage(job_id) as record:
                    if record.is_finished:
                        # the job has already finished. nothing to do here.
                        return

                    logger.info("Canceling job %s", job_id)
                    record.status = JobStatus.CANCELLED

                return
            except JobStorageTransactionError:
                logger.warning("Failed to mark a job %s as canceled. Retrying.", job_id)
        logger.warning("Failed to mark a job %s as canceled. Giving up.", job_id)

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


class JobsPollerApi(abc.ABC):
    async def get_unfinished_jobs(self) -> List[JobRecord]:
        raise NotImplementedError

    async def get_jobs_for_deletion(self, *, delay: timedelta) -> List[JobRecord]:
        raise NotImplementedError

    async def push_status(self, job_id: str, status: JobStatusItem) -> None:
        raise NotImplementedError

    async def set_materialized(self, job_id: str, materialized: bool) -> None:
        raise NotImplementedError


class JobsPollerService:
    def __init__(
        self,
        cluster_registry: ClusterRegistry,
        jobs_config: JobsConfig,
        scheduler: JobsScheduler,
        auth_client: AuthClient,
        api: JobsPollerApi,
    ) -> None:
        self._cluster_registry = cluster_registry
        self._jobs_config = jobs_config
        self._scheduler = scheduler
        self._api = api

        self._max_deletion_attempts = 10

        self._dummy_cluster_orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.missing-cluster",
            resource_pool_types=(),
        )
        self._auth_client = auth_client

    @asynccontextmanager
    async def _get_cluster(
        self, name: str, tolerate_unavailable: bool = False
    ) -> AsyncIterator[Cluster]:
        async with self._cluster_registry.get(
            name, skip_circuit_breaker=tolerate_unavailable
        ) as cluster:
            yield cluster

    async def update_jobs_statuses(
        self,
    ) -> None:
        unfinished = await self._api.get_unfinished_jobs()
        result = await self._scheduler.schedule(unfinished)

        for record in result.jobs_to_update:
            await self._update_job_status_wrapper(record)

        for record in result.jobs_to_suspend:
            await self._suspend_job_wrapper(record)

        for record in await self._api.get_jobs_for_deletion(
            delay=self._jobs_config.deletion_delay
        ):
            # finished, but not yet dematerialized jobs
            # assert job.is_finished and job.materialized
            await self._delete_job_wrapper(record)

    def _make_job(self, record: JobRecord, cluster: Optional[Cluster] = None) -> Job:
        if cluster is not None:
            orchestrator_config = cluster.orchestrator.config
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
                await orchestrator.start_job(job)
                status_item = job.status_history.current
                job.materialized = True
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
                job.materialized = False
                await self._revoke_pass_config(job)
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
        if job.pass_config:
            token_uri = f"token://job/{job.id}"
            await self._auth_client.revoke_user_permissions(job.owner, [token_uri])

    async def _get_cluster_job(self, record: JobRecord) -> Job:
        try:
            async with self._get_cluster(
                record.cluster_name, tolerate_unavailable=True
            ) as cluster:
                return self._make_job(record, cluster)
        except ClusterNotFound:
            # in case the cluster is missing, we still want to return the job
            # to be able to render a proper HTTP response, therefore we have
            # the fallback logic that uses the dummy cluster instead.
            logger.warning(
                "Falling back to dummy cluster config to retrieve job '%s'", record.id
            )
            return self._make_job(record)

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
        """
        Wrapper around self._jobs_storage.try_update_job() with notification
        """
        status_cnt = len(record.status_history.all)
        initial_materialized = record.materialized
        yield record
        for status in record.status_history.all[status_cnt:]:
            await self._api.push_status(record.id, status)
        if initial_materialized != record.materialized:
            await self._api.set_materialized(record.id, record.materialized)
