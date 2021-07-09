import base64
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Iterable, List, Optional, Sequence, Tuple

from neuro_auth_client import (
    AuthClient,
    Cluster as AuthCluster,
    Permission,
    User as AuthUser,
)
from notifications_client import (
    Client as NotificationsClient,
    JobCannotStartNoCredits,
    JobTransition,
)
from yarl import URL

from platform_api.cluster import ClusterConfig, ClusterConfigRegistry, ClusterNotFound
from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import JobsConfig
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from ..user import get_cluster
from .job import (
    Job,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
    JobStatusReason,
    maybe_job_id,
)
from .job_request import JobError, JobRequest, JobStatus
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


class RunningJobsQuotaExceededError(JobsServiceException):
    def __init__(self, user: str) -> None:
        super().__init__(f"Jobs limit quota exceeded for user '{user}'")


class NoCreditsError(JobsServiceException):
    def __init__(self, user: str) -> None:
        super().__init__(f"No credits left for user '{user}'")


class JobsService:
    def __init__(
        self,
        cluster_config_registry: ClusterConfigRegistry,
        jobs_storage: JobsStorage,
        jobs_config: JobsConfig,
        notifications_client: NotificationsClient,
        auth_client: AuthClient,
        api_base_url: URL,
    ) -> None:
        self._cluster_registry = cluster_config_registry
        self._jobs_storage = jobs_storage
        self._jobs_config = jobs_config
        self._notifications_client = notifications_client

        self._max_deletion_attempts = 10

        self._dummy_cluster_orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.missing-cluster",
            jobs_internal_domain_name_template="{job_id}.missing-cluster",
            resource_pool_types=(),
        )
        self._auth_client = auth_client
        self._api_base_url = api_base_url

    def _make_job(
        self, record: JobRecord, cluster_config: Optional[ClusterConfig] = None
    ) -> Job:
        if cluster_config is not None:
            orchestrator_config = cluster_config.orchestrator
        else:
            orchestrator_config = self._dummy_cluster_orchestrator_config
        return Job(
            orchestrator_config=orchestrator_config,
            record=record,
            image_pull_error_delay=self._jobs_config.image_pull_error_delay,
        )

    async def _raise_for_running_jobs_quota(
        self, user: AuthUser, cluster: AuthCluster
    ) -> None:
        if cluster.quota.total_running_jobs is None:
            return
        running_filter = JobFilter(
            owners={user.name},
            clusters={cluster.name: {}},
            statuses={JobStatus(value) for value in JobStatus.active_values()},
        )
        running_count = len(await self._jobs_storage.get_all_jobs(running_filter))
        if running_count >= cluster.quota.total_running_jobs:
            raise RunningJobsQuotaExceededError(user.name)

    async def _raise_for_no_credits(self, user: AuthUser, cluster: AuthCluster) -> None:
        if cluster.quota.credits is not None and cluster.quota.credits <= 0:
            raise NoCreditsError(user.name)

    async def _make_pass_config_token(
        self, username: str, cluster_name: str, job_id: str
    ) -> str:
        token_uri = f"token://{cluster_name}/job/{job_id}"
        await self._auth_client.grant_user_permissions(
            username, [Permission(uri=token_uri, action="read")]
        )
        return await self._auth_client.get_user_token(username, new_token_uri=token_uri)

    async def _setup_pass_config(
        self, user: AuthUser, cluster_name: str, job_request: JobRequest
    ) -> JobRequest:
        if NEURO_PASSED_CONFIG in job_request.container.env:
            raise JobsServiceException(
                f"Cannot pass config: ENV '{NEURO_PASSED_CONFIG}' " "already specified"
            )
        token = await self._make_pass_config_token(
            user.name, cluster_name, job_request.job_id
        )
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

    async def _prepare_job_hostnames(
        self, job: Job, orchestrator_config: OrchestratorConfig
    ) -> None:
        job.internal_hostname = (
            orchestrator_config.jobs_internal_domain_name_template.format(job_id=job.id)
        )
        if job.is_named:
            from platform_api.handlers.validators import JOB_USER_NAMES_SEPARATOR

            job.internal_hostname_named = (
                orchestrator_config.jobs_internal_domain_name_template.format(
                    job_id=f"{job.name}{JOB_USER_NAMES_SEPARATOR}{job.base_owner}"
                )
            )

    async def create_job(
        self,
        job_request: JobRequest,
        user: AuthUser,
        cluster_name: str,
        *,
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
        user_cluster = get_cluster(user, cluster_name)
        assert user_cluster

        if job_name is not None and maybe_job_id(job_name):
            raise JobsServiceException(
                "Failed to create job: job name cannot start with 'job-' prefix."
            )
        if not wait_for_jobs_quota:
            await self._raise_for_running_jobs_quota(user, user_cluster)
        try:
            await self._raise_for_no_credits(user, user_cluster)
        except NoCreditsError:
            await self._notifications_client.notify(
                JobCannotStartNoCredits(
                    user_id=user.name,
                    cluster_name=cluster_name,
                )
            )
            raise

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

        try:
            cluster_config = self._cluster_registry.get(cluster_name)

            if (
                record.privileged
                and not cluster_config.orchestrator.allow_privileged_mode
            ):
                raise JobsServiceException(
                    f"Cluster {cluster_name} does not allow privileged jobs"
                )

            async with self._create_job_in_storage(record) as record:
                job = self._make_job(record, cluster_config)
                await self._prepare_job_hostnames(job, cluster_config.orchestrator)
            return job, Status.create(job.status)

        except ClusterNotFound as cluster_err:
            # NOTE: this will result in 400 HTTP response which may not be
            # what we want to convey really
            raise JobsServiceException(
                f"Cluster '{record.cluster_name}' not found"
            ) from cluster_err
        except JobsStorageException as transaction_err:
            logger.error(f"Failed to create job {job_id}: {transaction_err}")
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

    async def update_max_run_time(
        self,
        job_id: str,
        max_run_time_minutes: Optional[int] = None,
        additional_max_run_time_minutes: Optional[int] = None,
    ) -> None:
        assert (
            max_run_time_minutes is not None
            or additional_max_run_time_minutes is not None
        ), (
            "Either max_run_time_minutes or "
            "additional_max_run_time_minutes should not be None"
        )
        assert (
            max_run_time_minutes is None or additional_max_run_time_minutes is None
        ), (
            "Either max_run_time_minutes or "
            "additional_max_run_time_minutes should be None"
        )
        async with self._update_job_in_storage(job_id) as record:
            if max_run_time_minutes is not None:
                record.max_run_time_minutes = max_run_time_minutes
            else:
                assert additional_max_run_time_minutes
                record.max_run_time_minutes = record.max_run_time_minutes or 0
                record.max_run_time_minutes += additional_max_run_time_minutes

    async def _get_cluster_job(self, record: JobRecord) -> Job:
        try:
            cluster_config = self._cluster_registry.get(record.cluster_name)
            return self._make_job(record, cluster_config)
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

    async def cancel_job(self, job_id: str, reason: Optional[str] = None) -> None:
        for _ in range(self._max_deletion_attempts):
            try:
                async with self._update_job_in_storage(job_id) as record:
                    if record.is_finished:
                        # the job has already finished. nothing to do here.
                        return

                    logger.info(f"Canceling job {job_id} for reason {reason}")
                    record.status_history.current = JobStatusItem.create(
                        JobStatus.CANCELLED, reason=reason
                    )

                return
            except JobStorageTransactionError:
                logger.warning("Failed to mark a job %s as canceled. Retrying.", job_id)
        logger.warning("Failed to mark a job %s as canceled. Giving up.", job_id)

    @asyncgeneratorcontextmanager
    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[Job]:
        async with self._jobs_storage.iter_all_jobs(
            job_filter, reverse=reverse, limit=limit
        ) as it:
            async for record in it:
                yield await self._get_cluster_job(record)

    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None, *, reverse: bool = False
    ) -> List[Job]:
        async with self.iter_all_jobs(job_filter, reverse=reverse) as it:
            return [job async for job in it]

    async def get_job_by_name(self, job_name: str, owner: AuthUser) -> Job:
        job_filter = JobFilter(owners={owner.name}, name=job_name)
        async with self._jobs_storage.iter_all_jobs(
            job_filter, reverse=True, limit=1
        ) as it:
            async for record in it:
                return await self._get_cluster_job(record)
        raise JobError(f"no such job {job_name}")

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[Job]:
        records = await self._jobs_storage.get_jobs_by_ids(
            job_ids, job_filter=job_filter
        )
        return [await self._get_cluster_job(record) for record in records]

    async def drop_job(self, job_id: str) -> None:
        # TODO: drop logs/add entry to queue
        record = await self._jobs_storage.get_job(job_id)
        if not record.is_finished:
            raise JobError("Cannot drop unfinished job")
        await self._jobs_storage.drop_job(job_id)

    async def get_user_cluster_configs(self, user: AuthUser) -> List[ClusterConfig]:
        configs = []
        for user_cluster in user.clusters:
            try:
                cluster_config = self._cluster_registry.get(user_cluster.name)
                configs.append(cluster_config)
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

    async def update_job_billing(
        self,
        job_id: str,
        last_billed: datetime,
        fully_billed: bool,
        new_charge: Decimal,
    ) -> None:
        async with self._jobs_storage.try_update_job(job_id) as record:
            record.total_price_credits += new_charge
            record.last_billed = last_billed
            record.fully_billed = fully_billed

    @asyncgeneratorcontextmanager
    async def get_not_billed_jobs(self) -> AsyncIterator[Job]:
        async with self._jobs_storage.iter_all_jobs(
            JobFilter(fully_billed=False)
        ) as it:
            async for record in it:
                yield await self._get_cluster_job(record)
