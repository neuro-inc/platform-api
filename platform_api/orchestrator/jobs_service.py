import base64
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import timedelta

from aiohttp import ClientResponseError
from neuro_admin_client import (
    AdminClient,
    ClusterUser,
    GetUserResponse,
    Org,
    OrgCluster,
    OrgUser,
    ProjectUser,
    User as AdminUser,
)
from neuro_auth_client import AuthClient, Permission, User as AuthUser
from neuro_notifications_client import (
    Client as NotificationsClient,
    JobCannotStartNoCredits,
    JobTransition,
)
from yarl import URL

from platform_api.cluster import ClusterConfig, ClusterConfigRegistry, ClusterNotFound
from platform_api.cluster_config import OrchestratorConfig, VolumeConfig
from platform_api.config import JobsConfig
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from .job import (
    Job,
    JobPriority,
    JobRecord,
    JobRestartPolicy,
    JobStatusHistory,
    JobStatusItem,
    JobStatusReason,
    get_base_owner,
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
    def __init__(self, quota_owner: str) -> None:
        super().__init__(f"Jobs limit quota exceeded for {quota_owner}")

    @classmethod
    def create_for_user(cls, user: str) -> "RunningJobsQuotaExceededError":
        return RunningJobsQuotaExceededError(f"user '{user}'")

    @classmethod
    def create_for_org(cls, org: str) -> "RunningJobsQuotaExceededError":
        return RunningJobsQuotaExceededError(f"org '{org}'")


class NoCreditsError(JobsServiceException):
    def __init__(self, quota_owner: str) -> None:
        super().__init__(f"No credits left for {quota_owner}")

    @classmethod
    def create_for_user(cls, user: str) -> "NoCreditsError":
        return NoCreditsError(f"user '{user}'")

    @classmethod
    def create_for_org(cls, org: str) -> "NoCreditsError":
        return NoCreditsError(f"org '{org}'")


@dataclass(frozen=True)
class UserClusterConfig:
    config: ClusterConfig
    # None value means the direct access to cluster without any or:
    orgs: list[str | None]


@dataclass(frozen=True)
class UserConfig:
    orgs: list[OrgUser]
    clusters: list[UserClusterConfig]
    projects: list[ProjectUser]


class JobsService:
    def __init__(
        self,
        cluster_config_registry: ClusterConfigRegistry,
        jobs_storage: JobsStorage,
        jobs_config: JobsConfig,
        notifications_client: NotificationsClient,
        auth_client: AuthClient,
        admin_client: AdminClient,
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
            presets=(),
        )
        self._auth_client = auth_client
        self._admin_client = admin_client
        self._api_base_url = api_base_url

    def _make_job(
        self, record: JobRecord, cluster_config: ClusterConfig | None = None
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

    async def _raise_for_running_jobs_quota(self, cluster_user: ClusterUser) -> None:
        if cluster_user.quota.total_running_jobs is None:
            return
        running_filter = JobFilter(
            owners={cluster_user.user_name},
            clusters={cluster_user.cluster_name: {}},
            statuses={JobStatus(value) for value in JobStatus.active_values()},
        )
        running_count = len(await self._jobs_storage.get_all_jobs(running_filter))
        if running_count >= cluster_user.quota.total_running_jobs:
            raise RunningJobsQuotaExceededError.create_for_user(cluster_user.user_name)

    async def _raise_for_orgs_running_jobs_quota(self, org_cluster: OrgCluster) -> None:
        if org_cluster.quota.total_running_jobs is None:
            return
        running_filter = JobFilter(
            orgs={org_cluster.org_name},
            clusters={org_cluster.cluster_name: {}},
            statuses={JobStatus(value) for value in JobStatus.active_values()},
        )
        running_count = len(await self._jobs_storage.get_all_jobs(running_filter))
        if running_count >= org_cluster.quota.total_running_jobs:
            raise RunningJobsQuotaExceededError.create_for_org(org_cluster.org_name)

    async def _raise_for_no_credits(self, org_entry: OrgUser | Org) -> None:
        if org_entry.balance.is_non_positive:
            if isinstance(org_entry, OrgUser):
                raise NoCreditsError.create_for_user(org_entry.user_name)
            raise NoCreditsError.create_for_org(org_entry.name)

    async def _make_pass_config_token(
        self, username: str, cluster_name: str, job_id: str
    ) -> str:
        token_uri = f"token://{cluster_name}/job/{job_id}"
        await self._auth_client.grant_user_permissions(
            username, [Permission(uri=token_uri, action="read")]
        )
        return await self._auth_client.get_user_token(username, new_token_uri=token_uri)

    async def _setup_pass_config(
        self,
        user: AuthUser,
        cluster_name: str,
        org_name: str | None,
        job_request: JobRequest,
        project_name: str | None = None,
    ) -> JobRequest:
        if NEURO_PASSED_CONFIG in job_request.container.env:
            raise JobsServiceException(
                f"Cannot pass config: ENV '{NEURO_PASSED_CONFIG}' already specified"
            )
        token = await self._make_pass_config_token(
            user.name, cluster_name, job_request.job_id
        )
        pass_config_data = base64.b64encode(
            json.dumps(
                {
                    "token": token,
                    "cluster": cluster_name,
                    "org_name": org_name,
                    "url": str(self._api_base_url),
                    "project_name": project_name,
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
        user: AuthUser,
        cluster_name: str,
        *,
        org_name: str,
        project_name: str,
        job_name: str | None = None,
        preset_name: str | None = None,
        tags: Sequence[str] = (),
        scheduler_enabled: bool = False,
        preemptible_node: bool = False,
        pass_config: bool = False,
        wait_for_jobs_quota: bool = False,
        privileged: bool = False,
        schedule_timeout: float | None = None,
        max_run_time_minutes: int | None = None,
        restart_policy: JobRestartPolicy = JobRestartPolicy.NEVER,
        priority: JobPriority = JobPriority.NORMAL,
        energy_schedule_name: str | None = None,
    ) -> tuple[Job, Status]:
        base_name = get_base_owner(
            user.name
        )  # SA has access to same clusters as a user

        if job_name is not None and maybe_job_id(job_name):
            raise JobsServiceException(
                "Failed to create job: job name cannot start with 'job-' prefix."
            )

        try:
            cluster_config = self._cluster_registry.get(cluster_name)
        except ClusterNotFound as cluster_err:
            # NOTE: this will result in 400 HTTP response which may not be
            # what we want to convey really
            raise JobsServiceException(
                f"Cluster '{cluster_name}' not found"
            ) from cluster_err

        # check quotas for both a user and a cluster
        cluster_user = await self._admin_client.get_cluster_user(
            user_name=base_name,
            cluster_name=cluster_name,
            org_name=org_name,
        )
        if not wait_for_jobs_quota:
            await self._raise_for_running_jobs_quota(cluster_user)

        org_cluster = await self._admin_client.get_org_cluster(cluster_name, org_name)
        if not wait_for_jobs_quota:
            await self._raise_for_orgs_running_jobs_quota(org_cluster)

        org_user = await self._admin_client.get_org_user(
            org_name=org_name,
            user_name=base_name,
        )

        try:
            await self._raise_for_no_credits(org_user)
        except NoCreditsError:
            await self._notifications_client.notify(
                JobCannotStartNoCredits(
                    user_id=user.name,
                    cluster_name=cluster_name,
                )
            )
            raise

        org = await self._admin_client.get_org(org_name)
        await self._raise_for_no_credits(org)

        if pass_config:
            job_request = await self._setup_pass_config(
                user, cluster_name, org_name, job_request, project_name
            )

        record = JobRecord.create(
            request=job_request,
            owner=user.name,
            cluster_name=cluster_name,
            org_name=org_name,
            project_name=project_name,
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
            priority=priority,
            energy_schedule_name=energy_schedule_name,
        )
        job_id = job_request.job_id

        try:
            if (
                record.privileged
                and not cluster_config.orchestrator.allow_privileged_mode
            ):
                raise JobsServiceException(
                    f"Cluster {cluster_name} does not allow privileged jobs"
                )

            if (
                record.priority != JobPriority.NORMAL
                and not cluster_config.orchestrator.allow_job_priority
            ):
                raise JobsServiceException(
                    f"Cluster {cluster_name} does not allow specifying job priority"
                )

            async with self._create_job_in_storage(record) as record:
                job = self._make_job(record, cluster_config)
                job.init_job_internal_hostnames()
            return job, Status.create(job.status)
        except JobsStorageException as transaction_err:
            logger.error("Failed to create job %s: %s", job_id, transaction_err)
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
        max_run_time_minutes: int | None = None,
        additional_max_run_time_minutes: int | None = None,
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

    async def cancel_job(self, job_id: str, reason: str | None = None) -> None:
        for _ in range(self._max_deletion_attempts):
            try:
                async with self._update_job_in_storage(job_id) as record:
                    if record.is_finished:
                        # the job has already finished. nothing to do here.
                        return

                    logger.info("Canceling job %s for reason %s", job_id, reason)
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
        job_filter: JobFilter | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
    ) -> AsyncIterator[Job]:
        async with self._jobs_storage.iter_all_jobs(
            job_filter, reverse=reverse, limit=limit
        ) as it:
            async for record in it:
                yield await self._get_cluster_job(record)

    async def get_all_jobs(
        self, job_filter: JobFilter | None = None, *, reverse: bool = False
    ) -> list[Job]:
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
        self, job_ids: Iterable[str], job_filter: JobFilter | None = None
    ) -> list[Job]:
        records = await self._jobs_storage.get_jobs_by_ids(
            job_ids, job_filter=job_filter
        )
        return [await self._get_cluster_job(record) for record in records]

    async def get_user_config(self, user: AuthUser) -> UserConfig:
        response = await self._get_admin_user(user)
        clusters = await self._get_user_cluster_configs(response)
        return UserConfig(
            orgs=response.orgs,
            clusters=clusters,
            projects=response.projects,
        )

    async def get_user_cluster_configs(self, user: AuthUser) -> list[UserClusterConfig]:
        response = await self._get_admin_user(user)
        return await self._get_user_cluster_configs(response)

    async def _get_admin_user(self, user: AuthUser) -> GetUserResponse:
        try:
            return await self._admin_client.get_user(
                get_base_owner(user.name),
                include_orgs=True,
                include_clusters=True,
                include_projects=True,
            )
        except ClientResponseError as e:
            if e.status != 404:
                raise
            return GetUserResponse(user=AdminUser(name=user.name, email=""))

    async def _get_user_cluster_configs(
        self, response: GetUserResponse
    ) -> list[UserClusterConfig]:
        configs = []
        cluster_configs = await self._get_user_cluster_configs_by_name(response)
        cluster_to_orgs = defaultdict(list)
        for user_cluster in response.clusters:
            cluster_to_orgs[user_cluster.cluster_name].append(user_cluster.org_name)
        for cluster_name, orgs in cluster_to_orgs.items():
            if cluster_config := cluster_configs.get(cluster_name):
                configs.append(UserClusterConfig(config=cluster_config, orgs=orgs))
        return configs

    async def _get_user_cluster_configs_by_name(
        self, response: GetUserResponse
    ) -> dict[str, ClusterConfig]:
        cluster_configs = self._get_cluster_configs_by_name(response)
        volumes_by_cluster = await self._get_user_storage_volumes_by_cluster_name(
            response.user.name, list(cluster_configs.values())
        )
        for cluster_name, volumes in volumes_by_cluster.items():
            cluster_configs[cluster_name] = cluster_configs[
                cluster_name
            ].with_storage_volumes(volumes)
        return cluster_configs

    def _get_cluster_configs_by_name(
        self, response: GetUserResponse
    ) -> dict[str, ClusterConfig]:
        cluster_configs: dict[str, ClusterConfig] = {}
        for cluster in response.clusters:
            try:
                cluster_configs[cluster.cluster_name] = self._cluster_registry.get(
                    cluster.cluster_name
                )
            except ClusterNotFound:
                pass
        return cluster_configs

    async def _get_user_storage_volumes_by_cluster_name(
        self, user_name: str, cluster_configs: Sequence[ClusterConfig]
    ) -> dict[str, list[VolumeConfig]]:
        missing_storage_uris = await self._get_user_storage_volume_missing_storage_uris(
            user_name, cluster_configs
        )
        volumes_by_cluster: dict[str, list[VolumeConfig]] = defaultdict(list)
        for cluster_config in cluster_configs:
            for volume in cluster_config.storage.volumes:
                if not volume.path or (
                    volume.path
                    and f"storage://{cluster_config.name}{volume.path}"
                    not in missing_storage_uris
                ):
                    volumes_by_cluster[cluster_config.name].append(volume)
        return volumes_by_cluster

    async def _get_user_storage_volume_missing_storage_uris(
        self, user_name: str, cluster_configs: Sequence[ClusterConfig]
    ) -> set[str]:
        permissions = []
        for cluster_config in cluster_configs:
            for volume in cluster_config.storage.volumes:
                if volume.path:
                    permissions.append(
                        Permission(
                            f"storage://{cluster_config.name}{volume.path}", "read"
                        )
                    )
        if not permissions:
            return set()
        missing_permissions = await self._auth_client.get_missing_permissions(
            user_name, permissions
        )
        return {p.uri for p in missing_permissions}

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

    async def get_job_ids_for_drop(
        self, *, delay: timedelta, limit: int | None = None
    ) -> list[str]:
        return [
            record.id
            for record in await self._jobs_storage.get_jobs_for_drop(
                delay=delay, limit=limit
            )
        ]

    async def drop_job(
        self,
        job_id: str,
    ) -> None:
        async with self._jobs_storage.try_update_job(job_id) as record:
            record.being_dropped = True

    async def drop_progress(
        self, job_id: str, *, logs_removed: bool | None = None
    ) -> None:
        async with self._jobs_storage.try_update_job(job_id) as record:
            if not record.being_dropped:
                raise JobError(f"Job {job_id} is not being dropped")
            if logs_removed:
                record.logs_removed = logs_removed
        all_resources_cleaned = record.logs_removed
        if all_resources_cleaned:
            await self._jobs_storage.drop_job(job_id)
