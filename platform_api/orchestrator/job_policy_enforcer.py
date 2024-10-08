import abc
import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from decimal import Decimal
from typing import Any, Optional, TypeVar

from aiohttp import ClientResponseError
from neuro_admin_client import AdminClient, ClusterUser, OrgCluster
from neuro_auth_client import AuthClient
from neuro_logging import new_trace, trace
from neuro_notifications_client import (
    Client as NotificationsClient,
    CreditsWillRunOutSoon,
)

from platform_api.cluster import ClusterConfigRegistry
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import Job, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.orchestrator.poller_service import _revoke_pass_config
from platform_api.utils.asyncio import run_and_log_exceptions

logger = logging.getLogger(__name__)


class JobPolicyEnforcer:
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class CreditsNotificationsEnforcer(JobPolicyEnforcer):
    def __init__(
        self,
        jobs_service: JobsService,
        admin_client: AdminClient,
        notifications_client: NotificationsClient,
        notification_threshold: Decimal,
    ):
        self._jobs_service = jobs_service
        self._admin_client = admin_client
        self._notifications_client = notifications_client
        self._threshold = notification_threshold
        self._sent: dict[tuple[str, str], Optional[Decimal]] = defaultdict(lambda: None)

    async def _notify_user_if_needed(
        self,
        username: str,
        cluster_name: str,
        org_name: Optional[str],
        credits: Optional[Decimal],
    ) -> None:
        notification_key = (username, cluster_name)
        if credits is None or credits >= self._threshold:
            return
        # Note: this check is also performed in notifications service
        # using redis storage, so it's OK to use in memory dict here:
        # this is just an optimization to avoid spamming it
        # with duplicate notifications
        if self._sent[notification_key] == credits:
            return
        # TODO patch notifications to support org_name
        await self._notifications_client.notify(
            CreditsWillRunOutSoon(
                user_id=username, cluster_name=cluster_name, credits=credits
            )
        )
        self._sent[notification_key] = credits

    @trace
    async def enforce(self) -> None:
        user_to_clusters: dict[str, set[tuple[str, Optional[str]]]] = defaultdict(set)
        job_filter = JobFilter(
            statuses={JobStatus(item) for item in JobStatus.active_values()}
        )
        async with self._jobs_service.iter_all_jobs(job_filter) as running_jobs:
            async for job in running_jobs:
                user_to_clusters[job.owner].add((job.cluster_name, job.org_name))
        await run_and_log_exceptions(
            self._enforce_for_user(username, clusters_with_org)
            for username, clusters_with_org in user_to_clusters.items()
        )

    async def _enforce_for_user(
        self, username: str, clusters_and_orgs: set[tuple[str, Optional[str]]]
    ) -> None:
        base_name = username.split("/", 1)[0]  # SA inherit balance from main user
        _, cluster_users = await self._admin_client.get_user_with_clusters(base_name)
        cluster_to_user = {
            (cluster_user.cluster_name, cluster_user.org_name): cluster_user
            for cluster_user in cluster_users
        }
        for cluster_name, org_name in clusters_and_orgs:
            cluster_user = cluster_to_user.get((cluster_name, org_name))
            if cluster_user:
                await self._notify_user_if_needed(
                    username=username,
                    cluster_name=cluster_name,
                    org_name=org_name,
                    credits=cluster_user.balance.credits,
                )


class RuntimeLimitEnforcer(JobPolicyEnforcer):
    def __init__(self, service: JobsService):
        self._service = service

    @trace
    async def enforce(self) -> None:
        active_jobs = await self._service.get_all_jobs(
            job_filter=JobFilter(
                statuses={JobStatus(item) for item in JobStatus.active_values()}
            )
        )
        await run_and_log_exceptions(
            self._enforce_job_lifetime(job) for job in active_jobs
        )

    def _is_runtime_limit_exceeded(self, job: Job) -> bool:
        if job.max_run_time_minutes is None:
            return False
        runtime_minutes = job.get_run_time().total_seconds() / 60
        return job.max_run_time_minutes < runtime_minutes

    async def _enforce_job_lifetime(self, job: Job) -> None:
        if self._is_runtime_limit_exceeded(job):
            logger.info(
                f"Job {job.id} by user '{job.owner}' exceeded its lifetime limit "
                f"on cluster '{job.cluster_name}'"
            )
            await self._service.cancel_job(job.id, JobStatusReason.LIFE_SPAN_ENDED)


class CreditsLimitEnforcer(JobPolicyEnforcer):
    def __init__(self, service: JobsService, admin_client: AdminClient):
        self._service = service
        self._admin_client = admin_client

    _T = TypeVar("_T")
    _K = TypeVar("_K")

    def _groupby(
        self, it: Iterable[_T], key: Callable[[_T], _K]
    ) -> Mapping[_K, list[_T]]:
        res = defaultdict(list)
        for item in it:
            res[key(item)].append(item)
        return res

    @trace
    async def enforce(self) -> None:
        jobs = await self._service.get_all_jobs(
            job_filter=JobFilter(
                statuses={JobStatus(item) for item in JobStatus.active_values()}
            )
        )
        owner_to_jobs = self._groupby(jobs, lambda job: job.owner)
        org_to_jobs = self._groupby(jobs, lambda job: (job.cluster_name, job.org_name))
        coros = [
            self._enforce_for_user(owner, user_jobs)
            for owner, user_jobs in owner_to_jobs.items()
        ]
        coros += [
            self._enforce_for_org(cluster_name, org_name, org_jobs)
            for (cluster_name, org_name), org_jobs in org_to_jobs.items()
            if org_name is not None
        ]
        await run_and_log_exceptions(coros)

    async def _enforce_for_user(self, username: str, user_jobs: Iterable[Job]) -> None:
        base_name = username.split("/", 1)[0]  # SA inherit balance from main user
        user, user_clusters = await self._admin_client.get_user_with_clusters(base_name)
        for (cluster_name, org_name), org_cluster_jobs in self._groupby(
            user_jobs, lambda job: (job.cluster_name, job.org_name)
        ).items():
            user_cluster: Optional[ClusterUser]
            try:
                user_cluster = next(
                    user_cluster
                    for user_cluster in user_clusters
                    if user_cluster.cluster_name == cluster_name
                    and user_cluster.org_name == org_name
                )
            except StopIteration:
                logger.warning(
                    f"User {username} has jobs in cluster {cluster_name} "
                    f"as part of org {org_name}, but has no access to this "
                    "cluster as part of this org. Jobs will be cancelled"
                )
                user_cluster = None
            if user_cluster is None or user_cluster.balance.is_non_positive:
                for job in org_cluster_jobs:
                    await self._service.cancel_job(
                        job.id, JobStatusReason.QUOTA_EXHAUSTED
                    )

    async def _enforce_for_org(
        self, cluster_name: str, org_name: str, org_cluster_jobs: Iterable[Job]
    ) -> None:
        org_cluster: Optional[OrgCluster] = None
        try:
            org_cluster = await self._admin_client.get_org_cluster(
                cluster_name, org_name
            )
        except ClientResponseError as e:
            if e.status == 404:
                logger.warning(
                    f"Org {org_name} has jobs in cluster {cluster_name} but has no "
                    f"access to this cluster as part of this org. "
                    f"Jobs will be cancelled"
                )
            else:
                raise
        if org_cluster is None or org_cluster.balance.is_non_positive:
            for job in org_cluster_jobs:
                await self._service.cancel_job(job.id, JobStatusReason.QUOTA_EXHAUSTED)


class StopOnClusterRemoveEnforcer(JobPolicyEnforcer):
    def __init__(
        self,
        jobs_service: JobsService,
        cluster_config_registry: ClusterConfigRegistry,
        auth_client: AuthClient,
    ):
        self._jobs_service = jobs_service
        self._clusters_registry = cluster_config_registry
        self._auth_client = auth_client

    @trace
    async def enforce(self) -> None:
        jobs = await self._jobs_service.get_all_jobs(
            job_filter=JobFilter(
                statuses={JobStatus(item) for item in JobStatus.active_values()}
            )
        )
        known_clusters = set(self._clusters_registry.cluster_names)
        for job in jobs:
            if job.cluster_name not in known_clusters:
                status_item = JobStatusItem.create(
                    JobStatus.FAILED,
                    reason=JobStatusReason.CLUSTER_NOT_FOUND,
                )
                await self._jobs_service.set_job_status(job.id, status_item)
                await self._jobs_service.set_job_materialized(job.id, False)
                await _revoke_pass_config(self._auth_client, job)


class RetentionPolicyEnforcer(JobPolicyEnforcer):
    def __init__(
        self,
        jobs_service: JobsService,
        retention_delay: timedelta,
    ):
        self._jobs_service = jobs_service
        self._retention_delay = retention_delay

    @trace
    async def enforce(self) -> None:
        job_ids = await self._jobs_service.get_job_ids_for_drop(
            delay=self._retention_delay, limit=100
        )
        for job_id in job_ids:
            await self._jobs_service.drop_job(job_id)


class JobPolicyEnforcePoller:
    def __init__(
        self, config: JobPolicyEnforcerConfig, enforcers: list[JobPolicyEnforcer]
    ) -> None:
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._enforcers = enforcers
        self._config = config

        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "JobPolicyEnforcePoller":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Concurrent usage of enforce poller not allowed")
        names = ", ".join(self._get_enforcer_name(e) for e in self._enforcers)
        logger.info(f"Starting job policy enforce polling with [{names}]")
        self._task = self._loop.create_task(self._run())

    async def stop(self) -> None:
        logger.info("Stopping job policy enforce polling")
        assert self._task is not None
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            start = self._loop.time()
            await self._run_once()
            elapsed = self._loop.time() - start
            delay = self._config.interval_sec - elapsed
            if delay < 0:
                delay = 0
            await asyncio.sleep(delay)

    @new_trace
    async def _run_once(self) -> None:
        for enforcer in self._enforcers:
            try:
                await enforcer.enforce()
            except asyncio.CancelledError:
                raise
            except BaseException:
                name = f"job policy enforcer {self._get_enforcer_name(enforcer)}"
                logger.exception(f"Failed to run iteration of the {name}, ignoring...")

    def _get_enforcer_name(self, enforcer: JobPolicyEnforcer) -> str:
        return type(enforcer).__name__
