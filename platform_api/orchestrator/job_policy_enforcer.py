import abc
import asyncio
import contextlib
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from neuro_auth_client import AuthClient
from neuro_logging import new_trace, trace
from notifications_client import Client as NotificationsClient, CreditsWillRunOutSoon

from platform_api.cluster import ClusterConfigRegistry
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.billing_log.service import BillingLogService
from platform_api.orchestrator.billing_log.storage import BillingLogEntry
from platform_api.orchestrator.job import Job, JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.orchestrator.poller_service import _revoke_pass_config
from platform_api.user import get_cluster
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
        auth_client: AuthClient,
        notifications_client: NotificationsClient,
        notification_threshold: Decimal,
    ):
        self._jobs_service = jobs_service
        self._auth_client = auth_client
        self._notifications_client = notifications_client
        self._threshold = notification_threshold
        self._sent: Dict[Tuple[str, str], Optional[Decimal]] = defaultdict(lambda: None)

    async def _notify_user_if_needed(
        self, username: str, cluster_name: str, credits: Optional[Decimal]
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
        await self._notifications_client.notify(
            CreditsWillRunOutSoon(
                user_id=username, cluster_name=cluster_name, credits=credits
            )
        )
        self._sent[notification_key] = credits

    @trace
    async def enforce(self) -> None:
        user_to_clusters: Dict[str, Set[str]] = defaultdict(set)
        job_filter = JobFilter(
            statuses={JobStatus(item) for item in JobStatus.active_values()}
        )
        async with self._jobs_service.iter_all_jobs(job_filter) as running_jobs:
            async for job in running_jobs:
                user_to_clusters[job.owner].add(job.cluster_name)
        await run_and_log_exceptions(
            self._enforce_for_user(username, clusters)
            for username, clusters in user_to_clusters.items()
        )

    async def _enforce_for_user(self, username: str, clusters: Set[str]) -> None:
        user = await self._auth_client.get_user(username)
        for cluster_name in clusters:
            cluster = get_cluster(user, cluster_name)
            if cluster:
                await self._notify_user_if_needed(
                    username=user.name,
                    cluster_name=cluster.name,
                    credits=cluster.quota.credits,
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
    def __init__(self, service: JobsService, auth_client: AuthClient):
        self._service = service
        self._auth_client = auth_client

    _T = TypeVar("_T")
    _K = TypeVar("_K")

    def _groupby(
        self, it: Iterable[_T], key: Callable[[_T], _K]
    ) -> Mapping[_K, List[_T]]:
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
        coros = [
            self._enforce_for_user(owner, user_jobs)
            for owner, user_jobs in owner_to_jobs.items()
        ]
        await run_and_log_exceptions(coros)

    async def _enforce_for_user(self, username: str, user_jobs: Iterable[Job]) -> None:
        user = await self._auth_client.get_user(username)
        for cluster_name, cluster_jobs in self._groupby(
            user_jobs, lambda job: job.cluster_name
        ).items():
            try:
                cluster = next(
                    cluster for cluster in user.clusters if cluster.name == cluster_name
                )
            except StopIteration:
                logger.warning(
                    f"User {username} has jobs in cluster {cluster_name}, "
                    f"but has no access to this cluster. Jobs will be cancelled"
                )
                cluster = None
            if cluster is None or (
                cluster.quota.credits is not None and cluster.quota.credits <= 0
            ):
                for job in cluster_jobs:
                    await self._service.cancel_job(
                        job.id, JobStatusReason.QUOTA_EXHAUSTED
                    )


class BillingEnforcer(JobPolicyEnforcer):
    def __init__(
        self,
        jobs_service: JobsService,
        billing_service: BillingLogService,
        proceed_wait_timeout_s: float = 15,
    ):
        self._jobs_service = jobs_service
        self._billing_service = billing_service
        self._proceed_wait_timeout_s = proceed_wait_timeout_s

    @trace
    async def enforce(self) -> None:
        async with self._jobs_service.get_not_billed_jobs() as it:
            not_billed_jobs = [job.id async for job in it]
        for job_id in not_billed_jobs:
            await self._bill_single(job_id)

    async def _bill_single(self, job_id: str) -> None:
        async with self._billing_service.entries_inserter() as inserter:
            last_id = await self._billing_service.get_last_entry_id(job_id)
            await asyncio.wait_for(
                self._billing_service.wait_until_processed(last_entry_id=last_id),
                timeout=self._proceed_wait_timeout_s,
            )

            job = await self._jobs_service.get_job(job_id)
            now = datetime.now(timezone.utc)
            new_runtime = job.get_run_time(only_after=job.last_billed, now=now)
            microseconds = int(new_runtime.total_seconds() * 1e6)
            hours = Decimal(microseconds) / int(1e6) / 3600
            charge = hours * job.price_credits_per_hour
            await inserter.insert(
                [
                    BillingLogEntry(
                        idempotency_key=str(uuid.uuid4()),
                        job_id=job.id,
                        last_billed=now,
                        charge=charge,
                        fully_billed=job.status.is_finished,
                    )
                ]
            )


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
        self, config: JobPolicyEnforcerConfig, enforcers: List[JobPolicyEnforcer]
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
