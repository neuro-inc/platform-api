import abc
import asyncio
import contextlib
import logging
from collections import defaultdict
from dataclasses import dataclass, field
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
    Sequence,
    TypeVar,
)

import aiohttp
from multidict import MultiDict
from neuro_auth_client import AuthClient
from notifications_client import QuotaResourceType, QuotaWillBeReachedSoon
from notifications_client.client import Client

from platform_api.admin_client import AdminClient
from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import (
    ZERO_RUN_TIME,
    AggregatedRunTime,
    Job,
    JobStatusReason,
)
from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobInfo:
    id: str
    status: JobStatus
    owner: str
    is_gpu: bool
    cluster_name: str
    run_time: timedelta
    run_time_limit: timedelta

    @property
    def is_run_time_exceeded(self) -> bool:
        return self.run_time >= self.run_time_limit


@dataclass(frozen=True)
class UserClusterStats:
    name: str
    quota: AggregatedRunTime
    jobs: AggregatedRunTime

    @classmethod
    def create_dummy(cls, name: str) -> "UserClusterStats":
        return cls(name=name, quota=ZERO_RUN_TIME, jobs=ZERO_RUN_TIME)

    @property
    def is_non_gpu_quota_exceeded(self) -> bool:
        return (
            self.quota.total_non_gpu_run_time_delta
            <= self.jobs.total_non_gpu_run_time_delta
        )

    @property
    def is_gpu_quota_exceeded(self) -> bool:
        return self.quota.total_gpu_run_time_delta <= self.jobs.total_gpu_run_time_delta


@dataclass(frozen=True)
class UserStats:
    name: str
    clusters: List[UserClusterStats]

    def get_cluster(self, name: str) -> UserClusterStats:
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return UserClusterStats.create_dummy(name)


class PlatformApiClient:
    def __init__(self, config: JobPolicyEnforcerConfig):
        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._session = aiohttp.ClientSession(headers=self._headers)

    async def __aenter__(self) -> "PlatformApiClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._session.close()

    async def get_non_terminated_jobs(self) -> List[JobInfo]:
        url = f"{self._platform_api_url}/jobs"
        params = MultiDict([("status", "pending"), ("status", "running")])
        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        return [_parse_job_info(job) for job in payload["jobs"]]

    async def get_user_stats(self, username: str) -> UserStats:
        url = f"{self._platform_api_url}/stats/users/{username}"
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        return _parse_user_stats(payload)

    async def kill_job(self, job_id: str, reason: str) -> None:
        url = f"{self._platform_api_url}/jobs/{job_id}"
        async with self._session.delete(url, options={"reason": reason}) as resp:
            resp.raise_for_status()


def _parse_job_info(value: Dict[str, Any]) -> JobInfo:
    return JobInfo(
        id=value["id"],
        status=JobStatus(value["status"]),
        owner=value["owner"],
        is_gpu=bool(value["container"]["resources"].get("gpu")),
        cluster_name=value["cluster_name"],
        run_time=timedelta(seconds=float(value["history"]["run_time_seconds"])),
        run_time_limit=_time_limit_minutes_to_timedelta(
            value.get("max_run_time_minutes")
        ),
    )


def _parse_user_stats(value: Dict[str, Any]) -> UserStats:
    return UserStats(
        name=value["name"],
        clusters=[_parse_user_cluster_stats(c) for c in value["clusters"]],
    )


def _parse_user_cluster_stats(value: Dict[str, Any]) -> UserClusterStats:
    return UserClusterStats(
        name=value["name"],
        quota=_parse_quota_runtime(value["quota"]),
        jobs=_parse_jobs_runtime(value["jobs"]),
    )


def _parse_jobs_runtime(value: Dict[str, int]) -> AggregatedRunTime:
    gpu_runtime = int(value["total_gpu_run_time_minutes"])
    cpu_runtime = int(value["total_non_gpu_run_time_minutes"])
    return AggregatedRunTime(
        total_gpu_run_time_delta=timedelta(minutes=gpu_runtime),
        total_non_gpu_run_time_delta=timedelta(minutes=cpu_runtime),
    )


def _parse_quota_runtime(value: Dict[str, Optional[int]]) -> AggregatedRunTime:
    gpu_runtime = value.get("total_gpu_run_time_minutes")
    cpu_runtime = value.get("total_non_gpu_run_time_minutes")
    return AggregatedRunTime(
        total_gpu_run_time_delta=_time_limit_minutes_to_timedelta(gpu_runtime),
        total_non_gpu_run_time_delta=_time_limit_minutes_to_timedelta(cpu_runtime),
    )


def _time_limit_minutes_to_timedelta(minutes: Optional[int]) -> timedelta:
    if minutes is None:
        return timedelta.max
    return timedelta(minutes=minutes)


@dataclass(frozen=True)
class ClusterJobs:
    name: str
    non_gpu: List[JobInfo] = field(default_factory=list)
    gpu: List[JobInfo] = field(default_factory=list)

    def add(self, job: JobInfo) -> None:
        assert self.name == job.cluster_name, "Invalid job cluster name"
        if job.is_gpu:
            self.gpu.append(job)
        else:
            self.non_gpu.append(job)

    @property
    def non_gpu_ids(self) -> Iterable[str]:
        return (job.id for job in self.non_gpu)

    @property
    def gpu_ids(self) -> Iterable[str]:
        return (job.id for job in self.gpu)


@dataclass(frozen=True)
class UserJobs:
    name: str
    clusters: Dict[str, ClusterJobs] = field(default_factory=dict)

    def add(self, job: JobInfo) -> None:
        assert self.name == job.owner, "Invalid job owner"
        cluster = self.clusters.get(job.cluster_name)
        if not cluster:
            self.clusters[job.cluster_name] = cluster = ClusterJobs(
                name=job.cluster_name
            )
        cluster.add(job)


class Jobs:
    @staticmethod
    def group_by_user(jobs: Sequence[JobInfo]) -> List[UserJobs]:
        groups: Dict[str, UserJobs] = {}
        for job in jobs:
            user_jobs = groups.get(job.owner)
            if not user_jobs:
                groups[job.owner] = user_jobs = UserJobs(name=job.owner)
            user_jobs.add(job)
        return list(groups.values())


@dataclass(frozen=True)
class QuotaNotificationKey:
    username: str
    cluster_name: str
    resource_type: QuotaResourceType


class QuotaNotifier:
    def __init__(
        self, notifications_client: Client, notification_threshold: float = 0.9
    ):
        self._notifications_client = notifications_client
        self._sent_quota_will_be_reached_soon_notifications: Dict[
            QuotaNotificationKey, int
        ] = {}
        self._notifications_threshold = notification_threshold

    async def notify_for_quota(
        self, username: str, cluster_stats: UserClusterStats
    ) -> None:
        cluster_name = cluster_stats.name
        quota = cluster_stats.quota
        jobs = cluster_stats.jobs

        await self._notify_quota_will_be_reached_soon(
            username,
            QuotaResourceType.NON_GPU,
            jobs.total_non_gpu_run_time_delta,
            quota.total_non_gpu_run_time_delta,
            cluster_name,
        )
        await self._notify_quota_will_be_reached_soon(
            username,
            QuotaResourceType.GPU,
            jobs.total_gpu_run_time_delta,
            quota.total_gpu_run_time_delta,
            cluster_name,
        )

    async def _notify_quota_will_be_reached_soon(
        self,
        username: str,
        resource_type: QuotaResourceType,
        used_quota: timedelta,
        total_quota: timedelta,
        cluster_name: str,
    ) -> None:

        notification_key = QuotaNotificationKey(username, cluster_name, resource_type)
        if (
            self._need_to_send_quota_notification(
                notification_key, int(total_quota.total_seconds())
            )
            and used_quota >= self._notifications_threshold * total_quota
        ):
            notification = QuotaWillBeReachedSoon(
                username,
                resource=resource_type,
                used=used_quota.total_seconds(),
                quota=total_quota.total_seconds(),
                cluster_name=cluster_name,
            )
            await self._notifications_client.notify(notification)
            self._store_sent_quota_notification(
                notification_key, int(total_quota.total_seconds())
            )

    def _store_sent_quota_notification(
        self,
        quota_notification_key: QuotaNotificationKey,
        current_quota_value_seconds: int,
    ) -> None:
        self._sent_quota_will_be_reached_soon_notifications[
            quota_notification_key
        ] = current_quota_value_seconds

    def _need_to_send_quota_notification(
        self,
        quota_notification_key: QuotaNotificationKey,
        current_quota_value_seconds: int,
    ) -> bool:
        stored_quota_value = self._sent_quota_will_be_reached_soon_notifications.get(
            quota_notification_key
        )
        if stored_quota_value != current_quota_value_seconds:
            self._sent_quota_will_be_reached_soon_notifications.pop(
                quota_notification_key, None
            )
            return True
        else:
            return False


class JobPolicyEnforcer:
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaEnforcer(JobPolicyEnforcer):
    def __init__(
        self,
        platform_api_client: PlatformApiClient,
        notifications_client: Client,
        enforcer_config: JobPolicyEnforcerConfig,
    ):
        self._platform_api_client = platform_api_client
        self._quota_notifier = QuotaNotifier(
            notifications_client, enforcer_config.quota_notification_threshold
        )

    async def enforce(self) -> None:
        users_with_active_jobs = await self._get_active_users_and_jobs()
        for jobs_by_user in users_with_active_jobs:
            await self._enforce_user_quota(jobs_by_user)

    async def _get_active_users_and_jobs(self) -> List[UserJobs]:
        jobs = await self._platform_api_client.get_non_terminated_jobs()
        return Jobs.group_by_user(jobs)

    async def _enforce_user_quota(self, user_jobs: UserJobs) -> None:
        user_name = user_jobs.name
        user_stats = await self._platform_api_client.get_user_stats(user_name)
        jobs_to_delete: List[str] = []
        for cluster_name, cluster_jobs in user_jobs.clusters.items():
            cluster_stats = user_stats.get_cluster(cluster_name)
            jobs_to_delete_in_current_cluster: List[str] = []
            logger.debug(f"Checking {cluster_stats}")
            if cluster_stats.is_non_gpu_quota_exceeded:
                logger.info(
                    f"User '{user_name}' exceeded non-GPU quota "
                    f"on cluster '{cluster_name}'"
                )
                jobs_to_delete_in_current_cluster.extend(cluster_jobs.non_gpu_ids)
            if cluster_stats.is_gpu_quota_exceeded:
                logger.info(
                    f"User '{user_name}' exceeded GPU quota "
                    f"on cluster '{cluster_name}'"
                )
                jobs_to_delete_in_current_cluster.extend(cluster_jobs.gpu_ids)
            if not jobs_to_delete_in_current_cluster:
                # NOTE: `_enforce_user_quota` gets executed only when a user
                # has some 'pending' or 'running' jobs. Assuming that, if any
                # type of quota is exceeded, there should be some jobs to
                # delete, otherwise we wouldn't even enter the loop above.
                # This condition allows us not to send a
                # `QuotaWillBeReachedSoon` notification in case the quota has
                # been already exceeded.
                await self._quota_notifier.notify_for_quota(user_name, cluster_stats)
            jobs_to_delete.extend(jobs_to_delete_in_current_cluster)

        for job_id in jobs_to_delete:
            try:
                await self._platform_api_client.kill_job(
                    job_id, JobStatusReason.QUOTA_EXHAUSTED
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to kill job %s", job_id)


class RuntimeLimitEnforcer(JobPolicyEnforcer):
    def __init__(self, platform_api_client: PlatformApiClient):
        self._platform_api_client = platform_api_client

    async def enforce(self) -> None:
        active_jobs = await self._platform_api_client.get_non_terminated_jobs()
        for job in active_jobs:
            await self._enforce_job_lifetime(job)

    async def _enforce_job_lifetime(self, job: JobInfo) -> None:
        if job.is_run_time_exceeded:
            logger.info(
                f"Job {job.id} by user '{job.owner}' exceeded its lifetime limit "
                f"on cluster '{job.cluster_name}'"
            )
            try:
                await self._platform_api_client.kill_job(
                    job.id, JobStatusReason.LIFE_SPAN_ENDED
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to kill job %s", job.id)


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

    async def enforce(self) -> None:
        jobs = await self._service.get_all_jobs(
            job_filter=JobFilter(
                statuses={JobStatus(item) for item in JobStatus.active_values()}
            )
        )
        await asyncio.gather(
            *[
                asyncio.create_task(self._enforce_for_user(owner, user_jobs))
                for owner, user_jobs in self._groupby(
                    jobs, lambda job: job.owner
                ).items()
            ]
        )

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
                    f"but has no access to this cluster"
                )
                continue
            if cluster.quota.credits == 0:
                for job in cluster_jobs:
                    await self._service.cancel_job(
                        job.id, JobStatusReason.QUOTA_EXHAUSTED
                    )


class BillingEnforcer(JobPolicyEnforcer):
    def __init__(self, service: JobsService, admin_client: AdminClient):
        self._service = service
        self._admin_client = admin_client

    async def enforce(self) -> None:
        await asyncio.gather(
            *[
                asyncio.create_task(self._bill_single_wrapper(job))
                async for job in self._service.get_not_billed_jobs()
            ],
        )

    async def _bill_single_wrapper(self, job: Job) -> None:
        try:
            await self._bill_single(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Failed to bill job {job.id}:")

    async def _bill_single(self, job: Job) -> None:
        new_runtime = job.get_run_time(only_after=job.last_billed)
        now = datetime.now(timezone.utc)
        # ? Rounding ?
        microseconds = int(new_runtime.total_seconds() * 1e6)
        hours = Decimal(microseconds) / int(1e6) / 3600
        new_charge = hours * job.price_credits_per_hour
        await self._admin_client.change_user_credits(
            job.cluster_name, job.owner, -new_charge
        )
        await self._service.update_job_billing(
            job_id=job.id,
            last_billed=now,
            new_charge=new_charge,
            fully_billed=job.status.is_finished,
        )


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
