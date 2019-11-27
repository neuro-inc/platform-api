import abc
import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

import aiohttp
from multidict import MultiDict
from notifications_client import QuotaResourceType, QuotaWillBeReachedSoon
from notifications_client.client import Client

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import ZERO_RUN_TIME, AggregatedRunTime
from platform_api.orchestrator.job_request import JobStatus


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobInfo:
    id: str
    status: JobStatus
    owner: str
    is_gpu: bool
    cluster_name: str


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

    async def kill_job(self, job_id: str) -> None:
        url = f"{self._platform_api_url}/jobs/{job_id}"
        async with self._session.delete(url) as resp:
            resp.raise_for_status()


def _parse_job_info(value: Dict[str, Any]) -> JobInfo:
    return JobInfo(
        id=value["id"],
        status=JobStatus(value["status"]),
        owner=value["owner"],
        is_gpu=bool(value["container"]["resources"].get("gpu")),
        cluster_name=value["cluster_name"],
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
        total_gpu_run_time_delta=_quota_minutes_to_timedelta(gpu_runtime),
        total_non_gpu_run_time_delta=_quota_minutes_to_timedelta(cpu_runtime),
    )


def _quota_minutes_to_timedelta(minutes: Optional[int]) -> timedelta:
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


class JobPolicyEnforcer:
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaEnforcer(JobPolicyEnforcer):
    def __init__(
        self, platform_api_client: PlatformApiClient, notifications_client: Client
    ):
        self._platform_api_client = platform_api_client
        self._notifications_client = notifications_client

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
            if cluster_stats.is_non_gpu_quota_exceeded:
                logger.info(
                    f"User '{user_name}' exceeded non-GPU quota "
                    f"on cluster '{cluster_name}'"
                )
                jobs_to_delete.extend(cluster_jobs.non_gpu_ids)
            if cluster_stats.is_gpu_quota_exceeded:
                logger.info(
                    f"User '{user_name}' exceeded GPU quota "
                    f"on cluster '{cluster_name}'"
                )
                jobs_to_delete.extend(cluster_jobs.gpu_ids)
        for job_id in jobs_to_delete:
            try:
                await self._platform_api_client.kill_job(job_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to kill job %s", job_id)


class JobPolicyEnforcePoller:
    def __init__(
        self, config: JobPolicyEnforcerConfig, enforcers: List[JobPolicyEnforcer],
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
