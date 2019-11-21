import abc
import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp
from multidict import MultiDict

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.orchestrator.job_request import JobStatus


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobInfo:
    id: str
    status: JobStatus
    owner: str
    is_gpu: bool


@dataclass(frozen=True)
class UserQuotaInfo:
    quota: AggregatedRunTime
    jobs: AggregatedRunTime


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

    async def get_user_stats(self, username: str) -> UserQuotaInfo:
        url = f"{self._platform_api_url}/stats/users/{username}"
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        quota = _parse_quota_runtime(payload["quota"])
        jobs = _parse_jobs_runtime(payload["jobs"])
        return UserQuotaInfo(quota=quota, jobs=jobs)

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
class JobsByUser:
    username: str
    cpu_job_ids: Set[str] = field(default_factory=set)
    gpu_job_ids: Set[str] = field(default_factory=set)

    @property
    def all_job_ids(self) -> Set[str]:
        return self.cpu_job_ids | self.gpu_job_ids


class JobPolicyEnforcer:
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaEnforcer(JobPolicyEnforcer):
    def __init__(self, platform_api_client: PlatformApiClient):
        self._platform_api_client = platform_api_client

    async def enforce(self) -> None:
        users_with_active_jobs = await self._get_active_users_and_jobs()
        for jobs_by_user in users_with_active_jobs:
            await self._enforce_user_quota(jobs_by_user)

    async def _get_active_users_and_jobs(self) -> List[JobsByUser]:
        active_jobs = await self._platform_api_client.get_non_terminated_jobs()
        jobs_by_owner: Dict[str, JobsByUser] = {}
        for job_info in active_jobs:
            owner = job_info.owner
            existing_jobs = jobs_by_owner.get(owner) or JobsByUser(username=owner)
            if job_info.is_gpu:
                existing_jobs.gpu_job_ids.add(job_info.id)
            else:
                existing_jobs.cpu_job_ids.add(job_info.id)
            jobs_by_owner[owner] = existing_jobs

        return list(jobs_by_owner.values())

    async def _enforce_user_quota(self, jobs_by_user: JobsByUser) -> None:
        username = jobs_by_user.username
        user_quota_info = await self._platform_api_client.get_user_stats(username)
        quota = user_quota_info.quota
        jobs = user_quota_info.jobs

        jobs_to_delete: Set[str] = set()
        if quota.total_non_gpu_run_time_delta <= jobs.total_non_gpu_run_time_delta:
            logger.info(f"CPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.all_job_ids
        elif quota.total_gpu_run_time_delta <= jobs.total_gpu_run_time_delta:
            logger.info(f"GPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.gpu_job_ids

        for job_id in jobs_to_delete:
            await self._platform_api_client.kill_job(job_id)


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
