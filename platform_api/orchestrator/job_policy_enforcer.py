import abc
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.orchestrator.job_request import JobStatus


logger = logging.getLogger(__name__)


def _minutes_to_timedelta(minutes: Optional[int]) -> timedelta:
    if minutes is None:
        return timedelta.max
    else:
        return timedelta(minutes=minutes)


@dataclass(frozen=True)
class JobInfo:
    id: str
    status: JobStatus
    owner: str
    is_gpu: bool

    @classmethod
    def from_json(cls, payload: Dict[str, Any]) -> "JobInfo":
        is_gpu = bool(payload["container"]["resources"].get("gpu"))
        return cls(
            payload["id"], JobStatus(payload["status"]), payload["owner"], is_gpu
        )


@dataclass(frozen=True)
class UserQuotaInfo:
    quota: AggregatedRunTime
    jobs: AggregatedRunTime


class AbstractPlatformApiClient:
    @abc.abstractmethod
    async def __aenter__(self) -> "AbstractPlatformApiClient":
        return self

    @abc.abstractmethod
    async def __aexit__(self, *args: Any) -> None:
        pass

    # TODO(artem) when moved to a separate microservice, add `ping()`

    @abc.abstractmethod
    async def get_non_terminated_jobs(self) -> List[JobInfo]:
        pass

    @abc.abstractmethod
    async def get_user_stats(self, username: str) -> UserQuotaInfo:
        pass

    @abc.abstractmethod
    async def kill_job(self, job_id: str) -> None:
        pass

    @classmethod
    def convert_response_to_runtime(
        cls, payload: Dict[str, Optional[int]]
    ) -> AggregatedRunTime:
        return AggregatedRunTime(
            total_gpu_run_time_delta=_minutes_to_timedelta(
                payload.get("total_gpu_run_time_minutes")
            ),
            total_non_gpu_run_time_delta=_minutes_to_timedelta(
                payload.get("total_non_gpu_run_time_minutes")
            ),
        )


class PlatformApiClient(AbstractPlatformApiClient):
    def __init__(self, config: JobPolicyEnforcerConfig):
        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "PlatformApiClient":
        assert self._session is None, "already initialized"
        self._session = aiohttp.ClientSession(headers=self._headers)
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def get_non_terminated_jobs(self) -> List[JobInfo]:
        assert self._session is not None, "not initialized"
        async with self._session.get(
            f"{self._platform_api_url}/jobs?status=pending&status=running"
        ) as resp:
            resp.raise_for_status()
            payload = (await resp.json())["jobs"]
        return [JobInfo.from_json(job) for job in payload]

    async def get_user_stats(self, username: str) -> UserQuotaInfo:
        assert self._session is not None, "not initialized"
        async with self._session.get(
            f"{self._platform_api_url}/stats/user/{username}"
        ) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        quota = self.convert_response_to_runtime(payload["quota"])
        jobs = self.convert_response_to_runtime(payload["jobs"])
        return UserQuotaInfo(quota=quota, jobs=jobs)

    async def kill_job(self, job_id: str) -> None:
        assert self._session is not None, "not initialized"
        async with self._session.delete(
            self._platform_api_url / f"jobs/{job_id}"
        ) as resp:
            resp.raise_for_status()


@dataclass(frozen=True)
class JobsByUser:
    username: str
    cpu_job_ids: Set[str] = field(default_factory=set)
    gpu_job_ids: Set[str] = field(default_factory=set)

    @property
    def all_job_ids(self) -> Set[str]:
        return self.cpu_job_ids | self.gpu_job_ids


class JobPolicyEnforcer:
    async def __aenter__(self) -> "JobPolicyEnforcer":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaEnforcer(JobPolicyEnforcer):
    def __init__(self, platform_api_client: AbstractPlatformApiClient):
        self._platform_api_client = platform_api_client

    async def __aenter__(self) -> "QuotaEnforcer":
        await self._platform_api_client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._platform_api_client.__aexit__(*args)

    async def enforce(self) -> None:
        users_with_active_jobs = await self.get_active_users_and_jobs()
        for jobs_by_user in users_with_active_jobs:
            await self.check_user_quota(jobs_by_user)

    async def get_active_users_and_jobs(self) -> List[JobsByUser]:
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

    async def check_user_quota(self, jobs_by_user: JobsByUser) -> None:
        username = jobs_by_user.username
        user_quota_info = await self._platform_api_client.get_user_stats(username)
        quota = user_quota_info.quota
        jobs = user_quota_info.jobs

        jobs_to_delete: Set[str] = set()
        if quota.total_non_gpu_run_time_delta < jobs.total_non_gpu_run_time_delta:
            logger.info(f"CPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.all_job_ids
        elif quota.total_gpu_run_time_delta < jobs.total_gpu_run_time_delta:
            logger.info(f"GPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.gpu_job_ids

        for job_id in jobs_to_delete:
            await self._platform_api_client.kill_job(job_id)


class AggregatedEnforcer(JobPolicyEnforcer):
    def __init__(self, enforcers: List[JobPolicyEnforcer]):
        self._enforcers = enforcers

    async def __aenter__(self) -> "AggregatedEnforcer":
        for enforcer in self._enforcers:
            await enforcer.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        for enforcer in self._enforcers:
            await enforcer.__aexit__(*args)

    async def enforce(self) -> None:
        for enforcer in self._enforcers:
            try:
                await enforcer.enforce()
            except Exception:
                logger.exception("Failed to run %s", type(enforcer).__name__)


class JobPolicyEnforcePoller:
    def __init__(
        self, policy_enforcer: JobPolicyEnforcer, config: JobPolicyEnforcerConfig
    ) -> None:
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._policy_enforcer = policy_enforcer
        self._config = config

        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "JobPolicyEnforcePoller":
        logger.info("Starting job policy enforce polling")
        if self._task is not None:
            raise RuntimeError("Concurrent usage of enforce poller not allowed")
        await self._policy_enforcer.__aenter__()
        self._task = self._loop.create_task(self._run())
        return self

    async def __aexit__(self, *args: Any) -> None:
        logger.info("Finishing job policy enforce polling")
        assert self._task is not None
        await self._policy_enforcer.__aexit__(*args)
        self._task.cancel()

    async def _run(self) -> None:
        try:
            while True:
                start = self._loop.time()
                await self._run_once()
                elapsed = self._loop.time() - start
                delay = self._config.interval_sec - elapsed
                if delay < 0:
                    delay = 0
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            logger.info("Enforcer loop cancelled")

    async def _run_once(self) -> None:
        try:
            await self._policy_enforcer.enforce()
        except asyncio.CancelledError:
            raise
        except BaseException:
            logger.exception("Exception when trying to enforce jobs policies")
