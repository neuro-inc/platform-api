import abc
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


class AbstractPlatformApiHelper:
    @abc.abstractmethod
    async def get_users_and_active_job_ids(self) -> Dict[Any, Any]:
        pass

    @abc.abstractmethod
    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        pass

    @abc.abstractmethod
    async def kill_job(self, job_id: str) -> None:
        pass

    @classmethod
    def convert_response_to_runtime(
        cls, json: Dict[str, Optional[int]]
    ) -> AggregatedRunTime:
        return AggregatedRunTime(
            total_gpu_run_time_delta=_minutes_to_timedelta(
                json.get("total_gpu_run_time_minutes")
            ),
            total_non_gpu_run_time_delta=_minutes_to_timedelta(
                json.get("total_non_gpu_run_time_minutes")
            ),
        )


class PlatformApiHelper(AbstractPlatformApiHelper):
    def __init__(self, config: JobPolicyEnforcerConfig):
        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._session = aiohttp.ClientSession(headers=self._headers)

    async def get_users_and_active_job_ids(self) -> Dict[str, Any]:
        async with self._session.get(
            f"{self._platform_api_url}/jobs?status=pending&status=running"
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        async with self._session.get(
            self._platform_api_url / f"stats/user/{username}"
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def kill_job(self, job_id: str) -> None:
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
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaEnforcer(JobPolicyEnforcer):
    def __init__(self, platform_api_helper: AbstractPlatformApiHelper):
        self._platform_api_helper = platform_api_helper

    async def enforce(self) -> None:
        users_with_active_jobs = await self.get_active_users_and_jobs()
        for jobs_by_user in users_with_active_jobs:
            await self.check_user_quota(jobs_by_user)

    async def get_active_users_and_jobs(self) -> List[JobsByUser]:
        response_payload = (
            await self._platform_api_helper.get_users_and_active_job_ids()
        )
        jobs = response_payload["jobs"]
        jobs_by_owner: Dict[str, JobsByUser] = {}
        for job in jobs:
            job_status = JobStatus(job["status"])
            if job_status.is_running or job_status.is_pending:
                owner = job.get("owner")
                if owner:
                    existing_jobs = jobs_by_owner.get(owner, JobsByUser(username=owner))
                    is_gpu = job["container"]["resources"].get("gpu") is not None
                    if is_gpu:
                        existing_jobs.gpu_job_ids.add(job["id"])
                    else:
                        existing_jobs.cpu_job_ids.add(job["id"])
                    jobs_by_owner[owner] = existing_jobs

        return list(jobs_by_owner.values())

    async def check_user_quota(self, jobs_by_user: JobsByUser) -> None:
        username = jobs_by_user.username
        response_payload = await self._platform_api_helper.get_user_stats(username)
        quota = AbstractPlatformApiHelper.convert_response_to_runtime(
            response_payload["quota"]
        )
        jobs = AbstractPlatformApiHelper.convert_response_to_runtime(
            response_payload["jobs"]
        )

        jobs_to_delete: Set[str] = set()
        if quota.total_non_gpu_run_time_delta < jobs.total_non_gpu_run_time_delta:
            logger.info(f"CPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.all_job_ids
        elif quota.total_gpu_run_time_delta < jobs.total_gpu_run_time_delta:
            logger.info(f"GPU quota exceeded for {username}")
            jobs_to_delete = jobs_by_user.gpu_job_ids
        else:
            logger.debug(f"No quota issues for {username}")

        if len(jobs_to_delete) > 0:
            for job_id in jobs_to_delete:
                await self._platform_api_helper.kill_job(job_id)


class AggregatedEnforcer(JobPolicyEnforcer):
    def __init__(self, enforcers: List[JobPolicyEnforcer]):
        self._enforcers = enforcers

    async def enforce(self) -> None:
        for enforcer in self._enforcers:
            try:
                await enforcer.enforce()
            except Exception:
                logger.exception("Failed to run enforcer")
