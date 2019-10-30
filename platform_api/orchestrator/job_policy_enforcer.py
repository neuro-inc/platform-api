import abc
import logging
from typing import Any, Dict, Set

import aiohttp

from platform_api.config import JobPolicyEnforcerConfig
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.orchestrator.job_request import JobStatus


logger = logging.getLogger(__name__)


class JobPolicyEnforcerClientWrapper:
    @abc.abstractmethod
    async def get_users_with_active_jobs(self) -> Dict[Any, Any]:
        pass

    @abc.abstractmethod
    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        pass

    @abc.abstractmethod
    async def kill_job(self, job_id: str) -> None:
        pass


class RealJobPolicyEnforcerClientWrapper(JobPolicyEnforcerClientWrapper):
    def __init__(self, config: JobPolicyEnforcerConfig):
        self._platform_api_url = config.platform_api_url
        self._headers = {"Authorization": f"Bearer {config.token}"}
        self._session = aiohttp.ClientSession(headers=self._headers)

    async def get_users_with_active_jobs(self) -> Dict[Any, Any]:
        async with self._session.get(
            self._platform_api_url / "jobs?status=pending&status=running",
            headers=self._headers,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_user_stats(self, username: str) -> Dict[Any, Any]:
        async with self._session.get(
            self._platform_api_url / f"stats/user/{username}", headers=self._headers
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def kill_job(self, job_id: str) -> None:
        async with self._session.delete(
            self._platform_api_url / f"jobs/{job_id}", headers=self._headers
        ) as resp:
            resp.raise_for_status()


class JobPolicyEnforcer:
    @abc.abstractmethod
    async def enforce(self) -> None:
        pass


class QuotaJobPolicyEnforcer(JobPolicyEnforcer):
    def __init__(self, wrapper: RealJobPolicyEnforcerClientWrapper):
        self._wrapper = wrapper

    async def enforce(self) -> None:
        users_with_active_jobs = await self.get_users_with_active_jobs()
        for user, job_ids in users_with_active_jobs.items():
            await self.check_user_quota(user, job_ids["cpu"], job_ids["gpu"])

    async def get_users_with_active_jobs(self) -> Dict[str, Dict[str, Set[str]]]:
        response_payload = await self._wrapper.get_users_with_active_jobs()
        jobs = response_payload["jobs"]
        jobs_by_owner: Dict[str, Dict[str, Set[str]]] = {}
        for job in jobs:
            job_status = JobStatus(response_payload["status"])
            if job_status.is_running or job_status.is_pending:
                owner = job.get("owner")
                if owner:
                    existing_jobs = jobs_by_owner.get(
                        owner, {"gpu": set(), "cpu": set()}
                    )
                    is_gpu = job["container"]["resources"].get("gpu") is not None
                    if is_gpu:
                        existing_jobs["gpu"].add(job["id"])
                    else:
                        existing_jobs["cpu"].add(job["id"])
                    jobs_by_owner[owner] = existing_jobs

        return jobs_by_owner

    async def check_user_quota(
        self, username: str, cpu_job_ids: Set[str], gpu_job_ids: Set[str]
    ) -> None:
        response_payload = await self._wrapper.get_user_stats(username)
        quota = AggregatedRunTime.from_primitive(response_payload["quota"])
        jobs = AggregatedRunTime.from_primitive(response_payload["jobs"])

        assert quota is not None
        assert jobs is not None

        jobs_to_delete: Set[str] = set()
        if quota.total_non_gpu_run_time_delta < jobs.total_non_gpu_run_time_delta:
            logger.info(f"CPU quota exceeded for {username}")
            jobs_to_delete = cpu_job_ids.union(gpu_job_ids)
        elif quota.total_gpu_run_time_delta < jobs.total_gpu_run_time_delta:
            logger.info(f"GPU quota exceeded for {username}")
            jobs_to_delete = gpu_job_ids
        else:
            logger.info(f"No quota issues for {username}")

        if len(jobs_to_delete) > 0:
            logger.info(f"Killing jobs: {jobs_to_delete}")
            for job_id in jobs_to_delete:
                await self._wrapper.kill_job(job_id)
