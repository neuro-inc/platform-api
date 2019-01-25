import itertools
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

import aioredis

from .base import OrchestratorConfig
from .job import Job
from .job_request import JobError, JobStatus


class JobsStorage(ABC):
    @abstractmethod
    async def set_job(self, job: Job) -> None:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Job:
        pass

    @abstractmethod
    async def get_all_jobs(self, filters: Optional[Dict[str, Any]] = None) -> List[Job]:
        pass

    async def get_running_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.is_running]

    async def get_jobs_for_deletion(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.should_be_deleted]

    async def get_unfinished_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if not job.is_finished]


class InMemoryJobsStorage(JobsStorage):
    def __init__(self, orchestrator_config: OrchestratorConfig) -> None:
        self._orchestrator_config = orchestrator_config

        self._job_records: Dict[str, str] = {}

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator_config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = self._job_records.get(job_id)
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    async def get_all_jobs(self, filters: Optional[Dict[str, Any]] = None) -> List[Job]:
        jobs = []
        statuses = filters.get("status") if filters else None
        for payload in self._job_records.values():
            job = self._parse_job_payload(payload)
            if statuses and job.status not in statuses:
                continue
            jobs.append(job)
        return jobs


class RedisJobsStorage(JobsStorage):
    def __init__(
        self, client: aioredis.Redis, orchestrator_config: OrchestratorConfig
    ) -> None:
        self._client = client
        self._orchestrator_config = orchestrator_config

    def _generate_job_key(self, job_id: str) -> str:
        return f"jobs:{job_id}"

    def _generate_jobs_status_index_key(self, status: JobStatus) -> str:
        return f"jobs.status.{status}"

    def _generate_jobs_deleted_index_key(self) -> str:
        return "jobs.deleted"

    def _generate_jobs_index_key(self) -> str:
        return "jobs"

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)
        tr.sadd("jobs", job.id)
        for status in JobStatus:
            tr.srem(self._generate_jobs_status_index_key(status), job.id)
        tr.sadd(self._generate_jobs_status_index_key(job.status), job.id)
        if job.is_deleted:
            tr.sadd(self._generate_jobs_deleted_index_key(), job.id)
        await tr.execute()

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator_config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = await self._client.get(self._generate_job_key(job_id))
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    async def _get_jobs(self, ids: List[str]) -> List[Job]:
        jobs: List[Job] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        for payload in await self._client.mget(*keys):
            jobs.append(self._parse_job_payload(payload))
        return jobs

    async def _get_job_ids(self, statuses: Set[JobStatus]) -> List[str]:
        len_statuses = len(statuses)
        if len_statuses == 0:
            job_ids = []
            async for job_id in self._client.isscan(self._generate_jobs_index_key()):
                job_ids.append(job_id.decode())
            return job_ids
        elif len_statuses == 1:
            job_ids = []
            status = next(iter(statuses))
            async for job_id in self._client.isscan(
                self._generate_jobs_status_index_key(status)
            ):
                job_ids.append(job_id.decode())
            return job_ids
        else:
            job_ids = await self._client.sunion(
                *[self._generate_jobs_status_index_key(status) for status in statuses]
            )
            return [id_.decode() for id_ in job_ids]

    async def _get_job_ids_for_deletion(self) -> List[str]:
        tr = self._client.multi_exec()
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.FAILED),
            self._generate_jobs_deleted_index_key(),
        )
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.SUCCEEDED),
            self._generate_jobs_deleted_index_key(),
        )
        failed, succeeded = await tr.execute()
        return [id_.decode() for id_ in itertools.chain(failed, succeeded)]

    async def get_all_jobs(self, filters: Optional[Dict[str, Any]] = None) -> List[Job]:
        statuses: Set[JobStatus] = filters.get("status", {}) if filters else {}
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)

    async def get_running_jobs(self) -> List[Job]:
        statuses = {JobStatus.RUNNING}
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)

    async def get_unfinished_jobs(self) -> List[Job]:
        statuses = {JobStatus.PENDING, JobStatus.RUNNING}
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)

    async def get_jobs_for_deletion(self) -> List[Job]:
        job_ids = await self._get_job_ids_for_deletion()
        return await self._get_jobs(job_ids)
