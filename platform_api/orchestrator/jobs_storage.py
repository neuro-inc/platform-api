import itertools
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import aioredis

from .base import OrchestratorConfig
from .job import Job
from .job_request import JobError, JobStatus


@dataclass(frozen=True)
class JobFilter:
    statuses: Set[JobStatus] = field(default_factory=set)

    def apply(self, job: Job) -> bool:
        if self.statuses and job.status not in self.statuses:
            return False
        return True

    def with_status(self, status_str: str) -> "JobFilter":
        return JobFilter(statuses=self.parse_status_line(status_str))

    @classmethod
    def from_primitive(cls, value: Dict[str, str]) -> "JobFilter":
        return cls(statuses=cls.parse_status_line(value.get("status")))

    @classmethod
    def parse_status_line(cls, status_str: Optional[str]) -> Optional[Set[JobStatus]]:
        if status_str is not None:
            return {JobStatus.parse(status) for status in status_str.split("+")}


JOB_FILTER_RUNNING = JobFilter(statuses={JobStatus.RUNNING})
JOB_FILTER_FINISHED = JobFilter(statuses={JobStatus.SUCCEEDED, JobStatus.FAILED})
JOB_FILTER_UNFINISHED = JobFilter(statuses={JobStatus.PENDING, JobStatus.RUNNING})


class JobsStorage(ABC):
    @abstractmethod
    async def set_job(self, job: Job) -> None:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Job:
        pass

    @abstractmethod
    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        pass

    async def get_running_jobs(self) -> List[Job]:
        return await self.get_all_jobs(JOB_FILTER_RUNNING)

    async def get_jobs_for_deletion(self) -> List[Job]:
        return [
            job
            for job in await self.get_all_jobs(JOB_FILTER_FINISHED)
            if job.is_time_for_deletion
        ]

    async def get_unfinished_jobs(self) -> List[Job]:
        return await self.get_all_jobs(JOB_FILTER_UNFINISHED)


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

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        jobs = []
        for payload in self._job_records.values():
            job = self._parse_job_payload(payload)
            if job_filter and not job_filter.apply(job) or job.is_deleted:
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

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        job_ids = await self._get_all_job_ids(job_filter)
        return await self._get_all_jobs_payloads(job_ids)

    async def _get_all_job_ids(
        self, job_filter: Optional[JobFilter] = None
    ) -> List[str]:
        result = []
        if job_filter is None or len(job_filter.statuses) == 0:
            async for job_id in self._client.isscan(self._generate_jobs_index_key()):
                result.append(job_id.decode())
            return result
        else:
            tr = self._client.multi_exec()
            for status in job_filter.statuses:
                tr.sdiff(
                    self._generate_jobs_status_index_key(status),
                    self._generate_jobs_deleted_index_key(),
                )
            res = await tr.execute()
            for id_ in itertools.chain(*res):
                result.append(id_.decode())
        return result

    async def _get_all_jobs_payloads(self, ids: List[str]) -> List[Job]:
        jobs: List[Job] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        for payload in await self._client.mget(*keys):
            jobs.append(self._parse_job_payload(payload))
        return jobs
