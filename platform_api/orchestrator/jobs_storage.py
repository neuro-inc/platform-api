import json
import itertools
from abc import ABC, abstractmethod
from typing import Dict, List

import aioredis

from .base import Orchestrator
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
    async def get_all_jobs(self) -> List[Job]:
        pass

    async def get_running_jobs(self) -> List[Job]:
        jobs = []
        for job in await self.get_all_jobs():
            if not job.is_finished:
                jobs.append(job)
        return jobs

    async def get_jobs_for_deletion(self) -> List[Job]:
        jobs = []
        for job in await self.get_all_jobs():
            if job.should_be_deleted:
                jobs.append(job)
        return jobs


class InMemoryJobsStorage(JobsStorage):
    def __init__(self, orchestrator: Orchestrator) -> None:
        self._orchestrator = orchestrator

        self._job_records: Dict[str, str] = {}

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator.config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = self._job_records.get(job_id)
        if payload is None:
            raise JobError(f'no such job {job_id}')
        return self._parse_job_payload(payload)

    async def get_all_jobs(self) -> List[Job]:
        jobs = []
        for payload in self._job_records.values():
            jobs.append(self._parse_job_payload(payload))
        return jobs


class RedisJobsStorage(JobsStorage):
    def __init__(
            self, client: aioredis.Redis, orchestrator: Orchestrator) -> None:
        self._client = client
        self._orchestrator = orchestrator

    def _generate_job_key(self, job_id: str) -> str:
        return f'jobs:{job_id}'

    def _generate_jobs_status_index_key(self, status: JobStatus) -> str:
        return f'jobs.status.{status}'

    def _generate_jobs_deleted_index_key(self) -> str:
        return 'jobs.deleted'

    def _generate_jobs_index_key(self) -> str:
        return 'jobs'

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)
        tr.sadd('jobs', job.id)
        for status in JobStatus:
            tr.srem(self._generate_jobs_status_index_key(status), job.id)
        tr.sadd(self._generate_jobs_status_index_key(job.status), job.id)
        if job.is_deleted:
            tr.sadd(self._generate_jobs_deleted_index_key(), job.id)
        await tr.execute()

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator.config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = await self._client.get(self._generate_job_key(job_id))
        if payload is None:
            raise JobError(f'no such job {job_id}')
        return self._parse_job_payload(payload)

    async def _get_jobs(self, ids: List[str]) -> List[Job]:
        jobs: List[Job] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        for payload in await self._client.mget(*keys):
            jobs.append(self._parse_job_payload(payload))
        return jobs

    async def _get_all_job_ids(self) -> List[str]:
        job_ids = []
        async for job_id in self._client.isscan(
                self._generate_jobs_index_key()):
            job_ids.append(job_id.decode())
        return job_ids

    async def _get_running_job_ids(self) -> List[str]:
        job_ids = []
        async for job_id in self._client.isscan(
                self._generate_jobs_status_index_key(JobStatus.RUNNING)):
            job_ids.append(job_id.decode())
        return job_ids

    async def _get_job_ids_for_deletion(self) -> List[str]:
        tr = self._client.multi_exec()
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.FAILED),
            self._generate_jobs_deleted_index_key())
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.SUCCEEDED),
            self._generate_jobs_deleted_index_key())
        failed, succeeded = await tr.execute()
        return [id_.decode() for id_ in itertools.chain(failed, succeeded)]

    async def get_all_jobs(self) -> List[Job]:
        job_ids = await self._get_all_job_ids()
        return await self._get_jobs(job_ids)

    async def get_running_jobs(self) -> List[Job]:
        job_ids = await self._get_running_job_ids()
        return await self._get_jobs(job_ids)

    async def get_jobs_for_deletion(self) -> List[Job]:
        job_ids = await self._get_job_ids_for_deletion()
        return await self._get_jobs(job_ids)
