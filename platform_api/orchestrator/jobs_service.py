from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from typing import List, Tuple, Dict
import logging

from .job import Job
from .job_request import JobRequest, JobError, JobStatus
from .base import Orchestrator
from .status import Status

logger = logging.getLogger(__file__)


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


class InMemoryJobsStorage(JobsStorage):
    def __init__(self, orchestrator: Orchestrator) -> None:
        self._orchestrator = orchestrator

        self._job_records: Dict[str, str] = {}

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator, job_record)

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


class JobsService:
    def __init__(self, orchestrator: Orchestrator) -> None:
        self._jobs_storage = InMemoryJobsStorage(orchestrator=orchestrator)
        self._orchestrator = orchestrator

    async def update_jobs_statuses(self):
        for job in await self._jobs_storage.get_running_jobs():
            await self._update_job_status(job)

    async def _update_job_status(self, job: Job) -> None:
        assert not job.is_finished
        await self._orchestrator.update_job_status(job)
        await self._jobs_storage.set_job(job)

    async def create_job(self, job_request: JobRequest) -> Tuple[Job, Status]:
        job = Job(
            orchestrator_config=self._orchestrator.config,
            job_request=job_request)
        await self._orchestrator.start_job(job)
        status = Status.create(job.status)
        await self._jobs_storage.set_job(job=job)
        return job, status

    async def get_job_status(self, job_id: str) -> JobStatus:
        job = await self._jobs_storage.get_job(job_id)
        return job.status

    async def get_job(self, job_id: str) -> Job:
        return await self._jobs_storage.get_job(job_id)

    async def delete_job(self, job_id: str) -> None:
        job = await self._jobs_storage.get_job(job_id)
        if not job.is_finished:
            await self._orchestrator.delete_job(job.id)

    async def get_all_jobs(self) -> List[Job]:
        return await self._jobs_storage.get_all_jobs()
