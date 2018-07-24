import json
from abc import ABC, abstractmethod
from typing import Dict, List

from .base import Orchestrator
from .job import Job
from .job_request import JobError


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
