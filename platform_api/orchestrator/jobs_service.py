from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Dict
import logging

from .job import Job
from .job_request import JobRequest, JobError, JobStatus
from .base import Orchestrator
from .status import Status

logger = logging.getLogger(__file__)


class JobsService(ABC):
    @abstractmethod
    async def create_job(self, job_request: JobRequest) -> Job:
        pass

    @abstractmethod
    async def get_job_status(self, job_id: str) -> JobStatus:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Job:
        pass

    @abstractmethod
    async def set_job(self, job: Job):
        pass

    @abstractmethod
    async def delete_job(self, job_id: str):
        pass

    @abstractmethod
    async def get_all_jobs(self):
        pass

    @abstractmethod
    async def update_jobs_status(self):
        pass


@dataclass
class JobRecord:
    job: Job
    status: Status

    @property
    def id(self):
        return self.job.id

    @property
    def job_status(self):
        return self.status.value


class InMemoryJobsService(JobsService):
    _job_records: Dict[str, JobRecord]

    def __init__(self, orchestrator: Orchestrator):
        self._job_records = {}
        self._status_id_to_jobs = {}
        self._orchestrator = orchestrator

    async def update_jobs_status(self):
        for job_record in self._job_records.values():
            if not job_record.status.value.is_finished:
                job_status = await job_record.job.status()
                job_record.status.set(job_status)

    async def create_job(self, job_request: JobRequest) -> Tuple[Job, Status]:
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        job_status = await job.start()
        status = Status.create(job_status)
        job_record = JobRecord(job=job, status=status)
        await self.set_job(job_record)
        return job, status

    async def get_job_status(self, job_id: str) -> JobStatus:
        job_record = await self.get_job(job_id)
        return job_record.job_status

    async def set_job(self, job_record: JobRecord):
        self._job_records[job_record.id] = job_record

    async def get_job(self, job_id: str) -> JobRecord:
        job_record = self._job_records.get(job_id)
        if job_record is None:
            raise JobError(f"not such job_id {job_id}")
        return job_record

    async def delete_job(self, job_id: str):
        job_records = await self.get_job(job_id)
        status = await job_records.job.delete()
        if status != JobStatus.SUCCEEDED:
            raise JobError(f'can not delete_job job with job_id {job_id}')
        return status

    async def get_all_jobs(self) -> List[dict]:
        jobs_result = []
        for job_record in self._job_records.values():
            jobs_result.append(({'job_id': job_record.id, 'status': job_record.status}))
        return jobs_result
