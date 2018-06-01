from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Dict
import logging
import uuid

from .job import Job
from .job_request import JobRequest, JobError, JobStatus
from .base import Orchestrator


logger = logging.getLogger(__file__)


class JobsService(ABC):
    @abstractmethod
    async def create_job(self, job_request: JobRequest) -> Job:
        pass

    @abstractmethod
    async def get_job_status(self, job_id: str) -> JobStatus:
        pass

    @abstractmethod
    async def get(self, job_id: str) -> Job:
        pass

    @abstractmethod
    async def set(self, job: Job):
        pass

    @abstractmethod
    async def delete(self, job_id: str):
        pass

    @abstractmethod
    async def get_all(self):
        pass


@dataclass
class Status:
    status_id: str
    _value: JobStatus

    @property
    def value(self) -> JobStatus:
        return self._value

    @classmethod
    def create(cls, value: JobStatus) -> 'Status':
        status_id = str(uuid.uuid4())
        return cls(status_id, value)  # type: ignore

    @property
    def id(self):
        return self.status_id


@dataclass
class JobRecord:
    job: Job
    status: Status

    @property
    def id(self):
        return self.job.id

    async def job_status(self):
        # TODO this one is need with background
        # return self.status.value
        return await self.job.status()


class InMemoryJobsService(JobsService):
    _job_records: Dict[str, JobRecord]

    def __init__(self, orchestrator: Orchestrator):
        self._job_records = {}
        self._status_id_to_jobs = {}
        self._orchestrator = orchestrator

    async def create_job(self, job_request: JobRequest) -> Tuple[Job, Status]:
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        job_status = await job.start()
        status = Status.create(job_status)
        job_record = JobRecord(job=job, status=status)
        await self.set(job_record)
        return job, status

    async def get_status_by_status_id(self, status_id: str):
        for job_record in self._job_records.values():
            if job_record.status.id == status_id:
                # TODO this one is need with background
                # return job_record.status.value
                status = job_record.job.status()
                return status
        raise JobError(f"not such status_id {status_id}")

    async def set(self, job_record: JobRecord):
        self._job_records[job_record.id] = job_record

    async def get(self, job_id: str) -> JobRecord:
        job_record = self._job_records.get(job_id)
        if job_record is None:
            raise JobError(f"not such job_id {job_id}")
        return job_record

    async def get_job_status(self, job_id: str) -> JobStatus:
        job_record = await self.get(job_id)
        return await job_record.job_status()

    async def background_pooling(self):
        # TODO pool all time status from orchestrator
        pass

    async def delete(self, job_id: str):
        job_records = await self.get(job_id)
        status = await job_records.job.delete()
        return status

    async def get_all(self) -> List[dict]:
        jobs_result = []
        for job_record in self._job_records.values():
            status = await job_record.job_status()
            jobs_result.append(({'job_id': job_record.job.id, 'status': status}))
        return jobs_result
