from abc import ABC, abstractmethod
from typing import List
import logging


from .job import Job
from .job_request import JobRequest
from .base import Orchestrator


logger = logging.getLogger(__file__)


class JobsService(ABC):
    @abstractmethod
    async def create_job(self, job_request: JobRequest) -> Job:
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


class InMemoryJobsService(JobsService):
    def __init__(self, orchestrator: Orchestrator):
        self._jobs = {}
        self._orchestrator = orchestrator

    async def create_job(self, job_request: JobRequest) -> Job:
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        _ = await job.start()
        await self.set(job)
        return job

    async def delete(self, job_id: str):
        job = self._jobs.get(job_id)
        status = await job.delete()
        return status

    async def get(self, job_id: str) -> Job:
        return self._jobs.get(job_id)

    async def set(self, job: Job):
        self._jobs[job.id] = job

    async def get_all(self) -> List[tuple]:
        jobs_result = []
        for job in self._jobs.values():
            # TODO replace with background
            status = await job.status()
            jobs_result.append((job.id, status))
        return jobs_result
