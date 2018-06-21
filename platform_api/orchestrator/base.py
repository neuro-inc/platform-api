from abc import ABC, abstractmethod
from .job_request import JobRequest, JobStatus


class Orchestrator(ABC):
    @property
    @abstractmethod
    def config(self):
        pass

    @abstractmethod
    async def start_job(self, job_request: JobRequest) -> JobStatus:
        pass

    @abstractmethod
    async def status_job(self, job_id: str) -> JobStatus:
        pass

    @abstractmethod
    async def delete_job(self, job_id: str) -> JobStatus:
        pass
