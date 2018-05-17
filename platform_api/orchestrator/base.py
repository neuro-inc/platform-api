from abc import ABC, abstractmethod
from platform_api.job_request import JobRequest, JobStatus


class Orchestrator(ABC):
    @abstractmethod
    async def job_start(self, job_request: JobRequest) -> JobStatus:
        pass

    @abstractmethod
    async def job_status(self, job_id: str) -> JobStatus:
        pass

    @abstractmethod
    async def job_delete(self, job_id: str) -> JobStatus:
        pass
