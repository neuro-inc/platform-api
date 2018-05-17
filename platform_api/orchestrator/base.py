from abc import ABC, abstractmethod
from job_request import JobRequest


class Orchestrator(ABC):
    @abstractmethod
    async def start_job(self, job_request: JobRequest):
        pass

    @abstractmethod
    async def status_job(self, job_id: str):
        pass

    @abstractmethod
    async def delete_job(self, job_id: str):
        pass

