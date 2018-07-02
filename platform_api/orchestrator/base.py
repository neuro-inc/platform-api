from abc import ABC, abstractmethod

from ..config import OrchestratorConfig
from .job import Job
from .job_request import JobStatus


class Orchestrator(ABC):
    @property
    @abstractmethod
    def config(self) -> OrchestratorConfig:
        pass

    @abstractmethod
    async def start_job(self, job: Job) -> JobStatus:
        pass

    @abstractmethod
    async def status_job(self, job_id: str) -> JobStatus:
        pass

    @abstractmethod
    async def update_job_status(self, job: Job) -> None:
        pass

    @abstractmethod
    async def delete_job(self, job: Job) -> JobStatus:
        pass
