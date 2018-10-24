from abc import ABC, abstractmethod
from typing import Sequence

from platform_api.resource import ResourcePoolType

from ..config import OrchestratorConfig  # noqa
from .job import Job, JobStatusItem
from .job_request import JobStatus


class LogReader(ABC):
    async def __aenter__(self) -> "LogReader":
        return self

    async def __aexit__(self, *args) -> None:
        pass

    @abstractmethod
    async def read(self, size: int = -1) -> bytes:
        pass


class Orchestrator(ABC):
    @property
    @abstractmethod
    def config(self) -> OrchestratorConfig:
        pass

    @abstractmethod
    async def start_job(self, job: Job, token: str) -> JobStatus:
        pass

    async def status_job(self, job_id: str) -> JobStatus:
        return (await self.get_job_status(job_id)).status

    @abstractmethod
    async def get_job_status(self, job_id: str) -> JobStatusItem:
        pass

    @abstractmethod
    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass

    @abstractmethod
    async def delete_job(self, job: Job) -> JobStatus:
        pass

    @abstractmethod
    async def get_resource_pool_types(self) -> Sequence[ResourcePoolType]:
        return self.config.resource_pool_types
