from abc import ABC, abstractmethod
from typing import List

from .job import Job, JobStatusItem
from .job_request import Disk, JobStatus


class Orchestrator(ABC):
    @abstractmethod
    async def start_job(self, job: Job) -> JobStatus:
        pass

    @abstractmethod
    async def get_job_status(self, job: Job) -> JobStatusItem:
        pass

    @abstractmethod
    async def delete_job(self, job: Job) -> JobStatus:
        pass

    @abstractmethod
    async def get_missing_secrets(
        self, secret_path: str, secret_names: List[str]
    ) -> List[str]:
        pass

    @abstractmethod
    async def get_missing_disks(self, disks: List[Disk]) -> List[Disk]:
        pass
