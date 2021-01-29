from abc import ABC, abstractmethod
from typing import List

from platform_api.cluster_config import StorageConfig

from .job import Job, JobStatusItem
from .job_request import Disk, JobStatus


class Orchestrator(ABC):
    @property
    @abstractmethod
    def storage_config(self) -> StorageConfig:
        pass

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
        self, user_name: str, secret_names: List[str]
    ) -> List[str]:
        pass

    @abstractmethod
    async def get_missing_disks(self, disks: List[Disk]) -> List[Disk]:
        pass
