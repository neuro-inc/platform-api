from abc import ABC, abstractmethod
from typing import Sequence

from platform_api.resource import ResourcePoolType

from ..config import OrchestratorConfig, StorageConfig
from .job import Job, JobStatusItem
from .job_request import JobStatus


class Orchestrator(ABC):
    @property
    @abstractmethod
    def config(self) -> OrchestratorConfig:
        pass

    @property
    @abstractmethod
    def storage_config(self) -> StorageConfig:
        pass

    @abstractmethod
    async def start_job(self, job: Job, token: str) -> JobStatus:
        pass

    @abstractmethod
    async def get_job_status(self, job: Job) -> JobStatusItem:
        pass

    @abstractmethod
    async def delete_job(self, job: Job) -> JobStatus:
        pass

    async def get_resource_pool_types(self) -> Sequence[ResourcePoolType]:
        return self.config.resource_pool_types

    async def get_available_gpu_models(self) -> Sequence[str]:
        pool_types = await self.get_resource_pool_types()
        return list(
            dict.fromkeys(
                [pool_type.gpu_model for pool_type in pool_types if pool_type.gpu_model]
            )
        )
