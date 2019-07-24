from abc import ABC, abstractmethod
from typing import Any, Optional, Sequence

from platform_api.resource import ResourcePoolType

from ..config import OrchestratorConfig, StorageConfig  # noqa
from .job import Job, JobStats, JobStatusItem
from .job_request import JobStatus


class LogReader(ABC):
    async def __aenter__(self) -> "LogReader":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    @abstractmethod
    async def read(self, size: int = -1) -> bytes:
        pass


class Telemetry(ABC):
    async def __aenter__(self) -> "Telemetry":
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    @abstractmethod
    async def get_latest_stats(self) -> Optional[JobStats]:
        pass


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
    async def get_job_log_reader(self, job: Job) -> LogReader:
        pass

    @abstractmethod
    async def get_job_telemetry(self, job: Job) -> Telemetry:
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
