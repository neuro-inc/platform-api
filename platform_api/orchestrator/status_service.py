from abc import ABC, abstractmethod
from dataclasses import dataclass
import uuid
import logging
from typing import Optional

from .job import Job
from .job_request import JobStatus


logger = logging.getLogger(__file__)


@dataclass(frozen=True)
class Status:
    status_id: str
    job: Job

    async def value(self) -> JobStatus:
        status = await self.job.status()
        # NOTE: this is a temporary workaround
        # until the job tracker is implemented
        if status.is_finished:
            await self.job.delete()
        return status

    @classmethod
    def create(cls, job) -> 'Status':
        status_id = str(uuid.uuid4())
        return cls(status_id, job)  # type: ignore

    @property
    def id(self):
        return self.status_id


class StatusService(ABC):
    @abstractmethod
    async def create(self, job: Job) -> Status:
        pass

    @abstractmethod
    async def get(self, status_id: str) -> Optional[Status]:
        pass


class InMemoryStatusService(StatusService):
    def __init__(self):
        self._statuses = {}

    async def create(self, job: Job) -> Status:
        status = Status.create(job)
        self._statuses[status.id] = status
        return status

    async def get(self, status_id: str) -> Optional[Status]:
        return self._statuses.get(status_id)
