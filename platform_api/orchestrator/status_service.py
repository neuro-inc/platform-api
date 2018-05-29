from abc import ABC, abstractmethod
import uuid
import logging
from typing import Optional

from .job import Job
from .job_request import JobStatus


logger = logging.getLogger(__file__)


class StatusService(ABC):
    @abstractmethod
    async def create(self, job: Job) -> str:
        pass

    @abstractmethod
    async def get(self, status_id: str) -> dict:
        pass


class InMemoryStatusService(StatusService):
    def __init__(self):
        self._statuses = {}

    async def create(self, job: Job) -> str:
        status_id = str(uuid.uuid4())
        self._statuses[status_id] = job
        return status_id

    async def get(self, status_id: str) -> Optional[JobStatus]:
        job = self._statuses.get(status_id)
        if job is None:
            return None
        else:
            return await job.status()
