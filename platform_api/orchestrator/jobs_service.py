from abc import ABC, abstractmethod
import logging


from .job import Job


logger = logging.getLogger(__file__)


class JobService(ABC):
    @abstractmethod
    def get(self, job_id: str) -> Job:
        pass

    @abstractmethod
    def set(self, job: Job):
        pass

    @abstractmethod
    def get_all(self):
        pass


class InMemoryJobService(JobService):
    def __init__(self):
        self._jobs = {}

    def get(self, job_id: str) -> Job:
        return self._jobs.get(job_id)

    def set(self, job: Job):
        self._jobs[job.id] = job

    def get_all(self):
        pass