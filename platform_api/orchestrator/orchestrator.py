from abc import ABC, abstractmethod

from platform_api.job import Job, JobRequest


class Orchestrator(ABC):
    @abstractmethod
    async def new_job(self, job_request: JobRequest) -> Job:
        """
        :return: Job
        """

    @abstractmethod
    async def get_job(self, job_id: str) -> Job:
        """
        :return: Job
        """
