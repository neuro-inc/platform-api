from abc import abstractmethod
from contextlib import AbstractAsyncContextManager

from .job import Job, JobStatusItem
from .job_request import Disk, JobStatus


class Orchestrator(AbstractAsyncContextManager[None]):
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
    async def preempt_jobs(
        self, jobs_to_schedule: list[Job], preemptible_jobs: list[Job]
    ) -> list[Job]:
        pass

    @abstractmethod
    async def get_scheduled_jobs(self, jobs: list[Job]) -> list[Job]:
        pass

    @abstractmethod
    async def get_schedulable_jobs(self, jobs: list[Job]) -> list[Job]:
        pass

    @abstractmethod
    async def get_missing_secrets(
        self, job: Job, secret_path: str, secret_names: list[str]
    ) -> list[str]:
        pass

    @abstractmethod
    async def get_missing_disks(
        self, namespace: str, org_name: str, project_name: str, disks: list[Disk]
    ) -> list[Disk]:
        pass
