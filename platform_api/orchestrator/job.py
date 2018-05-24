from .job_request import JobRequest
from .base import Orchestrator


class Job:
    def __init__(
            self, orchestrator: Orchestrator, job_request: JobRequest
            ) -> None:
        self._orchestrator = orchestrator
        self._job_request = job_request

    async def start(self):
        return await self._orchestrator.start_job(self._job_request)

    async def delete(self):
        return await self._orchestrator.delete_job(job_id=self.id)

    async def status(self):
        return await self._orchestrator.status_job(job_id=self.id)

    @property
    def id(self):
        return self._job_request.job_id
