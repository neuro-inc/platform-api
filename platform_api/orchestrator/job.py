from .job_request import JobRequest
from .base import Orchestrator


class Job:
    def __init__(self, orchestrator: Orchestrator, job_request: JobRequest):
        self._orchestrator = orchestrator
        self._job_request = job_request

    async def start(self):
        return await self._orchestrator.job_start(self._job_request)

    async def delete(self):
        job_id = await self.get_id()
        return await self._orchestrator.job_delete(job_id=job_id)

    async def status(self):
        job_id = await self.get_id()
        return await self._orchestrator.job_status(job_id=job_id)

    async def get_id(self):
        return self._job_request.job_id
