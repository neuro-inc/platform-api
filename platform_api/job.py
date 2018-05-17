from dataclasses import dataclass
from orchestrator.orchestrator import Orchestrator


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    docker_image: str
    args: [str]


class Job:
    def __init__(self, orchestrator: Orchestrator, job_request: JobRequest):
        self._orchestrator = orchestrator
        self._job_request = job_request

    async def start(self):
        return await self._orchestrator.start_job(self._job_request)

    async def stop(self):
        job_id = await self.get_id()
        return await self._orchestrator.stop_job(job_id=job_id)

    async def status(self):
        job_id = await self.get_id()
        return await self._orchestrator.job_status(job_id=job_id)

    async def get_id(self):
        return self._job_request.job_id
