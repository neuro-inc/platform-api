from .job_request import JobRequest, JobStatus
from .base import Orchestrator


class Job:
    def __init__(
            self, orchestrator: Orchestrator, job_request: JobRequest
            ) -> None:
        self._orchestrator = orchestrator
        self._job_request = job_request

    async def start(self) -> JobStatus:
        return await self._orchestrator.start_job(self._job_request)

    async def delete(self) -> JobStatus:
        return await self._orchestrator.delete_job(job_id=self.id)

    async def status(self) -> JobStatus:
        return await self._orchestrator.status_job(job_id=self.id)

    @property
    def id(self):
        return self._job_request.job_id

    @property
    def has_http_server_exposed(self) -> bool:
        return self._job_request.container.has_http_server_exposed

    @property
    def http_url(self) -> str:
        assert self.has_http_server_exposed
        jobs_domain_name = self._orchestrator.config.jobs_ingress_domain_name
        return f'http://{self.id}.{jobs_domain_name}'
