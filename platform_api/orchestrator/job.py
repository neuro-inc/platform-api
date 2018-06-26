from typing import Dict

from .job_request import JobRequest, JobStatus


class Job:
    def __init__(
            self, orchestrator: 'Orchestrator', job_request: JobRequest,
            status: JobStatus=JobStatus.PENDING) -> None:
        # TODO: replace Orchestrator with OrchestratorConfig/KubeConfig
        self._orchestrator = orchestrator
        self._job_request = job_request
        # TODO: introduce JobStatus object with diagnostic info
        self._status = status

    # WARNING: these three methods to be deleted soon
    async def start(self) -> JobStatus:
        return await self._orchestrator.start_job(self)

    async def delete(self) -> JobStatus:
        return await self._orchestrator.delete_job(self)

    async def query_status(self) -> JobStatus:
        return await self._orchestrator.status_job(job_id=self.id)

    @property
    def id(self):
        return self._job_request.job_id

    @property
    def request(self) -> JobRequest:
        return self._job_request

    @property
    def status(self) -> JobStatus:
        return self._status

    @status.setter
    def status(self, value: JobStatus) -> None:
        self._status = value

    @property
    def is_finished(self) -> bool:
        return self._status.is_finished

    @property
    def has_http_server_exposed(self) -> bool:
        return self._job_request.container.has_http_server_exposed

    @property
    def http_url(self) -> str:
        assert self.has_http_server_exposed
        jobs_domain_name = self._orchestrator.config.jobs_ingress_domain_name
        return f'http://{self.id}.{jobs_domain_name}'

    def to_primitive(self) -> Dict:
        return {
            'id': self.id,
            'request': self.request.to_primitive(),
            'status': self._status.value,
        }

    @classmethod
    def from_primitive(cls, orchestrator: 'Orchestrator', payload: Dict) -> 'Job':
        job_request = JobRequest.from_primitive(payload['request'])
        status = JobStatus(payload['status'])
        return cls(
            orchestrator=orchestrator, job_request=job_request, status=status)
