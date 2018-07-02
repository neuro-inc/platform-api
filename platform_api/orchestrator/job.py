from typing import Dict

from ..config import OrchestratorConfig  # noqa
from .job_request import JobRequest, JobStatus


class Job:
    def __init__(
            self, orchestrator_config: OrchestratorConfig,
            job_request: JobRequest,
            status: JobStatus=JobStatus.PENDING) -> None:
        self._orchestrator_config = orchestrator_config
        self._job_request = job_request
        # TODO: introduce JobStatus object with diagnostic info
        self._status = status

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
        jobs_domain_name = self._orchestrator_config.jobs_domain_name
        return f'http://{self.id}.{jobs_domain_name}'

    def to_primitive(self) -> Dict:
        return {
            'id': self.id,
            'request': self.request.to_primitive(),
            'status': self._status.value,
        }

    @classmethod
    def from_primitive(
            cls, orchestrator_config: OrchestratorConfig,
            payload: Dict) -> 'Job':
        job_request = JobRequest.from_primitive(payload['request'])
        status = JobStatus(payload['status'])
        return cls(
            orchestrator_config=orchestrator_config,
            job_request=job_request, status=status)
