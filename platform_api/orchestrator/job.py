from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Dict, Optional

from ..config import OrchestratorConfig  # noqa
from .job_request import JobRequest, JobStatus

current_datetime_factory = partial(datetime.now, timezone.utc)


class Job:
    def __init__(
            self, orchestrator_config: OrchestratorConfig,
            job_request: JobRequest,
            status: JobStatus=JobStatus.PENDING,
            current_datetime_factory=current_datetime_factory) -> None:
        self._orchestrator_config = orchestrator_config
        self._job_request = job_request
        # TODO: introduce JobStatus object with diagnostic info
        self._status = status

        self._finished_at: Optional[datetime] = None
        self._is_deleted: bool = False

        self._current_datetime_factory = current_datetime_factory

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
        if self.is_finished and not self._finished_at:
            self._finished_at = self._current_datetime_factory()

    @property
    def is_finished(self) -> bool:
        return self._status.is_finished

    @property
    def finished_at(self) -> Optional[datetime]:
        return self._finished_at

    @property
    def is_deleted(self) -> bool:
        return self._is_deleted

    @is_deleted.setter
    def is_deleted(self, value: bool) -> None:
        self._is_deleted = value

    @property
    def _deletion_planned_at(self) -> Optional[datetime]:
        if not self.finished_at:
            return None

        return (
            self.finished_at + self._orchestrator_config.job_deletion_delay)

    @property
    def _is_time_for_deletion(self) -> bool:
        return self._deletion_planned_at <= self._current_datetime_factory()

    @property
    def should_be_deleted(self) -> bool:
        return (
            self.is_finished and not self._is_deleted and
            self._is_time_for_deletion)

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
