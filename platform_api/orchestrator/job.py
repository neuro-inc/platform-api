import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Dict, Iterable, List, Optional, Sequence

import iso8601
from neuro_auth_client.client import Quota
from yarl import URL

from ..config import OrchestratorConfig  # noqa
from .job_request import JobRequest, JobStatus


logger = logging.getLogger(__name__)
current_datetime_factory = partial(datetime.now, timezone.utc)


@dataclass(frozen=True)
class AggregatedRunTime:
    total_gpu_run_time_delta: timedelta
    total_non_gpu_run_time_delta: timedelta

    @classmethod
    def from_quota(cls, quota: Quota) -> "AggregatedRunTime":
        # TODO (ajuszkowski 4-Apr-2019) platform-auth's Quota should have
        # a property `is_initialized` that should be saved in AggrRunTime instance
        return cls(
            total_gpu_run_time_delta=quota.total_gpu_run_time_delta,
            total_non_gpu_run_time_delta=quota.total_non_gpu_run_time_delta,
        )


DEFAULT_QUOTA_NO_RESTRICTIONS: AggregatedRunTime = AggregatedRunTime.from_quota(Quota())

# TODO: consider adding JobStatusReason Enum


@dataclass(frozen=True)
class JobStatusItem:
    status: JobStatus
    transition_time: datetime = field(compare=False)
    reason: Optional[str] = None
    description: Optional[str] = None

    @property
    def is_pending(self) -> bool:
        return self.status.is_pending

    @property
    def is_running(self) -> bool:
        return self.status.is_running

    @property
    def is_finished(self) -> bool:
        return self.status.is_finished

    @classmethod
    def create(
        cls,
        status: JobStatus,
        *,
        transition_time: Optional[datetime] = None,
        current_datetime_factory=current_datetime_factory,
        **kwargs,
    ) -> "JobStatusItem":
        transition_time = transition_time or current_datetime_factory()
        return cls(  # type: ignore
            status=status, transition_time=transition_time, **kwargs
        )

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "JobStatusItem":
        status = JobStatus(payload["status"])
        transition_time = iso8601.parse_date(payload["transition_time"])
        return cls(  # type: ignore
            status=status,
            transition_time=transition_time,
            reason=payload.get("reason"),
            description=payload.get("description"),
        )

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "status": str(self.status.value),
            "transition_time": self.transition_time.isoformat(),
            "reason": self.reason,
            "description": self.description,
        }


class JobStatusHistory:
    def __init__(self, items: List[JobStatusItem]) -> None:
        assert items, "JobStatusHistory should contain at least one entry"
        self._items = items

    @property
    def all(self) -> Sequence[JobStatusItem]:
        return self._items[:]

    @staticmethod
    def _find_with_status(
        items: Iterable[JobStatusItem], statuses: Sequence[JobStatus]
    ) -> Optional[JobStatusItem]:
        for item in items:
            if item.status in statuses:
                return item
        return None

    @property
    def _first_pending(self) -> Optional[JobStatusItem]:
        return self._find_with_status(self._items, (JobStatus.PENDING,))

    @property
    def _first_running(self) -> Optional[JobStatusItem]:
        return self._find_with_status(self._items, (JobStatus.RUNNING,))

    @property
    def _first_finished(self) -> Optional[JobStatusItem]:
        return self._find_with_status(
            self._items, (JobStatus.SUCCEEDED, JobStatus.FAILED)
        )

    @property
    def first(self) -> JobStatusItem:
        return self._items[0]

    @property
    def last(self) -> JobStatusItem:
        return self._items[-1]

    @property
    def current(self) -> JobStatusItem:
        return self.last

    @current.setter
    def current(self, item: JobStatusItem) -> None:
        if self.last != item:
            self._items.append(item)

    @property
    def created_at(self) -> datetime:
        return self.first.transition_time

    @property
    def created_at_str(self) -> str:
        return self.created_at.isoformat()

    @property
    def created_at_timestamp(self) -> float:
        return self.created_at.timestamp()

    @property
    def started_at(self) -> Optional[datetime]:
        """Return a `datetime` when a job became RUNNING.

        In case the job terminated instantly without an explicit transition to
        the RUNNING state, it is assumed that `started_at` gets its value from
        the transition time of the next state (either SUCCEEDED or FINISHED).
        """
        item = self._first_running or self._first_finished
        if item:
            return item.transition_time
        return None

    @property
    def started_at_str(self) -> Optional[str]:
        if self.started_at:
            return self.started_at.isoformat()
        return None

    @property
    def is_running(self) -> bool:
        return self.last.is_running

    @property
    def is_finished(self) -> bool:
        return bool(self._first_finished)

    @property
    def finished_at(self) -> Optional[datetime]:
        if self._first_finished:
            return self._first_finished.transition_time
        return None

    @property
    def finished_at_str(self) -> Optional[str]:
        if self.finished_at:
            return self.finished_at.isoformat()
        return None


class Job:
    @dataclass()
    class OrchestratorInfo:
        job_hostname: Optional[str] = None

    def __init__(
        self,
        orchestrator_config: OrchestratorConfig,
        job_request: JobRequest,
        status_history: Optional[JobStatusHistory] = None,
        # leaving `status` for backward compat with tests
        status: JobStatus = JobStatus.PENDING,
        is_deleted: bool = False,
        current_datetime_factory=current_datetime_factory,
        owner: str = "",
        name: Optional[str] = None,
        is_preemptible: bool = False,
        is_forced_to_preemptible_pool: bool = False,
    ) -> None:
        """
        :param bool is_forced_to_preemptible_pool:
            used in tests only
        """

        self._orchestrator_config = orchestrator_config
        self._job_request = job_request

        if status_history:
            self._status_history = status_history
        else:
            self._status_history = JobStatusHistory(
                [
                    JobStatusItem.create(
                        status, current_datetime_factory=current_datetime_factory
                    )
                ]
            )

        self._is_deleted = is_deleted

        self._current_datetime_factory = current_datetime_factory

        self._owner = owner
        self._name = name

        self._internal_orchestrator_info: Job.OrchestratorInfo = Job.OrchestratorInfo()
        self._is_preemptible = is_preemptible
        self._is_forced_to_preemptible_pool = is_forced_to_preemptible_pool

    @property
    def id(self) -> str:
        return self._job_request.job_id

    @property
    def description(self) -> Optional[str]:
        return self._job_request.description

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner or self._orchestrator_config.orphaned_job_owner

    def to_uri(self) -> URL:
        base_uri = "job:"
        if self.owner:
            base_uri += "//" + self.owner
        return URL(f"{base_uri}/{self.id}")

    @property
    def request(self) -> JobRequest:
        return self._job_request

    @property
    def has_gpu(self) -> bool:
        return bool(self._job_request.container.resources.gpu)

    @property
    def status(self) -> JobStatus:
        return self._status_history.current.status

    @status.setter
    def status(self, value: JobStatus) -> None:
        item = JobStatusItem.create(
            value, current_datetime_factory=self._current_datetime_factory
        )
        self._status_history.current = item

    @property
    def status_history(self) -> JobStatusHistory:
        return self._status_history

    @property
    def is_running(self) -> bool:
        return self._status_history.is_running

    @property
    def is_finished(self) -> bool:
        return self._status_history.is_finished

    @property
    def finished_at(self) -> Optional[datetime]:
        return self._status_history.finished_at

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

        return self.finished_at + self._orchestrator_config.job_deletion_delay

    @property
    def _is_time_for_deletion(self) -> bool:
        return self._deletion_planned_at <= self._current_datetime_factory()

    @property
    def should_be_deleted(self) -> bool:
        return (
            self.is_finished
            and not self.is_deleted
            and (
                self._is_time_for_deletion
                or self.status_history.current.reason == "Collected"
            )
        )

    @property
    def _collection_reason(self) -> Optional[str]:
        status_item = self._status_history.current
        if status_item.status == JobStatus.PENDING and (
            status_item.reason == "ErrImagePull"
            or status_item.reason == "ImagePullBackOff"
        ):
            # collect jobs stuck in ErrImagePull loop
            return "Image can not be pulled"
        return None

    def collect_if_needed(self) -> None:
        reason = self._collection_reason
        if reason:
            logger.info("Collecting job %s. Reason: %s", self.id, reason)
            status_item = JobStatusItem.create(
                JobStatus.FAILED, reason="Collected", description=reason
            )
            self.status_history.current = status_item

    @property
    def has_http_server_exposed(self) -> bool:
        return self._job_request.container.has_http_server_exposed

    @property
    def has_ssh_server_exposed(self) -> bool:
        return self._job_request.container.has_ssh_server_exposed

    @property
    def requires_http_auth(self) -> bool:
        return self._job_request.container.requires_http_auth

    @property
    def _http_scheme(self) -> str:
        if self._orchestrator_config.is_http_ingress_secure:
            return "https"
        return "http"

    @property
    def http_host(self) -> str:
        return self._orchestrator_config.jobs_domain_name_template.format(
            job_id=self.id
        )

    @property
    def http_url(self) -> str:
        assert self.has_http_server_exposed
        return f"{self._http_scheme}://{self.http_host}"

    @property
    def ssh_server(self) -> str:
        assert self.has_ssh_server_exposed
        ssh_domain_name = self._orchestrator_config.ssh_domain_name
        return f"ssh://{self.id}.{ssh_domain_name}:22"

    @property
    def ssh_auth_server(self) -> str:
        ssh_auth_domain_name = self._orchestrator_config.ssh_auth_domain_name
        return f"ssh://nobody@{ssh_auth_domain_name}:22"

    @property
    def finished_at_str(self) -> Optional[str]:
        return self._status_history.finished_at_str

    @property
    def internal_hostname(self):
        return self._internal_orchestrator_info.job_hostname

    @internal_hostname.setter
    def internal_hostname(self, value: Optional[str]):
        self._internal_orchestrator_info.job_hostname = value

    @property
    def is_preemptible(self) -> bool:
        return self._is_preemptible

    @property
    def is_forced_to_preemptible_pool(self) -> bool:
        return self.is_preemptible and self._is_forced_to_preemptible_pool

    def get_run_time(self) -> timedelta:
        end_time = self.finished_at or self._current_datetime_factory()
        start_time = self.status_history.created_at
        return end_time - start_time

    def to_primitive(self) -> Dict:
        statuses = [item.to_primitive() for item in self._status_history.all]
        # preserving `status` and `finished_at` for forward compat
        result = {
            "id": self.id,
            "owner": self._owner,
            "request": self.request.to_primitive(),
            "status": self.status.value,
            "statuses": statuses,
            "is_deleted": self.is_deleted,
            "finished_at": self.finished_at_str,
            "is_preemptible": self.is_preemptible,
        }
        if self.internal_hostname:
            result["internal_hostname"] = self.internal_hostname
        if self.name:
            result["name"] = self.name
        return result

    @classmethod
    def from_primitive(
        cls, orchestrator_config: OrchestratorConfig, payload: Dict
    ) -> "Job":
        job_request = JobRequest.from_primitive(payload["request"])
        status_history = cls.create_status_history_from_primitive(
            job_request.job_id, payload
        )
        is_deleted = payload.get("is_deleted", False)
        owner = payload.get("owner", "")
        name = payload.get("name")
        is_preemptible = payload.get("is_preemptible", False)
        job = cls(
            orchestrator_config=orchestrator_config,
            job_request=job_request,
            status_history=status_history,
            is_deleted=is_deleted,
            owner=owner,
            name=name,
            is_preemptible=is_preemptible,
        )
        job.internal_hostname = payload.get("internal_hostname", None)
        return job

    @staticmethod
    def create_status_history_from_primitive(
        job_id: str, payload: Dict
    ) -> JobStatusHistory:
        if "statuses" in payload:
            # already migrated to history
            items = [JobStatusItem.from_primitive(item) for item in payload["statuses"]]
        else:
            logger.info(f"Migrating job {job_id} to status history")
            status = JobStatus(payload["status"])
            transition_time = None
            if status.is_finished:
                finished_at = payload.get("finished_at")
                if finished_at:
                    transition_time = iso8601.parse_date(finished_at)
            items = [JobStatusItem.create(status, transition_time=transition_time)]
        return JobStatusHistory(items)


@dataclass(frozen=True)
class JobStats:
    cpu: float
    memory: float

    gpu_duty_cycle: Optional[int] = None
    gpu_memory: Optional[float] = None

    timestamp: float = field(default_factory=time.time)
