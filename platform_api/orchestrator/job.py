from __future__ import annotations

import enum
import hashlib
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import partial
from typing import Any

import iso8601
from yarl import URL

from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import NO_ORG

from ..cluster_config import DEFAULT_ENERGY_SCHEDULE_NAME
from ..resource import Preset
from .job_request import (
    ContainerResources,
    ContainerVolume,
    JobError,
    JobRequest,
    JobStatus,
)

JOB_NAME_SEPARATOR = "--"


logger = logging.getLogger(__name__)
current_datetime_factory = partial(datetime.now, timezone.utc)


DEFAULT_ORPHANED_JOB_OWNER = "compute"


class JobStatusReason:
    # TODO (A.Yushkovskiy) Convert to enum to use as a type of `JobStatusItem.reason`
    # TODO (A.Yushkovskiy) Refactor job status reasons taxonomy (issue #796)
    # k8s reasons:
    # - 'waiting' reasons:
    POD_INITIALIZING = "PodInitializing"
    CONTAINER_CREATING = "ContainerCreating"
    ERR_IMAGE_PULL = "ErrImagePull"
    IMAGE_PULL_BACK_OFF = "ImagePullBackOff"
    INVALID_IMAGE_NAME = "InvalidImageName"
    # - 'terminated' reasons:
    OOM_KILLED = "OOMKilled"
    COMPLETED = "Completed"
    ERROR = "Error"
    CONTAINER_CANNOT_RUN = "ContainerCannotRun"
    # neuromation custom reasons:
    CREATING = "Creating"
    COLLECTED = "Collected"
    SCHEDULING = "Scheduling"
    PULLING = "Pulling"
    NOT_FOUND = "NotFound"  # "The job could not be scheduled or was preempted."
    CLUSTER_NOT_FOUND = "ClusterNotFound"
    CLUSTER_SCALING_UP = "ClusterScalingUp"
    CLUSTER_SCALE_UP_FAILED = "ClusterScaleUpFailed"
    RESTARTING = "Restarting"
    DISK_UNAVAILABLE = "DiskUnavailable"
    QUOTA_EXHAUSTED = "QuotaExhausted"
    LIFE_SPAN_ENDED = "LifeSpanEnded"
    USER_REQUESTED = "UserRequested"


@dataclass(frozen=True)
class JobStatusItem:
    status: JobStatus
    transition_time: datetime = field(compare=False)
    # TODO (A.Yushkovskiy) it's better to have `reason: Optional[JobStatusReason]`
    reason: str | None = None
    description: str | None = None
    exit_code: int | None = None

    @property
    def is_pending(self) -> bool:
        return self.status.is_pending

    @property
    def is_running(self) -> bool:
        return self.status.is_running

    @property
    def is_suspended(self) -> bool:
        return self.status.is_suspended

    @property
    def is_finished(self) -> bool:
        return self.status.is_finished

    @classmethod
    def create(
        cls,
        status: JobStatus,
        *,
        transition_time: datetime | None = None,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
        **kwargs: Any,
    ) -> JobStatusItem:
        transition_time = transition_time or current_datetime_factory()
        return cls(status=status, transition_time=transition_time, **kwargs)

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> JobStatusItem:
        status = JobStatus(payload["status"])
        transition_time = iso8601.parse_date(payload["transition_time"])
        return cls(
            status=status,
            transition_time=transition_time,
            reason=payload.get("reason"),
            description=payload.get("description"),
            exit_code=payload.get("exit_code"),
        )

    def to_primitive(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": str(self.status.value),
            "transition_time": self.transition_time.isoformat(),
            "reason": self.reason,
            "description": self.description,
        }
        if self.exit_code is not None:
            result["exit_code"] = self.exit_code
        return result


class JobStatusHistory:
    def __init__(self, items: list[JobStatusItem]) -> None:
        assert items, "JobStatusHistory should contain at least one entry"
        self._items = items

    @property
    def all(self) -> Sequence[JobStatusItem]:
        return self._items[:]

    @staticmethod
    def _find_with_status(
        items: Iterable[JobStatusItem], statuses: Sequence[JobStatus]
    ) -> JobStatusItem | None:
        for item in items:
            if item.status in statuses:
                return item
        return None

    @property
    def _first_running(self) -> JobStatusItem | None:
        return self._find_with_status(self._items, (JobStatus.RUNNING,))

    @property
    def _first_finished(self) -> JobStatusItem | None:
        return self._find_with_status(
            self._items, (JobStatus.SUCCEEDED, JobStatus.CANCELLED, JobStatus.FAILED)
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
            if self.last.is_finished:
                raise JobError("Invalid job status transition")
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
    def started_at(self) -> datetime | None:
        """Return a `datetime` when a job became RUNNING.

        In case the job terminated instantly without an explicit transition to
        the RUNNING state, it is assumed that `started_at` gets its value from
        the transition time of the next state (either SUCCEEDED or FAILED or
        CANCELLED).
        """
        item = self._first_running or self._first_finished
        if item:
            return item.transition_time
        return None

    @property
    def continued_at(self) -> datetime | None:
        result: JobStatusItem | None = None
        for item in reversed(self._items):
            if item.status == JobStatus.RUNNING:
                result = item
            elif result is not None:
                return result.transition_time
        return None

    @property
    def started_at_str(self) -> str | None:
        if self.started_at:
            return self.started_at.isoformat()
        return None

    @property
    def is_running(self) -> bool:
        return self.last.is_running

    @property
    def is_suspended(self) -> bool:
        return self.last.is_suspended

    @property
    def is_finished(self) -> bool:
        return self.last.is_finished

    @property
    def finished_at(self) -> datetime | None:
        if self.last.is_finished:
            return self.last.transition_time
        return None

    @property
    def finished_at_str(self) -> str | None:
        if self.finished_at:
            return self.finished_at.isoformat()
        return None

    @property
    def restart_count(self) -> int:
        # This field is not 100% accurate because of polling nature of collecting
        # status items. On other side, even k8s `restartCount` can be wrong,
        # so it is should be OK.
        return sum(
            1
            for item in self._items
            if item.reason == JobStatusReason.RESTARTING
            or item.status == JobStatus.SUSPENDED
        )


@enum.unique
class JobRestartPolicy(str, enum.Enum):
    ALWAYS = "always"
    ON_FAILURE = "on-failure"
    NEVER = "never"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.__str__().__repr__()


@enum.unique
class JobPriority(enum.IntEnum):
    LOW = -1
    NORMAL = 0
    HIGH = 1

    def to_name(self) -> str:
        return self.name.lower()

    @classmethod
    def from_name(cls, name: str) -> JobPriority:
        return cls[name.upper()]


@dataclass
class JobRecord:
    request: JobRequest
    owner: str
    status_history: JobStatusHistory
    cluster_name: str
    project_name: str
    org_project_hash: bytes
    org_name: str | None = None
    name: str | None = None
    preset_name: str | None = None
    tags: Sequence[str] = ()
    scheduler_enabled: bool = False
    preemptible_node: bool = False
    pass_config: bool = False
    materialized: bool = False
    privileged: bool = False
    max_run_time_minutes: int | None = None
    internal_hostname: str | None = None
    internal_hostname_named: str | None = None
    schedule_timeout: float | None = None
    restart_policy: JobRestartPolicy = JobRestartPolicy.NEVER
    priority: JobPriority = JobPriority.NORMAL
    energy_schedule_name: str = DEFAULT_ENERGY_SCHEDULE_NAME

    # Retention (allows other services as platform-monitoring to cleanup jobs resources)
    being_dropped: bool = False
    logs_removed: bool = False

    # for testing only
    allow_empty_cluster_name: bool = False

    @classmethod
    def create(
        cls,
        *,
        status: JobStatus = JobStatus.PENDING,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
        orphaned_job_owner: str = DEFAULT_ORPHANED_JOB_OWNER,
        **kwargs: Any,
    ) -> JobRecord:
        if not kwargs.get("status_history"):
            status_history = JobStatusHistory(
                [
                    JobStatusItem.create(
                        status, current_datetime_factory=current_datetime_factory
                    )
                ]
            )
            kwargs["status_history"] = status_history
        if not kwargs.get("owner"):
            kwargs["owner"] = orphaned_job_owner
        if not kwargs.get("project_name"):
            kwargs["project_name"] = get_base_owner(kwargs["owner"])
        kwargs["org_project_hash"] = cls._create_org_project_hash(
            kwargs.get("org_name"), kwargs["project_name"]
        )
        return cls(**kwargs)

    @classmethod
    def _create_org_project_hash(cls, org_name: str | None, project_name: str) -> bytes:
        return cls._create_hash(org_name or NO_ORG, project_name)[:5]

    @classmethod
    def _create_hash(cls, *args: str) -> bytes:
        hasher = hashlib.new("sha256")
        for arg in args:
            hasher.update(arg.encode("utf-8"))
        return hasher.digest()

    @property
    def id(self) -> str:
        return self.request.job_id

    @property
    def base_owner(self) -> str:
        return get_base_owner(self.owner)

    @property
    def status(self) -> JobStatus:
        return self.status_history.current.status

    @status.setter
    def status(self, value: JobStatus) -> None:
        self.set_status(value)

    def set_status(
        self,
        value: JobStatus,
        *,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> None:
        item = JobStatusItem.create(
            value, current_datetime_factory=current_datetime_factory
        )
        self.status_history.current = item

    @property
    def is_restartable(self) -> bool:
        return (
            self.scheduler_enabled
            or self.preemptible_node
            or self.restart_policy
            in (
                JobRestartPolicy.ALWAYS,
                JobRestartPolicy.ON_FAILURE,
            )
        )

    @property
    def is_finished(self) -> bool:
        return self.status_history.is_finished

    @property
    def finished_at(self) -> datetime | None:
        return self.status_history.finished_at

    @property
    def finished_at_str(self) -> str | None:
        return self.status_history.finished_at_str

    @property
    def has_nvidia_gpu(self) -> bool:
        return bool(self.request.container.resources.nvidia_gpu)

    @property
    def has_amd_gpu(self) -> bool:
        return bool(self.request.container.resources.amd_gpu)

    def get_run_time(
        self,
        *,
        only_after: datetime | None = None,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> timedelta:
        def _filter_only_after(begin: datetime, end: datetime) -> timedelta:
            if only_after is None or only_after <= begin:
                return end - begin
            if only_after < end:
                return end - only_after
            return timedelta()

        run_time = timedelta()
        prev_time: datetime | None = None
        for item in self.status_history.all:
            if prev_time:
                run_time += _filter_only_after(prev_time, item.transition_time)
            prev_time = item.transition_time if item.status.is_running else None
        if prev_time:
            # job still running
            run_time += _filter_only_after(prev_time, current_datetime_factory())
        return run_time

    def _is_time_for_deletion(
        self, delay: timedelta, current_datetime_factory: Callable[[], datetime]
    ) -> bool:
        assert self.finished_at
        deletion_planned_at = self.finished_at + delay
        return deletion_planned_at <= current_datetime_factory()

    def _is_reason_for_deletion(self) -> bool:
        return self.status_history.current.reason in (
            JobStatusReason.COLLECTED,
            JobStatusReason.CLUSTER_SCALE_UP_FAILED,
        )

    def _is_status_for_deletion(self) -> bool:
        return self.status_history.current.status in (JobStatus.CANCELLED,)

    def should_be_deleted(
        self,
        *,
        delay: timedelta = timedelta(),
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
    ) -> bool:
        return (
            self.is_finished
            and self.materialized
            and (
                self._is_time_for_deletion(
                    delay=delay, current_datetime_factory=current_datetime_factory
                )
                or self._is_reason_for_deletion()
                or self._is_status_for_deletion()
            )
        )

    def to_primitive(self) -> dict[str, Any]:
        if not self.allow_empty_cluster_name and not self.cluster_name:
            raise RuntimeError(
                "empty cluster name must be already replaced with `default`"
            )
        statuses = [item.to_primitive() for item in self.status_history.all]
        # preserving `status` and `finished_at` for forward compat
        result = {
            "id": self.id,
            "owner": self.owner,
            "cluster_name": self.cluster_name,
            "project_name": self.project_name,
            "org_project_hash": self.org_project_hash.hex(),
            "request": self.request.to_primitive(),
            "status": self.status.value,
            "statuses": statuses,
            "materialized": self.materialized,
            "finished_at": self.finished_at_str,
            "scheduler_enabled": self.scheduler_enabled,
            "preemptible_node": self.preemptible_node,
            "pass_config": self.pass_config,
            "privileged": self.privileged,
            "restart_policy": str(self.restart_policy),
            "priority": int(self.priority),
        }
        if self.schedule_timeout:
            result["schedule_timeout"] = self.schedule_timeout
        if self.max_run_time_minutes is not None:
            result["max_run_time_minutes"] = self.max_run_time_minutes
        if self.internal_hostname:
            result["internal_hostname"] = self.internal_hostname
        if self.internal_hostname_named:
            result["internal_hostname_named"] = self.internal_hostname_named
        if self.name:
            result["name"] = self.name
        if self.preset_name:
            result["preset_name"] = self.preset_name
        if self.tags:
            result["tags"] = self.tags
        if self.being_dropped:
            result["being_dropped"] = self.being_dropped
        if self.logs_removed:
            result["logs_removed"] = self.logs_removed
        if self.org_name:
            result["org_name"] = self.org_name
        if self.energy_schedule_name:
            result["energy_schedule_name"] = self.energy_schedule_name
        return result

    @classmethod
    def from_primitive(
        cls,
        payload: dict[str, Any],
        orphaned_job_owner: str = DEFAULT_ORPHANED_JOB_OWNER,
    ) -> JobRecord:
        request = JobRequest.from_primitive(payload["request"])
        status_history = cls.create_status_history_from_primitive(
            request.job_id, payload
        )
        owner = payload.get("owner") or orphaned_job_owner
        org_name = payload.get("org_name", None)
        project_name = payload.get("project_name") or get_base_owner(owner)
        org_project_hash = payload.get("org_project_hash")
        if org_project_hash and isinstance(org_project_hash, str):
            org_project_hash = bytes.fromhex(org_project_hash)
        elif not org_project_hash:
            org_project_hash = cls._create_org_project_hash(org_name, project_name)
        return cls(
            request=request,
            status_history=status_history,
            materialized=payload.get("materialized", False),
            owner=owner,
            cluster_name=payload.get("cluster_name") or "",
            name=payload.get("name"),
            preset_name=payload.get("preset_name"),
            tags=payload.get("tags", ()),
            org_name=org_name,
            project_name=project_name,
            org_project_hash=org_project_hash,
            scheduler_enabled=payload.get("scheduler_enabled", None)
            or payload.get("is_preemptible", False),
            preemptible_node=payload.get("preemptible_node", None)
            or payload.get("is_preemptible_node_required", False),
            pass_config=payload.get("pass_config", False),
            privileged=payload.get("privileged", False),
            max_run_time_minutes=payload.get("max_run_time_minutes", None),
            internal_hostname=payload.get("internal_hostname", None),
            internal_hostname_named=payload.get("internal_hostname_named", None),
            schedule_timeout=payload.get("schedule_timeout", None),
            restart_policy=JobRestartPolicy(
                payload.get("restart_policy", str(cls.restart_policy))
            ),
            priority=JobPriority(payload.get("priority", int(cls.priority))),
            being_dropped=payload.get("being_dropped", False),
            logs_removed=payload.get("logs_removed", False),
            energy_schedule_name=payload.get(
                "energy_schedule_name", cls.energy_schedule_name
            ),
        )

    @staticmethod
    def create_status_history_from_primitive(
        job_id: str, payload: dict[str, Any]
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


class Job:
    def __init__(
        self,
        orchestrator_config: OrchestratorConfig,
        *,
        record: JobRecord,
        current_datetime_factory: Callable[[], datetime] = current_datetime_factory,
        image_pull_error_delay: timedelta = timedelta(minutes=2),
    ) -> None:
        self._orchestrator_config = orchestrator_config

        self._record = record
        self._job_request = record.request
        self._status_history = record.status_history

        self._current_datetime_factory = current_datetime_factory

        self._owner = record.owner
        self._name = record.name
        self._tags = record.tags

        self._scheduler_enabled = record.scheduler_enabled
        self._preemptible_node = record.preemptible_node
        self._pass_config = record.pass_config
        self._image_pull_error_delay = image_pull_error_delay

    @property
    def id(self) -> str:
        return self._job_request.job_id

    @property
    def description(self) -> str | None:
        return self._job_request.description

    @property
    def name(self) -> str | None:
        return self._name

    @property
    def preset_name(self) -> str | None:
        return self._record.preset_name

    @property
    def preset(self) -> Preset | None:
        try:
            return next(
                preset
                for preset in self._orchestrator_config.presets
                if preset.name == self.preset_name
            )
        except StopIteration:
            return None

    @property
    def price_credits_per_hour(self) -> Decimal:
        preset = self.preset
        if preset:
            return preset.credits_per_hour
        # Default cost is maximal cost through all presets
        # If there is no presets, that it is badly configured cluster in general
        # and it is safe to assume zero cost
        result = max(
            (preset.credits_per_hour for preset in self._orchestrator_config.presets),
            default=Decimal(0),
        )
        for preset in self._orchestrator_config.presets:
            if self.resources.check_fit_into_preset(preset):
                result = min(result, preset.credits_per_hour)
        return result

    @property
    def is_named(self) -> bool:
        return self.name is not None

    @property
    def tags(self) -> Sequence[str]:
        return self._tags

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def base_owner(self) -> str:
        return get_base_owner(self._owner)

    @property
    def cluster_name(self) -> str:
        return self._record.cluster_name

    def to_uri(self) -> URL:
        assert self.cluster_name
        uri = URL.build(scheme="job", host=self.cluster_name)
        if self.org_name:
            uri /= self.org_name
        if self.project_name:
            uri /= self.project_name
        return uri / self.id

    @property
    def request(self) -> JobRequest:
        return self._job_request

    @property
    def env(self) -> dict[str, str]:
        return self._job_request.container.env

    @property
    def volumes(self) -> Sequence[ContainerVolume]:
        return self._job_request.container.volumes

    @property
    def resources(self) -> ContainerResources:
        return self._job_request.container.resources

    @property
    def has_nvidia_gpu(self) -> bool:
        return self._record.has_nvidia_gpu

    @property
    def has_amd_gpu(self) -> bool:
        return self._record.has_amd_gpu

    @property
    def status(self) -> JobStatus:
        return self._status_history.current.status

    @status.setter
    def status(self, value: JobStatus) -> None:
        self._record.set_status(
            value, current_datetime_factory=self._current_datetime_factory
        )

    @property
    def status_history(self) -> JobStatusHistory:
        return self._status_history

    @property
    def is_creating(self) -> bool:
        status_item = self.status_history.current
        return (
            status_item.status == JobStatus.PENDING
            and status_item.reason == JobStatusReason.CREATING
        )

    @property
    def is_running(self) -> bool:
        return self._status_history.is_running

    @property
    def is_finished(self) -> bool:
        return self._status_history.is_finished

    @property
    def finished_at(self) -> datetime | None:
        return self._status_history.finished_at

    @property
    def materialized(self) -> bool:
        return self._record.materialized

    @materialized.setter
    def materialized(self, value: bool) -> None:
        self._record.materialized = value

    @property
    def being_dropped(self) -> bool:
        return self._record.being_dropped

    @being_dropped.setter
    def being_dropped(self, value: bool) -> None:
        self._record.being_dropped = value

    @property
    def logs_removed(self) -> bool:
        return self._record.logs_removed

    @logs_removed.setter
    def logs_removed(self, value: bool) -> None:
        self._record.logs_removed = value

    @property
    def schedule_timeout(self) -> float | None:
        return self._record.schedule_timeout

    @property
    def _collection_reason(self) -> str | None:
        status_item = self._status_history.current
        if status_item.status == JobStatus.PENDING:
            if status_item.reason == JobStatusReason.INVALID_IMAGE_NAME:
                return f"Invalid image name '{self.request.container.image}'"
            # collect jobs stuck in ErrImagePull loop
            first_pull_error = None
            for item in reversed(self.status_history.all):
                if item.reason in (
                    JobStatusReason.ERR_IMAGE_PULL,
                    JobStatusReason.IMAGE_PULL_BACK_OFF,
                ):
                    first_pull_error = item
            if first_pull_error is not None:
                now = self._current_datetime_factory()
                if (
                    now - first_pull_error.transition_time
                    > self._image_pull_error_delay
                ):
                    return f"Image '{self.request.container.image}' can not be pulled"
        return None

    def collect_if_needed(self) -> None:
        reason = self._collection_reason
        if reason:
            logger.info("Collecting job %s. Reason: %s", self.id, reason)
            status_item = JobStatusItem.create(
                JobStatus.FAILED, reason=JobStatusReason.COLLECTED, description=reason
            )
            self.status_history.current = status_item

    @property
    def has_http_server_exposed(self) -> bool:
        return self._job_request.container.has_http_server_exposed

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
    def host_segment_named(self) -> str:
        suffix = self._record.org_project_hash.hex()
        return f"{self.name}{JOB_NAME_SEPARATOR}{suffix}"

    @property
    def http_host_named(self) -> str | None:
        if not self.is_named:
            return None
        return self._orchestrator_config.jobs_domain_name_template.format(
            job_id=self.host_segment_named
        )

    @property
    def http_hosts(self) -> Iterator[str]:
        yield self.http_host
        if host_named := self.http_host_named:
            yield host_named

    @property
    def http_url(self) -> str:
        assert self.has_http_server_exposed
        return f"{self._http_scheme}://{self.http_host}"

    @property
    def http_url_named(self) -> str | None:
        assert self.has_http_server_exposed
        if host_named := self.http_host_named:
            return f"{self._http_scheme}://{host_named}"
        return None

    @property
    def finished_at_str(self) -> str | None:
        return self._status_history.finished_at_str

    @property
    def internal_hostname(self) -> str | None:
        return self._record.internal_hostname

    @property
    def internal_hostname_named(self) -> str | None:
        return self._record.internal_hostname_named

    def init_job_internal_hostnames(self) -> None:
        self._record.internal_hostname = (
            self._orchestrator_config.jobs_internal_domain_name_template.format(
                job_id=self.id
            )
        )
        if self.is_named:
            self._record.internal_hostname_named = (
                self._orchestrator_config.jobs_internal_domain_name_template.format(
                    job_id=self.host_segment_named
                )
            )

    @property
    def scheduler_enabled(self) -> bool:
        return self._scheduler_enabled

    @property
    def preemptible_node(self) -> bool:
        return self._preemptible_node

    @property
    def energy_schedule_name(self) -> str | None:
        if self.scheduler_enabled:
            return self._record.energy_schedule_name
        return None

    @property
    def pass_config(self) -> bool:
        return self._pass_config

    @property
    def restart_policy(self) -> JobRestartPolicy:
        return self._record.restart_policy

    @property
    def privileged(self) -> bool:
        return self._record.privileged

    @property
    def is_restartable(self) -> bool:
        return self._record.is_restartable

    @property
    def is_external(self) -> bool:
        preset = self.preset
        return False if preset is None else preset.is_external_job

    def get_run_time(
        self, only_after: datetime | None = None, now: datetime | None = None
    ) -> timedelta:
        def datetime_factory() -> datetime:
            if now:
                return now
            else:
                return self._current_datetime_factory()

        return self._record.get_run_time(
            only_after=only_after,
            current_datetime_factory=datetime_factory,
        )

    @property
    def max_run_time_minutes(self) -> int | None:
        return self._record.max_run_time_minutes

    def get_total_price_credits(self) -> Decimal:
        runtime = self.get_run_time()
        runtime_microseconds = int(runtime.total_seconds() * 1e6)
        runtime_hours = Decimal(runtime_microseconds) / int(1e6) / 3600
        return runtime_hours * self.price_credits_per_hour

    @property
    def org_name(self) -> str | None:
        return self._record.org_name

    @property
    def project_name(self) -> str:
        return self._record.project_name

    @property
    def org_project_hash(self) -> bytes:
        return self._record.org_project_hash

    @property
    def priority(self) -> JobPriority:
        return self._record.priority

    def to_primitive(self) -> dict[str, Any]:
        return self._record.to_primitive()

    @classmethod
    def from_primitive(
        cls,
        orchestrator_config: OrchestratorConfig,
        payload: dict[str, Any],
    ) -> Job:
        record = JobRecord.from_primitive(payload)
        return cls(
            orchestrator_config=orchestrator_config,
            record=record,
        )


def maybe_job_id(value: str) -> bool:
    """Check whether the string looks like a job id"""
    return value.startswith("job-")


def get_base_owner(value: str) -> str:
    return value.split("/", 1)[0]
