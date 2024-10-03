import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable, Set
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional, Union, cast

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobStatus

logger = logging.getLogger(__name__)


class JobsStorageException(Exception):
    pass


class JobStorageTransactionError(JobsStorageException):
    pass


class JobStorageJobFoundError(JobsStorageException):
    def __init__(self, job_name: str, project_name: str, found_job_id: str):
        super().__init__(
            f"job with name '{job_name}' and project '{project_name}' "
            f"already exists: '{found_job_id}'"
        )


ClusterOrgProjectNameSet = dict[str, dict[Optional[str], dict[str, Set[str]]]]


@dataclass(frozen=True)
class JobFilter:
    statuses: Set[JobStatus] = field(default_factory=cast(type[Set[JobStatus]], set))
    clusters: ClusterOrgProjectNameSet = field(
        default_factory=cast(type[ClusterOrgProjectNameSet], dict)
    )
    orgs: Set[Optional[str]] = field(
        default_factory=cast(type[Set[Optional[str]]], set)
    )
    owners: Set[str] = field(default_factory=cast(type[Set[str]], set))
    projects: Set[str] = field(default_factory=cast(type[Set[str]], set))
    base_owners: Set[str] = field(default_factory=cast(type[Set[str]], set))
    tags: Set[str] = field(default_factory=cast(type[Set[str]], set))
    name: Optional[str] = None
    ids: Set[str] = field(default_factory=cast(type[Set[str]], set))
    since: datetime = datetime(1, 1, 1, tzinfo=timezone.utc)
    until: datetime = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    materialized: Optional[bool] = None
    being_dropped: Optional[bool] = None
    logs_removed: Optional[bool] = None
    org_project_hash: Union[bytes, str, None] = None

    def check(self, job: JobRecord) -> bool:
        if self.statuses and job.status not in self.statuses:
            return False
        if self.owners and job.owner not in self.owners:
            return False
        if self.base_owners and job.base_owner not in self.base_owners:
            return False
        if self.projects and job.project_name not in self.projects:
            return False
        if self.clusters:
            orgs = self.clusters.get(job.cluster_name)
            if orgs is None:
                return False
            if orgs:
                projects = orgs.get(job.org_name)
                if projects is None:
                    return False
                if projects:
                    names = projects.get(job.project_name)
                    if names is None:
                        return False
                    if names and job.name not in names:
                        return False
        if self.name and self.name != job.name:
            return False
        if self.ids and job.id not in self.ids:
            return False
        if self.tags and not self.tags <= set(job.tags):
            return False
        created_at = job.status_history.created_at
        if not self.since <= created_at <= self.until:
            return False
        if self.materialized is not None:
            return self.materialized == job.materialized
        if self.being_dropped is not None:
            return self.being_dropped == job.being_dropped
        if self.logs_removed is not None:
            return self.logs_removed == job.logs_removed
        if self.org_project_hash is not None:
            return self.org_project_hash == job.org_project_hash
        return True


class JobsStorage(ABC):
    @abstractmethod
    def try_create_job(self, job: JobRecord) -> AbstractAsyncContextManager[JobRecord]:
        pass

    @abstractmethod
    async def set_job(self, job: JobRecord) -> None:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> JobRecord:
        pass

    @abstractmethod
    async def drop_job(self, job_id: str) -> None:
        pass

    @abstractmethod
    def try_update_job(self, job_id: str) -> AbstractAsyncContextManager[JobRecord]:
        pass

    @abstractmethod
    def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[JobRecord]]:
        pass

    @abstractmethod
    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> list[JobRecord]:
        pass

    # Only used in tests
    async def get_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> list[JobRecord]:
        async with self.iter_all_jobs(job_filter, reverse=reverse, limit=limit) as it:
            return [job async for job in it]

    # Only used in tests
    async def get_running_jobs(self) -> list[JobRecord]:
        filt = JobFilter(statuses={JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

    # Only used in tests
    async def get_unfinished_jobs(self) -> list[JobRecord]:
        filt = JobFilter(
            statuses={JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUSPENDED}
        )
        return await self.get_all_jobs(filt)

    @abstractmethod
    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> list[JobRecord]:
        pass

    @abstractmethod
    async def get_jobs_for_drop(
        self, *, delay: timedelta = timedelta(), limit: Optional[int] = None
    ) -> list[JobRecord]:
        pass
