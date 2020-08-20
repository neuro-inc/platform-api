import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    AbstractSet,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    cast,
)

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.job_request import JobStatus


logger = logging.getLogger(__name__)


class JobsStorageException(Exception):
    pass


class JobStorageTransactionError(JobsStorageException):
    pass


class JobStorageJobFoundError(JobsStorageException):
    def __init__(self, job_name: str, job_owner: str, found_job_id: str):
        super().__init__(
            f"job with name '{job_name}' and owner '{job_owner}' "
            f"already exists: '{found_job_id}'"
        )


ClusterOwnerNameSet = Dict[str, Dict[str, AbstractSet[str]]]


@dataclass(frozen=True)
class JobFilter:
    statuses: AbstractSet[JobStatus] = field(
        default_factory=cast(Type[Set[JobStatus]], set)
    )
    clusters: ClusterOwnerNameSet = field(
        default_factory=cast(Type[ClusterOwnerNameSet], dict)
    )
    owners: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    tags: Set[str] = field(default_factory=cast(Type[Set[str]], set))
    name: Optional[str] = None
    ids: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    since: datetime = datetime(1, 1, 1, tzinfo=timezone.utc)
    until: datetime = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def check(self, job: JobRecord) -> bool:
        if self.statuses and job.status not in self.statuses:
            return False
        if self.owners and job.owner not in self.owners:
            return False
        if self.clusters:
            owners = self.clusters.get(job.cluster_name)
            if owners is None:
                return False
            if owners:
                names = owners.get(job.owner)
                if names is None:
                    return False
                if names and job.name not in names:
                    return False
        if self.name and self.name != job.name:
            return False
        if self.ids and job.id not in self.ids:
            return False
        if self.tags and not self.tags.issubset(job.tags):
            return False
        created_at = job.status_history.created_at
        if not self.since <= created_at <= self.until:
            return False
        return True


class JobsStorage(ABC):
    @abstractmethod
    def try_create_job(self, job: JobRecord) -> AsyncContextManager[JobRecord]:
        pass

    @abstractmethod
    async def set_job(self, job: JobRecord) -> None:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> JobRecord:
        pass

    @abstractmethod
    def try_update_job(self, job_id: str) -> AsyncContextManager[JobRecord]:
        pass

    @abstractmethod
    def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        pass

    @abstractmethod
    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        pass

    # Only used in tests
    async def get_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> List[JobRecord]:
        return [
            job
            async for job in self.iter_all_jobs(
                job_filter, reverse=reverse, limit=limit
            )
        ]

    # Only used in tests
    async def get_running_jobs(self) -> List[JobRecord]:
        filt = JobFilter(statuses={JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

    # Only used in tests
    async def get_unfinished_jobs(self) -> List[JobRecord]:
        filt = JobFilter(statuses={JobStatus.PENDING, JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

    @abstractmethod
    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        pass

    async def get_aggregated_run_time(self, job_filter: JobFilter) -> AggregatedRunTime:
        run_times = await self.get_aggregated_run_time_by_clusters(job_filter)
        gpu_run_time, non_gpu_run_time = timedelta(), timedelta()
        for run_time in run_times.values():
            gpu_run_time += run_time.total_gpu_run_time_delta
            non_gpu_run_time += run_time.total_non_gpu_run_time_delta
        return AggregatedRunTime(
            total_gpu_run_time_delta=gpu_run_time,
            total_non_gpu_run_time_delta=non_gpu_run_time,
        )

    @abstractmethod
    async def get_tags(self, owner: str) -> List[str]:
        pass

    @abstractmethod
    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        pass

    async def migrate(self) -> bool:
        return False
