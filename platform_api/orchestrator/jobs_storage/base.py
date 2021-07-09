import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (
    AbstractSet,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Mapping,
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
    base_owners: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    tags: Set[str] = field(default_factory=cast(Type[Set[str]], set))
    name: Optional[str] = None
    ids: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    since: datetime = datetime(1, 1, 1, tzinfo=timezone.utc)
    until: datetime = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    materialized: Optional[bool] = None
    fully_billed: Optional[bool] = None

    def check(self, job: JobRecord) -> bool:
        if self.statuses and job.status not in self.statuses:
            return False
        if self.owners and job.owner not in self.owners:
            return False
        if self.base_owners and job.base_owner not in self.base_owners:
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
        if self.materialized is not None:
            return self.materialized == job.materialized
        if self.fully_billed is not None:
            return self.fully_billed == job.fully_billed
        return True


@dataclass
class RunTimeEntry:
    """Helper class for calculations"""

    gpu_run_time: timedelta = timedelta()
    non_gpu_run_time: timedelta = timedelta()

    @classmethod
    def for_job(cls, job: JobRecord) -> "RunTimeEntry":
        if job.has_gpu:
            return cls(gpu_run_time=job.get_run_time())
        else:
            return cls(non_gpu_run_time=job.get_run_time())

    def to_aggregated_run_time(self) -> "AggregatedRunTime":
        return AggregatedRunTime(
            total_gpu_run_time_delta=self.gpu_run_time,
            total_non_gpu_run_time_delta=self.non_gpu_run_time,
        )

    def to_primitive(self) -> Mapping[str, float]:
        return {
            "gpu_run_time": self.gpu_run_time.total_seconds(),
            "non_gpu_run_time": self.non_gpu_run_time.total_seconds(),
        }

    @classmethod
    def from_primitive(cls, data: Mapping[str, float]) -> "RunTimeEntry":
        return cls(
            gpu_run_time=timedelta(seconds=data["gpu_run_time"]),
            non_gpu_run_time=timedelta(seconds=data["non_gpu_run_time"]),
        )

    def increase_by(self, other: "RunTimeEntry") -> None:
        self.gpu_run_time += other.gpu_run_time
        self.non_gpu_run_time += other.non_gpu_run_time


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
    async def drop_job(self, job_id: str) -> None:
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
    ) -> AsyncContextManager[AsyncIterator[JobRecord]]:
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
        async with self.iter_all_jobs(job_filter, reverse=reverse, limit=limit) as it:
            return [job async for job in it]

    # Only used in tests
    async def get_running_jobs(self) -> List[JobRecord]:
        filt = JobFilter(statuses={JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

    # Only used in tests
    async def get_unfinished_jobs(self) -> List[JobRecord]:
        filt = JobFilter(
            statuses={JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUSPENDED}
        )
        return await self.get_all_jobs(filt)

    @abstractmethod
    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        pass

    async def get_aggregated_run_time(self, owner: str) -> AggregatedRunTime:
        run_times = await self.get_aggregated_run_time_by_clusters(owner)
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

    async def get_aggregated_run_time_by_clusters(
        self, owner: str
    ) -> Dict[str, AggregatedRunTime]:
        aggregated_run_times: Dict[str, RunTimeEntry] = defaultdict(RunTimeEntry)
        async with self.iter_all_jobs(JobFilter(owners={owner})) as it:
            async for job in it:
                aggregated_run_times[job.cluster_name].increase_by(
                    RunTimeEntry.for_job(job)
                )
        return {
            cluster_name: run_time_entry.to_aggregated_run_time()
            for cluster_name, run_time_entry in aggregated_run_times.items()
        }
