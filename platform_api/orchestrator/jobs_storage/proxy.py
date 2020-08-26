import dataclasses
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
)

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.jobs_storage import JobFilter

from .base import JobsStorage


logger = logging.getLogger(__name__)


class ProxyJobStorage(JobsStorage):
    """Proxy JobStorage for passing calls to real storages

    Always has single primary JobStorage and, in addition to it, can have
    any number for secondary storages. Write calls are passed to all storages,
    but result only returned from primary one. Read calls are only passed
    to primary storage.

    Main goal of this class to allow smooth transition from one storage
    engine to another by maintaining same data in storages.
    """

    _primary_storage: JobsStorage
    _secondary_storages: List[JobsStorage]

    def __init__(
        self, primary_storage: JobsStorage, secondary_storages: Iterable[JobsStorage]
    ):
        self._primary_storage = primary_storage
        self._secondary_storages = list(secondary_storages)

    @asynccontextmanager
    async def _pass_through_job_context_manager(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> AsyncIterator[JobRecord]:
        primary_manager = getattr(self._primary_storage, method_name)(*args, **kwargs)
        async with primary_manager as primary_job:
            yield primary_job
        for secondary in self._secondary_storages:
            manager = getattr(secondary, method_name)(*args, **kwargs)
            async with manager as secondary_job:
                # Sync up secondary job with primary
                for field in dataclasses.fields(secondary_job):
                    setattr(secondary_job, field.name, getattr(primary_job, field.name))

    async def set_job(self, job: JobRecord) -> None:
        # Order here is important: if primary raises something,
        # we should not touch secondary
        await self._primary_storage.set_job(job)
        for secondary in self._secondary_storages:
            await secondary.set_job(job)

    async def get_job(self, job_id: str) -> JobRecord:
        return await self._primary_storage.get_job(job_id)

    def try_create_job(self, job: JobRecord) -> AsyncContextManager[JobRecord]:
        return self._pass_through_job_context_manager("try_create_job", job)

    def try_update_job(self, job_id: str) -> AsyncContextManager[JobRecord]:
        return self._pass_through_job_context_manager("try_update_job", job_id)

    def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        return self._primary_storage.iter_all_jobs(
            job_filter, reverse=reverse, limit=limit
        )

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        return await self._primary_storage.get_jobs_by_ids(job_ids, job_filter)

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        return await self._primary_storage.get_jobs_for_deletion(delay=delay)

    async def get_tags(self, owner: str) -> List[str]:
        return await self._primary_storage.get_tags(owner=owner)

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        return await self._primary_storage.get_aggregated_run_time_by_clusters(
            job_filter
        )
