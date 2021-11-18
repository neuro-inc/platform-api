import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Dict, Iterable, List, Optional, Tuple

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobError
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from .base import JobFilter, JobsStorage, JobStorageJobFoundError


class InMemoryJobsStorage(JobsStorage):
    def __init__(self) -> None:
        # job_id to job mapping:
        self._job_records: Dict[str, str] = {}
        # job_name+owner to job_id mapping:
        self._last_alive_job_records: Dict[Tuple[str, str], str] = {}
        # owner to job tags mapping:
        self._owner_to_tags: Dict[str, List[str]] = {}

    @asynccontextmanager
    async def try_create_job(self, job: JobRecord) -> AsyncIterator[JobRecord]:
        if job.name is not None:
            key = (job.owner, job.name)
            same_name_job_id = self._last_alive_job_records.get(key)
            if same_name_job_id is not None:
                same_name_job = await self.get_job(same_name_job_id)
                if not same_name_job.is_finished:
                    raise JobStorageJobFoundError(job.name, job.owner, same_name_job_id)
            self._last_alive_job_records[key] = job.id

        if job.tags:
            if job.owner not in self._owner_to_tags:
                self._owner_to_tags[job.owner] = []
            owner_tags = self._owner_to_tags[job.owner]
            for tag in sorted(job.tags, reverse=True):
                if tag in owner_tags:
                    owner_tags.remove(tag)
                owner_tags.insert(0, tag)

        yield job
        await self.set_job(job)

    async def set_job(self, job: JobRecord) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> JobRecord:
        return JobRecord.from_primitive(json.loads(payload))

    async def get_job(self, job_id: str) -> JobRecord:
        payload = self._job_records.get(job_id)
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    async def drop_job(self, job_id: str) -> None:
        payload = self._job_records.pop(job_id, None)
        if payload is None:
            raise JobError(f"no such job {job_id}")

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        job = await self.get_job(job_id)
        yield job
        await self.set_job(job)

    @asyncgeneratorcontextmanager
    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        # Accumulate results in a list to avoid RuntimeError when
        # the self._job_records dictionary is modified during iteration
        jobs = []
        for payload in self._job_records.values():
            job = self._parse_job_payload(payload)
            if job_filter and not job_filter.check(job):
                continue
            jobs.append(job)
        if reverse:
            jobs.reverse()
        if limit is not None:
            del jobs[limit:]
        for job in jobs:
            yield job

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        jobs = []
        for job_id in job_ids:
            try:
                job = await self.get_job(job_id)
            except JobError:
                # skipping missing
                continue
            if not job_filter or job_filter.check(job):
                jobs.append(job)
        return jobs

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        async with self.iter_all_jobs() as it:
            return [job async for job in it if job.should_be_deleted(delay=delay)]

    async def get_jobs_for_drop(
        self,
        *,
        delay: timedelta = timedelta(),
        limit: Optional[int] = None,
    ) -> List[JobRecord]:
        now = datetime.now(timezone.utc)
        jobs = []
        async with self.iter_all_jobs() as it:
            async for job in it:
                if (
                    not job.materialized
                    and job.is_finished
                    and job.finished_at
                    and job.finished_at + delay < now
                ):
                    jobs.append(job)
        if limit:
            jobs = jobs[:limit]
        return jobs
