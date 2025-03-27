import json
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobError
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from .base import JobFilter, JobsStorage, JobStorageJobFoundError


class InMemoryJobsStorage(JobsStorage):
    def __init__(self) -> None:
        # job_id to job mapping:
        self._job_records: dict[str, str] = {}
        # job_name+project to job_id mapping:
        self._last_alive_job_records: dict[tuple[str, str], str] = {}
        # project to job tags mapping:
        self._project_to_tags: dict[str, list[str]] = {}

    @asynccontextmanager
    async def try_create_job(self, job: JobRecord) -> AsyncIterator[JobRecord]:
        if job.name is not None:
            key = (job.project_name, job.name)
            same_name_job_id = self._last_alive_job_records.get(key)
            if same_name_job_id is not None:
                same_name_job = await self.get_job(same_name_job_id)
                if not same_name_job.is_finished:
                    raise JobStorageJobFoundError(
                        job.name, job.project_name, same_name_job_id
                    )
            self._last_alive_job_records[key] = job.id

        if job.tags:
            if job.project_name not in self._project_to_tags:
                self._project_to_tags[job.project_name] = []
            project_tags = self._project_to_tags[job.project_name]
            for tag in sorted(job.tags, reverse=True):
                if tag in project_tags:
                    project_tags.remove(tag)
                project_tags.insert(0, tag)

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
        job_filter: JobFilter | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
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
        self, job_ids: Iterable[str], job_filter: JobFilter | None = None
    ) -> list[JobRecord]:
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
    ) -> list[JobRecord]:
        async with self.iter_all_jobs() as it:
            return [job async for job in it if job.should_be_deleted(delay=delay)]

    async def get_jobs_for_drop(
        self,
        *,
        delay: timedelta = timedelta(),
        limit: int | None = None,
    ) -> list[JobRecord]:
        now = datetime.now(UTC)
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
