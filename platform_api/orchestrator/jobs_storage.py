import itertools
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import AbstractSet, AsyncIterator, Dict, List, Optional

import aioredis
from async_generator import asynccontextmanager

from .base import OrchestratorConfig
from .job import Job
from .job_request import JobError, JobStatus


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


@dataclass(frozen=True)
class JobFilter:
    statuses: AbstractSet[JobStatus]


class JobsStorage(ABC):
    @abstractmethod
    async def try_create_job(self, job: Job) -> AsyncIterator[Job]:
        pass

    @abstractmethod
    async def set_job(self, job: Job) -> None:
        pass

    @abstractmethod
    async def get_job(self, job_id: str) -> Job:
        pass

    @abstractmethod
    async def try_update_job(self, job_id: str) -> AsyncIterator[Job]:
        pass

    @abstractmethod
    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        pass

    async def get_running_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.is_running]

    async def get_jobs_for_deletion(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if job.should_be_deleted]

    async def get_unfinished_jobs(self) -> List[Job]:
        return [job for job in await self.get_all_jobs() if not job.is_finished]


class InMemoryJobsStorage(JobsStorage):
    def __init__(self, orchestrator_config: OrchestratorConfig) -> None:
        self._orchestrator_config = orchestrator_config

        self._job_records: Dict[str, str] = {}

    @asynccontextmanager
    async def try_create_job(self, job: Job) -> AsyncIterator[Job]:
        if job.name is not None:
            for record in self._job_records.values():
                stored_job = self._parse_job_payload(record)
                if (
                    stored_job.owner == stored_job.owner
                    and stored_job.name == stored_job.name
                    and not stored_job.is_finished
                ):
                    raise JobStorageJobFoundError(job.name, job.owner, stored_job.id)
        yield job
        await self.set_job(job)

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())
        self._job_records[job.id] = payload

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator_config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = self._job_records.get(job_id)
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[Job]:
        job = await self.get_job(job_id)
        yield job
        await self.set_job(job)

    def _apply_filter(self, job_filter: JobFilter, job: Job) -> bool:
        if job_filter.statuses and job.status not in job_filter.statuses:
            return False
        return True

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        jobs = []
        for payload in self._job_records.values():
            job = self._parse_job_payload(payload)
            if job_filter and not self._apply_filter(job_filter, job):
                continue
            jobs.append(job)
        return jobs


class RedisJobsStorage(JobsStorage):
    def __init__(
        self,
        client: aioredis.Redis,
        orchestrator_config: OrchestratorConfig,
        encoding: str = "utf8",
    ) -> None:
        self._client = client
        self._orchestrator_config = orchestrator_config
        # TODO (ajuszkowski 1-mar-2019) I think it's better to somehow get encoding
        # from redis configuration (client or server?), e.g. 'self._client.encoding'
        self._encoding = encoding

    def _decode(self, value: bytes) -> str:
        return value.decode(self._encoding)

    def _generate_job_key(self, job_id: str) -> str:
        return f"jobs:{job_id}"

    def _generate_jobs_status_index_key(self, status: JobStatus) -> str:
        return f"jobs.status.{status}"

    def _generate_alive_job_name_index_key(self, owner: str, job_name: str) -> str:
        return f"job-alive.name.{owner}.{job_name}"

    def _generate_jobs_deleted_index_key(self) -> str:
        return "jobs.deleted"

    def _generate_jobs_index_key(self) -> str:
        return "jobs"

    @asynccontextmanager
    async def _acquire_conn(self) -> AsyncIterator[aioredis.Redis]:
        pool = self._client.connection
        try:
            conn = await pool.acquire()
            yield aioredis.Redis(conn)
        finally:
            pool.release(conn)

    @asynccontextmanager
    async def _watch_job_id(self, job_id: str) -> AsyncIterator["RedisJobsStorage"]:
        key = self._generate_job_key(job_id)
        error_msg = f"Job with id='{job_id}' has been changed"
        async with self._watch_key(key, error_msg) as storage:
            yield storage

    @asynccontextmanager
    async def _watch_alive_job_name(
        self, owner: str, job_name: str
    ) -> AsyncIterator["RedisJobsStorage"]:
        key = self._generate_alive_job_name_index_key(owner, job_name)
        error_msg = f"Job with owner='{owner}', name='{job_name}' has been changed"
        async with self._watch_key(key, error_msg) as storage:
            yield storage

    @asynccontextmanager
    async def _watch_key(self, key: str, error_msg: str) -> AsyncIterator["RedisJobsStorage"]:
        async with self._acquire_conn() as client:
            try:
                await client.watch(key)

                yield type(self)(
                    client=client, orchestrator_config=self._orchestrator_config
                )
            except (aioredis.errors.MultiExecError, aioredis.errors.WatchVariableError):
                raise JobStorageTransactionError(error_msg)
            finally:
                await client.unwatch()

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[Job]:
        async with self._watch_job_id(job_id) as storage:
            job = await storage.get_job(job_id)
            yield job
            await storage.set_job(job)

    @asynccontextmanager
    async def try_create_job(self, job: Job) -> AsyncIterator[Job]:
        if job.name is not None:
            async with self._watch_alive_job_name(job.owner, job.name) as storage:
                another_job = await storage.get_alive_job_or_none(job.owner, job.name)
                if another_job is not None:
                    raise JobStorageJobFoundError(
                        another_job.name, another_job.owner, another_job.id
                    )
                yield job
                await storage.set_job(job)
        else:
            yield job
            await self.set_job(job)

    async def set_job(self, job: Job) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)
        tr.sadd("jobs", job.id)
        for status in JobStatus:
            tr.srem(self._generate_jobs_status_index_key(status), job.id)
        tr.sadd(self._generate_jobs_status_index_key(job.status), job.id)

        if job.name is not None:
            name_key = self._generate_alive_job_name_index_key(job.owner, job.name)
            if job.is_finished:
                tr.delete(name_key)
            else:
                tr.set(name_key, job.id)  # overwrites existing value

        if job.is_deleted:
            tr.sadd(self._generate_jobs_deleted_index_key(), job.id)
        await tr.execute()

    def _parse_job_payload(self, payload: str) -> Job:
        job_record = json.loads(payload)
        return Job.from_primitive(self._orchestrator_config, job_record)

    async def get_job(self, job_id: str) -> Job:
        payload = await self._client.get(self._generate_job_key(job_id))
        if payload is None:
            raise JobError(f"no such job {job_id}")
        return self._parse_job_payload(payload)

    async def get_alive_job_or_none(self, owner: str, job_name: str) -> Optional[Job]:
        key = self._generate_alive_job_name_index_key(owner, job_name)
        job_id_bytes = await self._client.get(key)
        if job_id_bytes is not None:
            job_id = self._decode(job_id_bytes)
            return await self.get_job(job_id)
        return None

    async def _get_jobs(self, ids: List[str]) -> List[Job]:
        jobs: List[Job] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        for payload in await self._client.mget(*keys):
            jobs.append(self._parse_job_payload(payload))
        return jobs

    async def _get_job_ids(self, statuses: AbstractSet[JobStatus]) -> List[str]:
        if statuses:
            keys = [self._generate_jobs_status_index_key(s) for s in statuses]
            return [job_id.decode() for job_id in await self._client.sunion(*keys)]
        else:
            key = self._generate_jobs_index_key()
            return [job_id.decode() async for job_id in self._client.isscan(key)]

    async def _get_job_ids_for_deletion(self) -> List[str]:
        tr = self._client.multi_exec()
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.FAILED),
            self._generate_jobs_deleted_index_key(),
        )
        tr.sdiff(
            self._generate_jobs_status_index_key(JobStatus.SUCCEEDED),
            self._generate_jobs_deleted_index_key(),
        )
        failed, succeeded = await tr.execute()
        return [id_.decode() for id_ in itertools.chain(failed, succeeded)]

    async def get_all_jobs(self, job_filter: Optional[JobFilter] = None) -> List[Job]:
        statuses = job_filter.statuses if job_filter else set()
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)

    async def get_running_jobs(self) -> List[Job]:
        statuses = {JobStatus.RUNNING}
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)

    async def get_jobs_for_deletion(self) -> List[Job]:
        job_ids = await self._get_job_ids_for_deletion()
        jobs = await self._get_jobs(job_ids)
        return [job for job in jobs if job.should_be_deleted]

    async def get_unfinished_jobs(self) -> List[Job]:
        statuses = {JobStatus.PENDING, JobStatus.RUNNING}
        job_ids = await self._get_job_ids(statuses)
        return await self._get_jobs(job_ids)
