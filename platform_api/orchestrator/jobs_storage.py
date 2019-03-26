import itertools
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    AbstractSet,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    cast,
)
from uuid import uuid4

import aioredis
from aioredis.commands import Pipeline
from async_generator import asynccontextmanager

from .base import OrchestratorConfig
from .job import Job
from .job_request import JobError, JobStatus


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


@dataclass(frozen=True)
class JobFilter:
    statuses: AbstractSet[JobStatus] = field(
        default_factory=cast(Type[Set[JobStatus]], set)
    )
    owners: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    name: Optional[str] = None


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

    async def migrate(self) -> None:
        pass


class InMemoryJobsStorage(JobsStorage):
    def __init__(self, orchestrator_config: OrchestratorConfig) -> None:
        self._orchestrator_config = orchestrator_config
        # job_id to job mapping:
        self._job_records: Dict[str, str] = {}
        # job_name+owner to job_id mapping:
        self._last_alive_job_records: Dict[Tuple[str, str], str] = {}

    @asynccontextmanager
    async def try_create_job(self, job: Job) -> AsyncIterator[Job]:
        if job.name is not None:
            key = (job.owner, job.name)
            same_name_job_id = self._last_alive_job_records.get(key)
            if same_name_job_id is not None:
                same_name_job = await self.get_job(same_name_job_id)
                if not same_name_job.is_finished:
                    raise JobStorageJobFoundError(job.name, job.owner, same_name_job_id)
            self._last_alive_job_records[key] = job.id
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
        if job_filter.owners and job.owner not in job_filter.owners:
            return False
        if job_filter.name and job_filter.name != job.name:
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

    def _generate_jobs_owner_index_key(self, owner: str) -> str:
        return f"jobs.owner.{owner}"

    def _generate_jobs_name_index_zset_key(self, owner: str, job_name: str) -> str:
        assert owner, "job owner is not defined"
        assert job_name, "job name is not defined"
        return f"jobs.name.{owner}.{job_name}"

    def _generate_jobs_deleted_index_key(self) -> str:
        return "jobs.deleted"

    def _generate_jobs_index_key(self) -> str:
        return "jobs"

    def _generate_temp_zset_key(self) -> str:
        """ Temporary index used for storing the result of operations over
        Z-sets (union, intersection)
        """
        return f"temp_zset_{uuid4()}"

    @asynccontextmanager
    async def _acquire_conn(self) -> AsyncIterator[aioredis.Redis]:
        pool = self._client.connection
        conn = None
        try:
            conn = await pool.acquire()
            yield aioredis.Redis(conn)
        finally:
            if conn:
                pool.release(conn)

    @asynccontextmanager
    async def _watch_keys(
        self, key: str, *other_keys: Sequence[str], description: str
    ) -> AsyncIterator[JobsStorage]:
        async with self._acquire_conn() as client:
            try:
                await client.watch(key, *other_keys)

                yield type(self)(
                    client=client, orchestrator_config=self._orchestrator_config
                )
            except (aioredis.errors.MultiExecError, aioredis.errors.WatchVariableError):
                raise JobStorageTransactionError(
                    "Job {" + description + "} has changed"
                )
            finally:
                await client.unwatch()

    @asynccontextmanager
    async def _watch_all_job_keys(
        self, job_id: str, owner: str, job_name: str
    ) -> AsyncIterator[JobsStorage]:
        id_key = self._generate_job_key(job_id)
        name_key = self._generate_jobs_name_index_zset_key(owner, job_name)
        desc = f"id={job_id}, owner={owner}, name={job_name}"
        async with self._watch_keys(id_key, name_key, description=desc) as storage:
            yield storage

    @asynccontextmanager
    async def _watch_job_id_key(self, job_id: str) -> AsyncIterator[JobsStorage]:
        id_key = self._generate_job_key(job_id)
        async with self._watch_keys(id_key, description=f"id={job_id}") as storage:
            yield storage

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[Job]:
        """ NOTE: this method yields the job retrieved from the database
        """
        async with self._watch_job_id_key(job_id) as storage:
            # NOTE: this method does not need to WATCH the job-last-created key as it
            # does not rely on this key
            job = await storage.get_job(job_id)
            yield job
            await storage.update_job_atomic(job, is_job_creation=False)

    @asynccontextmanager
    async def try_create_job(
        self, job: Job, skip_owner_index: bool = False
    ) -> AsyncIterator[Job]:
        """ NOTE: this method yields the job, the same object as it came as an argument

        :param bool skip_owner_index:
            Prevents indexing the job by owner for testing purposes.
        """
        if job.name:
            async with self._watch_all_job_keys(job.id, job.owner, job.name) as storage:
                other_id = await storage.get_last_created_job_id(job.owner, job.name)
                if other_id is not None:
                    other = await self.get_job(other_id)
                    if not other.is_finished:
                        raise JobStorageJobFoundError(other.name, other.owner, other_id)
                # with yield below, the job creation signal is sent to the orchestrator.
                # Thus, if a job with the same name already exists, the appropriate
                # exception is thrown before sending this signal to the orchestrator.
                yield job
                # after the orchestrator has started the job, it fills the 'job' object
                # with some new values, so we write the new value of 'job' to Redis:
                await storage.update_job_atomic(
                    job, is_job_creation=True, skip_owner_index=skip_owner_index
                )
        else:
            async with self._watch_job_id_key(job.id) as storage:
                yield job
                await storage.update_job_atomic(
                    job, is_job_creation=True, skip_owner_index=skip_owner_index
                )

    async def set_job(self, job: Job) -> None:
        # TODO (ajuszkowski, 4-feb-2019) remove this method from interface as well
        # because it does not watch keys that it updates AND it does not update
        # the 'last-job-created' key!
        await self.update_job_atomic(job, is_job_creation=True)

    def _update_owner_index(self, tr: Pipeline, job: Job) -> None:
        owner_key = self._generate_jobs_owner_index_key(job.owner)
        tr.zadd(
            owner_key,
            job.status_history.created_at_timestamp,
            job.id,
            exist=tr.ZSET_IF_NOT_EXIST,
        )

    def _update_name_index(self, tr: Pipeline, job: Job) -> None:
        name_key = self._generate_jobs_name_index_zset_key(job.owner, job.name)
        tr.zadd(name_key, job.status_history.created_at_timestamp, job.id)

    async def update_job_atomic(
        self, job: Job, is_job_creation: bool, skip_owner_index: bool = False
    ) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)

        for status in JobStatus:
            tr.srem(self._generate_jobs_status_index_key(status), job.id)
        tr.sadd(self._generate_jobs_status_index_key(job.status), job.id)

        if is_job_creation:
            tr.sadd(self._generate_jobs_index_key(), job.id)
            if not skip_owner_index:
                self._update_owner_index(tr, job)
            if job.name:
                self._update_name_index(tr, job)

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

    async def get_last_created_job_id(self, owner: str, job_name: str) -> Optional[str]:
        job_ids_key = self._generate_jobs_name_index_zset_key(owner, job_name)
        last_job_id_singleton = await self._client.zrange(
            job_ids_key, start=-1, stop=-1
        )
        if last_job_id_singleton:
            last_job_id = self._decode(last_job_id_singleton[0])
            return last_job_id
        return None

    async def _get_jobs(self, ids: List[str]) -> List[Job]:
        jobs: List[Job] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        for payload in await self._client.mget(*keys):
            jobs.append(self._parse_job_payload(payload))
        return jobs

    async def _get_job_ids(
        self,
        statuses: AbstractSet[JobStatus],
        owners: Optional[AbstractSet[str]] = None,
        name: Optional[str] = None,
    ) -> List[str]:
        owners = owners or set()
        if name and not owners:
            raise ValueError(
                "filtering jobs by name is allowed only together with owners"
            )

        status_keys = [self._generate_jobs_status_index_key(s) for s in statuses]
        status_keys = status_keys or [self._generate_jobs_index_key()]

        owner_keys = [
            self._generate_jobs_name_index_zset_key(owner, name)
            if name
            else self._generate_jobs_owner_index_key(owner)
            for owner in owners
        ]

        temp_key = self._generate_temp_zset_key()

        tr = self._client.multi_exec()
        tr.zunionstore(temp_key, *status_keys, aggregate=tr.ZSET_AGGREGATE_MAX)
        if owner_keys:
            owners_temp_key = self._generate_temp_zset_key()
            tr.zunionstore(
                owners_temp_key, *owner_keys, aggregate=tr.ZSET_AGGREGATE_MAX
            )
            tr.zinterstore(
                temp_key, owners_temp_key, temp_key, aggregate=tr.ZSET_AGGREGATE_MAX
            )
            tr.delete(owners_temp_key)
        tr.zrange(temp_key)
        tr.delete(temp_key)
        *_, payloads, _ = await tr.execute()

        return [job_id.decode() for job_id in payloads]

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
        if not job_filter:
            job_filter = JobFilter()
        job_ids = await self._get_job_ids(
            statuses=job_filter.statuses, owners=job_filter.owners, name=job_filter.name
        )
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

    async def migrate(self) -> None:
        await self.reindex_job_owners()

    async def reindex_job_owners(self) -> int:
        logger.info("Starting reindexing job owners")

        job_ids = await self._get_job_ids_unindexed_owner()
        for job_id in job_ids:
            job = await self.get_job(job_id)
            tr = self._client.pipeline()
            self._update_owner_index(tr, job)
            await tr.execute()

        logger.info("Finished reindexing job owners")
        return len(job_ids)

    async def _get_job_ids_unindexed_owner(self) -> Set[str]:
        jobs_key = self._generate_jobs_index_key()
        job_ids = {job_id.decode() async for job_id in self._client.isscan(jobs_key)}
        job_ids -= await self._get_job_ids_indexed_owner()

        logger.info(f"Found {len(job_ids)} jobs with unindexed owners")
        return job_ids

    async def _get_job_owner_keys(self) -> List[str]:
        owner_index_key_template = self._generate_jobs_owner_index_key("*")
        return [key async for key in self._client.iscan(match=owner_index_key_template)]

    async def _get_job_ids_indexed_owner(self) -> Set[str]:
        owners_keys = await self._get_job_owner_keys()
        logger.info(f"Found {len(owners_keys)} job owner index keys")
        if not owners_keys:
            return set()

        temp_key = self._generate_temp_zset_key()
        tr = self._client.multi_exec()
        tr.zunionstore(temp_key, *owners_keys)
        tr.zrange(temp_key)
        tr.delete(temp_key)
        *_, job_ids, _ = await tr.execute()
        return {job_id.decode() for job_id in job_ids}
