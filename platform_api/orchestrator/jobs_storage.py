import asyncio
import itertools
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import timedelta
from typing import (
    AbstractSet,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)
from uuid import uuid4

import aioredis
from aioredis.commands import Pipeline
from async_generator import asynccontextmanager

from .job import AggregatedRunTime, JobRecord
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
    clusters: Dict[str, AbstractSet[str]] = field(
        default_factory=cast(Type[Dict[str, AbstractSet[str]]], dict)
    )
    owners: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))
    name: Optional[str] = None
    ids: AbstractSet[str] = field(default_factory=cast(Type[Set[str]], set))

    def check(self, job: JobRecord) -> bool:
        if self.statuses and job.status not in self.statuses:
            return False
        if self.owners and job.owner not in self.owners:
            return False
        if self.clusters:
            owners = self.clusters.get(job.cluster_name)
            if owners is None or (owners and job.owner not in owners):
                return False
        if self.name and self.name != job.name:
            return False
        if self.ids and job.id not in self.ids:
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
    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        pass

    @abstractmethod
    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        pass

    async def get_running_jobs(self) -> List[JobRecord]:
        filt = JobFilter(statuses={JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

    @abstractmethod
    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        pass

    async def get_unfinished_jobs(self) -> List[JobRecord]:
        filt = JobFilter(statuses={JobStatus.PENDING, JobStatus.RUNNING})
        return await self.get_all_jobs(filt)

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
    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        pass

    async def migrate(self) -> bool:
        return False


class InMemoryJobsStorage(JobsStorage):
    def __init__(self) -> None:
        # job_id to job mapping:
        self._job_records: Dict[str, str] = {}
        # job_name+owner to job_id mapping:
        self._last_alive_job_records: Dict[Tuple[str, str], str] = {}

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

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        job = await self.get_job(job_id)
        yield job
        await self.set_job(job)

    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        jobs = []
        for payload in self._job_records.values():
            job = self._parse_job_payload(payload)
            if job_filter and not job_filter.check(job):
                continue
            jobs.append(job)
        return jobs

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

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        jobs = await self.get_all_jobs(job_filter)
        zero_run_time = (timedelta(), timedelta())
        aggregated_run_times: Dict[str, Tuple[timedelta, timedelta]] = {}
        for job in jobs:
            gpu_run_time, non_gpu_run_time = aggregated_run_times.get(
                job.cluster_name, zero_run_time
            )
            if job.has_gpu:
                gpu_run_time += job.get_run_time()
            else:
                non_gpu_run_time += job.get_run_time()
            aggregated_run_times[job.cluster_name] = (
                gpu_run_time,
                non_gpu_run_time,
            )
        return {
            cluster_name: AggregatedRunTime(
                total_gpu_run_time_delta=gpu_run_time,
                total_non_gpu_run_time_delta=non_gpu_run_time,
            )
            for cluster_name, (
                gpu_run_time,
                non_gpu_run_time,
            ) in aggregated_run_times.items()
        }

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        return [
            job
            for job in await self.get_all_jobs()
            if job.should_be_deleted(delay=delay)
        ]


class RedisJobsStorage(JobsStorage):
    def __init__(self, client: aioredis.Redis, encoding: str = "utf8") -> None:
        self._client = client
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

    def _generate_jobs_cluster_index_key(self, cluster_name: str) -> str:
        return f"jobs.cluster.{cluster_name}"

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
        self, key: str, *other_keys: str, description: str
    ) -> AsyncIterator[JobsStorage]:
        async with self._acquire_conn() as client:
            try:
                await client.watch(key, *other_keys)

                yield type(self)(client=client)
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
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
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
        self, job: JobRecord, *, skip_index: bool = False
    ) -> AsyncIterator[JobRecord]:
        """ NOTE: this method yields the job, the same object as it came as an argument

        :param bool skip_index:
            Prevents indexing the job by owner and cluster for testing purposes.
        """
        if job.name:
            async with self._watch_all_job_keys(job.id, job.owner, job.name) as storage:
                other_id = await storage.get_last_created_job_id(job.owner, job.name)
                if other_id is not None:
                    other = await self.get_job(other_id)
                    assert other.name is not None
                    if not other.is_finished:
                        raise JobStorageJobFoundError(other.name, other.owner, other_id)
                # with yield below, the job creation signal is sent to the orchestrator.
                # Thus, if a job with the same name already exists, the appropriate
                # exception is thrown before sending this signal to the orchestrator.
                yield job
                # after the orchestrator has started the job, it fills the 'job' object
                # with some new values, so we write the new value of 'job' to Redis:
                await storage.update_job_atomic(
                    job, is_job_creation=True, skip_index=skip_index
                )
        else:
            async with self._watch_job_id_key(job.id) as storage:
                yield job
                await storage.update_job_atomic(
                    job, is_job_creation=True, skip_index=skip_index
                )

    async def set_job(self, job: JobRecord) -> None:
        # TODO (ajuszkowski, 4-feb-2019) remove this method from interface as well
        # because it does not watch keys that it updates AND it does not update
        # the 'last-job-created' key!
        await self.update_job_atomic(job, is_job_creation=True)

    def _update_owner_index(self, tr: Pipeline, job: JobRecord) -> None:
        owner_key = self._generate_jobs_owner_index_key(job.owner)
        score = job.status_history.created_at_timestamp
        tr.zadd(owner_key, score, job.id, exist=tr.ZSET_IF_NOT_EXIST)

    def _update_cluster_index(self, tr: Pipeline, job: JobRecord) -> None:
        cluster_key = self._generate_jobs_cluster_index_key(job.cluster_name)
        score = job.status_history.created_at_timestamp
        tr.zadd(cluster_key, score, job.id, exist=tr.ZSET_IF_NOT_EXIST)

    def _update_name_index(self, tr: Pipeline, job: JobRecord) -> None:
        assert job.name
        name_key = self._generate_jobs_name_index_zset_key(job.owner, job.name)
        tr.zadd(name_key, job.status_history.created_at_timestamp, job.id)

    async def update_job_atomic(
        self, job: JobRecord, *, is_job_creation: bool, skip_index: bool = False
    ) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)

        for status in JobStatus:
            tr.srem(self._generate_jobs_status_index_key(status), job.id)
        tr.sadd(self._generate_jobs_status_index_key(job.status), job.id)

        if is_job_creation:
            tr.sadd(self._generate_jobs_index_key(), job.id)
            if not skip_index:
                self._update_owner_index(tr, job)
                self._update_cluster_index(tr, job)
            if job.name:
                self._update_name_index(tr, job)

        if job.is_deleted:
            tr.sadd(self._generate_jobs_deleted_index_key(), job.id)
        await tr.execute()

    def _parse_job_payload(self, payload: str) -> JobRecord:
        return JobRecord.from_primitive(json.loads(payload))

    async def get_job(self, job_id: str) -> JobRecord:
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

    async def _get_jobs(self, ids: Iterable[str]) -> List[JobRecord]:
        jobs: List[JobRecord] = []
        if not ids:
            return jobs
        keys = [self._generate_job_key(id_) for id_ in ids]
        payloads = await self._client.mget(*keys)
        for chunk in self._iterate_in_chunks(payloads, chunk_size=10):
            jobs.extend(
                self._parse_job_payload(payload) for payload in chunk if payload
            )
            await asyncio.sleep(0.0)
        return jobs

    def _iterate_in_chunks(self, payloads: List[Any], chunk_size: int) -> Iterator[Any]:
        # in case there are lots of jobs to retrieve, the parsing code below
        # blocks the concurrent execution for significant amount of time.
        # to mitigate the issue, we call `asyncio.sleep` to let other
        # coroutines execute too.
        return itertools.zip_longest(*([iter(payloads)] * chunk_size))

    async def _get_job_ids(
        self,
        *,
        statuses: Iterable[JobStatus],
        clusters: Iterable[str],
        owners: Iterable[str],
        name: Optional[str] = None,
    ) -> List[str]:
        if name:
            owner_keys = [
                self._generate_jobs_name_index_zset_key(owner, name) for owner in owners
            ]
            if not owner_keys:
                raise JobsStorageException(
                    "filtering jobs by name is allowed only together with owners"
                )
        else:
            owner_keys = [
                self._generate_jobs_owner_index_key(owner) for owner in owners
            ]
        cluster_keys = [
            self._generate_jobs_cluster_index_key(cluster) for cluster in clusters
        ]
        status_keys = [self._generate_jobs_status_index_key(s) for s in statuses]

        temp_key = self._generate_temp_zset_key()
        tr = self._client.multi_exec()

        index_keys = owner_keys or cluster_keys
        if index_keys:
            tr.zunionstore(temp_key, *index_keys, aggregate=tr.ZSET_AGGREGATE_MAX)
            if owner_keys and cluster_keys:
                cluster_temp_key = self._generate_temp_zset_key()
                tr.zunionstore(
                    cluster_temp_key, *cluster_keys, aggregate=tr.ZSET_AGGREGATE_MAX
                )
                tr.zinterstore(
                    temp_key,
                    temp_key,
                    cluster_temp_key,
                    aggregate=tr.ZSET_AGGREGATE_MAX,
                )
                tr.delete(cluster_temp_key)
            if status_keys:
                status_temp_key = self._generate_temp_zset_key()
                tr.zunionstore(
                    status_temp_key, *status_keys, aggregate=tr.ZSET_AGGREGATE_MAX
                )
                tr.zinterstore(
                    temp_key, temp_key, status_temp_key, aggregate=tr.ZSET_AGGREGATE_MAX
                )
                tr.delete(status_temp_key)
            tr.zrange(temp_key)
        else:
            status_keys = status_keys or [self._generate_jobs_index_key()]
            tr.sunion(*status_keys)

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

    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        if not job_filter:
            job_filter = JobFilter()
        elif job_filter.ids:
            return await self.get_jobs_by_ids(job_filter.ids, job_filter)
        job_ids = await self._get_job_ids(
            statuses=job_filter.statuses,
            clusters=job_filter.clusters,
            owners=job_filter.owners,
            name=job_filter.name,
        )
        jobs = await self._get_jobs(job_ids)
        if any(job_filter.clusters.values()):
            jobs = [job for job in jobs if job_filter.check(job)]
        return jobs

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        jobs = await self._get_jobs(job_ids)
        if job_filter:
            jobs = [job for job in jobs if job_filter.check(job)]
        return jobs

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        job_ids = await self._get_job_ids_for_deletion()
        jobs = await self._get_jobs(job_ids)
        return [job for job in jobs if job.should_be_deleted(delay=delay)]

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        # NOTE (ajuszkowski 4-Apr-2019): because of possible high number of jobs
        # submitted by a user, we need to process all job separately iterating
        # by job-ids not by job objects in order not to store them all in memory
        jobs_ids = await self._get_job_ids(
            statuses=job_filter.statuses,
            clusters=job_filter.clusters,
            owners=job_filter.owners,
            name=job_filter.name,
        )

        zero_run_time = (timedelta(), timedelta())
        aggregated_run_times: Dict[str, Tuple[timedelta, timedelta]] = {}
        for job_id_chunk in self._iterate_in_chunks(jobs_ids, chunk_size=10):
            keys = [self._generate_job_key(job_id) for job_id in job_id_chunk if job_id]
            jobs = [
                self._parse_job_payload(payload)
                for payload in await self._client.mget(*keys)
            ]
            if any(job_filter.clusters.values()):
                jobs = [job for job in jobs if job_filter.check(job)]
            for job in jobs:
                gpu_run_time, non_gpu_run_time = aggregated_run_times.get(
                    job.cluster_name, zero_run_time
                )
                if job.has_gpu:
                    gpu_run_time += job.get_run_time()
                else:
                    non_gpu_run_time += job.get_run_time()
                aggregated_run_times[job.cluster_name] = (
                    gpu_run_time,
                    non_gpu_run_time,
                )

        return {
            cluster_name: AggregatedRunTime(
                total_gpu_run_time_delta=gpu_run_time,
                total_non_gpu_run_time_delta=non_gpu_run_time,
            )
            for cluster_name, (
                gpu_run_time,
                non_gpu_run_time,
            ) in aggregated_run_times.items()
        }

    async def migrate(self) -> bool:
        version = int(await self._client.get("version") or "0")
        if version < 1:
            await self._reindex_job_owners()
        if version < 3:
            await self._update_job_cluster_names()
            await self._reindex_job_clusters()
        else:
            return False
        await self._client.set("version", "3")
        return True

    async def _reindex_job_owners(self) -> None:
        logger.info("Starting reindexing job owners")

        tr = self._client.pipeline()
        async for job in self._iter_all_jobs():
            self._update_owner_index(tr, job)
        await tr.execute()

        logger.info("Finished reindexing job owners")

    async def _reindex_job_clusters(self) -> None:
        logger.info("Starting reindexing job clusters")

        tr = self._client.pipeline()
        async for job in self._iter_all_jobs():
            self._update_cluster_index(tr, job)
        await tr.execute()

        logger.info("Finished reindexing job clusters")

    async def _update_job_cluster_names(self) -> None:
        logger.info("Starting updating job cluster names")

        total = changed = 0
        tr = self._client.pipeline()
        async for job in self._iter_all_jobs():
            total += 1
            if job.cluster_name:
                continue
            changed += 1
            job.cluster_name = "default"
            payload = json.dumps(job.to_primitive())
            tr.set(self._generate_job_key(job.id), payload)
        await tr.execute()

        logger.info(f"Finished updating job cluster names ({changed}/{total})")

    async def _iter_all_jobs(self) -> AsyncIterator[JobRecord]:
        jobs_key = self._generate_jobs_index_key()
        async for job_id in self._client.isscan(jobs_key):
            job = await self.get_job(job_id.decode())
            yield job
