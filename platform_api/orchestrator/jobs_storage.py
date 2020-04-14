import heapq
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import chain, groupby, islice
from operator import itemgetter
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

from platform_api.trace import trace

from .job import AggregatedRunTime, JobRecord
from .job_request import JobError, JobStatus


logger = logging.getLogger(__name__)


JOBS_CHUNK_SIZE = 1000


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
            if owners is None or (owners and job.owner not in owners):
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
        self, job_filter: Optional[JobFilter] = None, *, reverse: bool = False
    ) -> AsyncIterator[JobRecord]:
        pass

    @abstractmethod
    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        pass

    # Only used in tests
    async def get_all_jobs(
        self, job_filter: Optional[JobFilter] = None, reverse: bool = False
    ) -> List[JobRecord]:
        return [job async for job in self.iter_all_jobs(job_filter, reverse=reverse)]

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

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        job = await self.get_job(job_id)
        yield job
        await self.set_job(job)

    async def iter_all_jobs(
        self, job_filter: Optional[JobFilter] = None, *, reverse: bool = False
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

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        zero_run_time = (timedelta(), timedelta())
        aggregated_run_times: Dict[str, Tuple[timedelta, timedelta]] = {}
        async for job in self.iter_all_jobs(job_filter):
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
            async for job in self.iter_all_jobs()
            if job.should_be_deleted(delay=delay)
        ]

    async def get_tags(self, owner: str) -> List[str]:
        return self._owner_to_tags.get(owner, [])


class RedisJobsStorage(JobsStorage):
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    def _generate_job_key(self, job_id: str) -> str:
        return f"jobs:{job_id}"

    def _generate_jobs_name_index_zset_key(self, owner: str, job_name: str) -> str:
        assert owner, "job owner is not defined"
        assert job_name, "job name is not defined"
        return f"jobs.name.{owner}.{job_name}"

    def _generate_tags_owner_index_zset_key(self, owner: str) -> str:
        return f"tags.owner.{owner}"

    def _generate_jobs_for_deletion_index_key(self) -> str:
        return f"jobs.for-deletion"

    def _generate_jobs_index_key(self) -> str:
        return "jobs"

    def _generate_temp_zset_key(self) -> str:
        """ Temporary index used for storing the result of operations over
        Z-sets (union, intersection)
        """
        return f"temp_zset_{uuid4()}"

    def _generate_jobs_composite_keys(
        self,
        *,
        statuses: Iterable[str],
        clusters: Dict[str, AbstractSet[str]],
        owners: Iterable[str],
        names: Iterable[str],
        tags: Iterable[str],
    ) -> List[str]:
        return [
            f"jobs.comp.{status}|{cluster}|{owner}|{name}|{tag}"
            for cluster, cluster_owners in clusters.items()
            for owner in cluster_owners or owners
            for status in statuses
            for name in names
            for tag in tags
        ]

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

    def _update_name_index(self, tr: Pipeline, job: JobRecord) -> None:
        assert job.name
        name_key = self._generate_jobs_name_index_zset_key(job.owner, job.name)
        tr.zadd(name_key, job.status_history.created_at_timestamp, job.id)

    def _update_tags_indexes(self, tr: Pipeline, job: JobRecord) -> None:
        for tag in job.tags:
            # use negative score + ZRANGE instead of positive score + ZREVRANGE
            # so that tags submitted with a single job go in lexicographic order
            neg_score = -job.status_history.created_at_timestamp
            tr.zadd(self._generate_tags_owner_index_zset_key(job.owner), neg_score, tag)

    def _update_for_deletion_index(self, tr: Pipeline, job: JobRecord) -> None:
        index_key = self._generate_jobs_for_deletion_index_key()
        if job.is_finished:
            if job.is_deleted:
                tr.srem(index_key, job.id)
            else:
                tr.sadd(index_key, job.id)

    def _update_composite_index(self, tr: Pipeline, job: JobRecord) -> None:
        statuses = [str(s) for s in JobStatus] + [""]
        clusters: Dict[str, AbstractSet[str]] = {job.cluster_name: set(), "": set()}
        owners = [job.owner, ""]
        tags = [t for t in job.tags] + [""]
        names = [job.name, ""] if job.name else [""]
        for key in self._generate_jobs_composite_keys(
            statuses=statuses, clusters=clusters, owners=owners, tags=tags, names=names,
        ):
            tr.zrem(key, job.id)

        statuses = [str(job.status), ""]
        for key in self._generate_jobs_composite_keys(
            statuses=statuses, clusters=clusters, owners=owners, tags=tags, names=names,
        ):
            tr.zadd(key, job.status_history.created_at_timestamp, job.id)

    @trace
    async def update_job_atomic(
        self, job: JobRecord, *, is_job_creation: bool, skip_index: bool = False
    ) -> None:
        payload = json.dumps(job.to_primitive())

        tr = self._client.multi_exec()
        tr.set(self._generate_job_key(job.id), payload)

        if is_job_creation:
            # NOTE: preserving the 'jobs' index for any future migration
            # purposes
            tr.sadd(self._generate_jobs_index_key(), job.id)
            if job.name:
                self._update_name_index(tr, job)
            self._update_tags_indexes(tr, job)

        if not skip_index:
            self._update_for_deletion_index(tr, job)

        if not skip_index:
            self._update_composite_index(tr, job)

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
            last_job_id = last_job_id_singleton[0]
            return last_job_id
        return None

    async def _get_jobs_by_ids_in_chunks(
        self, ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> AsyncIterator[Iterable[JobRecord]]:
        for chunk_ids in self._iterate_in_chunks(ids, JOBS_CHUNK_SIZE):
            keys = map(self._generate_job_key, chunk_ids)
            payloads = await self._client.mget(*keys)
            jobs = map(self._parse_job_payload, filter(None, payloads))
            if job_filter:
                jobs = filter(job_filter.check, jobs)
            yield jobs

    def _iterate_in_chunks(
        self, items: Iterable[Any], chunk_size: int
    ) -> Iterator[List[Any]]:
        # in case there are lots of jobs to retrieve, the parsing code below
        # blocks the concurrent execution for significant amount of time.
        # to mitigate the issue, we split passed long list by chunks,
        if isinstance(items, list):
            for i in range(0, len(items), chunk_size):
                yield items[i : i + chunk_size]
        else:
            it = iter(items)
            while True:
                chunk = list(islice(it, 0, chunk_size))
                if not chunk:
                    break
                yield chunk

    async def _get_job_ids(
        self,
        *,
        statuses: AbstractSet[JobStatus],
        clusters: Dict[str, AbstractSet[str]],
        owners: AbstractSet[str],
        tags: AbstractSet[str],
        name: Optional[str],
        since: datetime,
        until: datetime,
        reverse: bool,
    ) -> Iterable[str]:
        keys = self._generate_jobs_composite_keys(
            statuses=[str(s) for s in statuses] or [""],
            clusters=clusters or {"": set()},
            owners=owners or [""],
            tags=tags or [""],
            names=[name or ""],
        )

        if len(keys) == 1:
            result = await self._client.zrangebyscore(
                keys[0], since.timestamp(), until.timestamp()
            )
            if reverse:
                result.reverse()
            return result

        tr = self._client.multi_exec()
        for key in keys:
            tr.zrangebyscore(key, since.timestamp(), until.timestamp(), withscores=True)
        results = await tr.execute()
        if reverse:
            for x in results:
                x.reverse()
        it = heapq.merge(*results, key=itemgetter(1), reverse=reverse)
        # Merge repeated job ids for multiple tags
        if tags:
            it = chain.from_iterable(
                map(dict.fromkeys, map(itemgetter(1), groupby(it, itemgetter(1))))
            )

        return map(itemgetter(0), it)

    async def _get_job_ids_for_deletion(self) -> List[str]:
        return [
            job_id
            async for job_id in self._client.isscan(
                self._generate_jobs_for_deletion_index_key()
            )
        ]

    async def iter_all_jobs(
        self, job_filter: Optional[JobFilter] = None, *, reverse: bool = False
    ) -> AsyncIterator[JobRecord]:
        if not job_filter:
            job_filter = JobFilter()
        job_ids: Iterable[str] = job_filter.ids
        if not job_ids:
            job_ids = await self._get_job_ids(
                statuses=job_filter.statuses,
                clusters=job_filter.clusters,
                owners=job_filter.owners,
                tags=job_filter.tags,
                name=job_filter.name,
                since=job_filter.since,
                until=job_filter.until,
                reverse=reverse,
            )
            if len(job_filter.tags) > 1:
                job_filter = JobFilter(tags=job_filter.tags)
            else:
                job_filter = None

        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids, job_filter):
            for job in chunk:
                yield job

    @trace
    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        jobs: List[JobRecord] = []
        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids, job_filter):
            jobs.extend(chunk)
        return jobs

    @trace
    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        job_ids = await self._get_job_ids_for_deletion()
        jobs: List[JobRecord] = []
        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids):
            jobs.extend(job for job in chunk if job.should_be_deleted(delay=delay))
        return jobs

    async def get_tags(self, owner: str) -> List[str]:
        key = self._generate_tags_owner_index_zset_key(owner)
        return await self._client.zrange(key)

    @trace
    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        zero_run_time = (timedelta(), timedelta())
        aggregated_run_times: Dict[str, Tuple[timedelta, timedelta]] = {}
        async for job in self.iter_all_jobs(job_filter):
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
        if version < 3:
            await self._update_job_cluster_names()
        if version < 4:
            await self._reindex_jobs_for_deletion()
        if version < 5:
            await self._reindex_jobs_composite()
        else:
            return False
        await self._client.set("version", "5")
        return True

    async def _update_job_cluster_names(self) -> None:
        logger.info("Starting updating job cluster names")

        total = changed = 0
        async for chunk in self._iter_all_jobs_in_chunks():
            tr = self._client.pipeline()
            for job in chunk:
                total += 1
                if job.cluster_name:
                    continue
                changed += 1
                job.cluster_name = "default"
                payload = json.dumps(job.to_primitive())
                tr.set(self._generate_job_key(job.id), payload)
            await tr.execute()

        logger.info(f"Finished updating job cluster names ({changed}/{total})")

    async def _reindex_jobs_for_deletion(self) -> None:
        logger.info("Starting reindexing jobs for deletion")

        async for chunk in self._iter_all_jobs_in_chunks():
            tr = self._client.pipeline()
            for job in chunk:
                self._update_for_deletion_index(tr, job)
            await tr.execute()

        logger.info("Finished reindexing jobs for deletion")

    async def _reindex_jobs_composite(self) -> None:
        logger.info("Starting reindexing jobs composite")

        async for chunk in self._iter_all_jobs_in_chunks():
            tr = self._client.pipeline()
            for job in chunk:
                self._update_composite_index(tr, job)
            await tr.execute()

        logger.info("Finished reindexing jobs composite")

    async def _iter_all_jobs_in_chunks(self) -> AsyncIterator[Iterable[JobRecord]]:
        jobs_key = self._generate_jobs_index_key()
        job_ids = [job_id async for job_id in self._client.isscan(jobs_key)]
        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids):
            yield chunk
