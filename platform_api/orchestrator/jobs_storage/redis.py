import heapq
import json
import logging
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import chain, groupby, islice
from operator import itemgetter
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)
from uuid import uuid4

import aioredis
from aioredis.commands import Pipeline
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.job_request import ContainerVolume, JobError, JobStatus
from platform_api.trace import trace

from .base import (
    ClusterOwnerNameSet,
    JobFilter,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)


logger = logging.getLogger(__name__)


JOBS_CHUNK_SIZE = 1000


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
        return "jobs.for-deletion"

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
        clusters: ClusterOwnerNameSet,
        owners: Iterable[str],
        names: Iterable[str],
        tags: Iterable[str],
    ) -> List[str]:
        return [
            f"jobs.comp.{status}|{cluster}|{owner}|{name}|{tag}"
            for cluster, cluster_owners in clusters.items()
            for owner in cluster_owners or owners
            for name in cluster_owners.get(owner) or names
            for status in statuses
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
        clusters: ClusterOwnerNameSet = {job.cluster_name: {}, "": {}}
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
        self,
        ids: Iterable[str],
        job_filter: Optional[JobFilter] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[Iterable[JobRecord]]:
        if not job_filter and limit is not None:
            ids = islice(ids, limit)
        for chunk_ids in self._iterate_in_chunks(ids, JOBS_CHUNK_SIZE):
            keys = map(self._generate_job_key, chunk_ids)
            payloads = await self._client.mget(*keys)
            jobs = map(self._parse_job_payload, filter(None, payloads))
            if job_filter:
                jobs = filter(job_filter.check, jobs)
                if limit is not None:
                    jobs = islice(jobs, limit)

            if limit is None or not job_filter:
                yield jobs
            else:
                jobs_list = list(jobs)
                yield jobs_list
                limit -= len(jobs_list)
                if not limit:
                    break

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
        clusters: ClusterOwnerNameSet,
        owners: AbstractSet[str],
        tags: AbstractSet[str],
        name: Optional[str],
        since: datetime,
        until: datetime,
        reverse: bool,
        limit: Optional[int],
    ) -> Iterable[str]:
        keys = self._generate_jobs_composite_keys(
            statuses=[str(s) for s in statuses] or [""],
            clusters=clusters or {"": {}},
            owners=owners or [""],
            tags=tags or [""],
            names=[name or ""],
        )

        ntags = len(tags)
        count = None if ntags > 1 else limit
        offset = None if count is None else 0
        if len(keys) == 1:
            if reverse:
                result = await self._client.zrevrangebyscore(
                    keys[0],
                    until.timestamp(),
                    since.timestamp(),
                    offset=offset,
                    count=count,
                )
            else:
                result = await self._client.zrangebyscore(
                    keys[0],
                    since.timestamp(),
                    until.timestamp(),
                    offset=offset,
                    count=count,
                )
            return result

        tr = self._client.multi_exec()
        for key in keys:
            if reverse:
                tr.zrevrangebyscore(
                    key,
                    until.timestamp(),
                    since.timestamp(),
                    withscores=True,
                    offset=offset,
                    count=count,
                )
            else:
                tr.zrangebyscore(
                    key,
                    since.timestamp(),
                    until.timestamp(),
                    withscores=True,
                    offset=offset,
                    count=count,
                )
        results = await tr.execute()
        it = heapq.merge(*results, key=itemgetter(1), reverse=reverse)
        # Merge repeated job ids for multiple tags
        if ntags > 1:

            def merge_tags(
                it: Iterator[Tuple[str, float]]
            ) -> Iterator[Tuple[str, float]]:
                """Merge repeated items and return only those which are
                repeated ntags times."""
                for item, count in Counter(it).items():
                    if count == ntags:
                        yield item

            it = chain.from_iterable(
                map(merge_tags, map(itemgetter(1), groupby(it, itemgetter(1))))
            )
        if limit is not None:
            it = islice(it, 0, limit)

        return map(itemgetter(0), it)

    async def _get_job_ids_for_deletion(self) -> List[str]:
        return [
            job_id
            async for job_id in self._client.isscan(
                self._generate_jobs_for_deletion_index_key()
            )
        ]

    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
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
                limit=limit,
            )
            job_filter = None
        else:
            assert limit is None

        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids, job_filter, limit):
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
        if version < 6:
            await self._update_volume_storage_uris()
        else:
            return False
        await self._client.set("version", "6")
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

    @staticmethod
    def _migrate_storage_uri(uri: URL, cluster_name: str) -> URL:
        assert uri.scheme == "storage"
        assert uri.host != cluster_name
        user_name = uri.host
        path = uri.path.lstrip("/")
        if user_name:
            if path:
                path = f"/{user_name}/{path}"
            else:
                path = f"/{user_name}"
        else:
            path = f"/{path}"
        return URL.build(scheme=uri.scheme, host=cluster_name, path=path)

    async def _update_volume_storage_uris(self) -> None:
        logger.info("Starting updating volume storage URIs")

        total = changed = 0
        async for chunk in self._iter_all_jobs_in_chunks():
            tr = self._client.pipeline()
            for job in chunk:
                total += 1
                assert job.cluster_name
                volumes: List[ContainerVolume] = []
                changed_job = False
                for volume in job.request.container.volumes:
                    uri = volume.uri
                    if uri.scheme == "storage" and uri.host != job.cluster_name:
                        uri = self._migrate_storage_uri(uri, job.cluster_name)
                        volume = replace(volume, uri=uri)
                        changed_job = True
                    volumes.append(volume)
                if changed_job:
                    container = replace(job.request.container, volumes=volumes)
                    job.request = replace(job.request, container=container)
                    changed += 1
                    payload = json.dumps(job.to_primitive())
                    tr.set(self._generate_job_key(job.id), payload)
            await tr.execute()

        logger.info(f"Finished updating volume storage URIs ({changed}/{total})")

    async def _iter_all_jobs_in_chunks(self) -> AsyncIterator[Iterable[JobRecord]]:
        jobs_key = self._generate_jobs_index_key()
        job_ids = [job_id async for job_id in self._client.isscan(jobs_key)]
        async for chunk in self._get_jobs_by_ids_in_chunks(job_ids):
            yield chunk
