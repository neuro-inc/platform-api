import json
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
)

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from asyncpg import Connection, SerializationError, UniqueViolationError
from asyncpg.pool import Pool
from asyncpg.protocol.protocol import Record
from sqlalchemy import Boolean, Integer, and_, asc, desc, func, or_, select

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.job_request import JobError, JobStatus
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from ..base_postgres_storage import BasePostgresStorage
from .base import (
    ClusterOwnerNameSet,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
    RunTimeEntry,
)


@dataclass(frozen=True)
class JobTables:
    jobs: sa.Table
    jobs_runtime_cache: sa.Table

    @classmethod
    def create(cls) -> "JobTables":
        metadata = sa.MetaData()
        jobs_table = sa.Table(
            "jobs",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("owner", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("cluster_name", sa.String(), nullable=False),
            sa.Column("tags", sapg.JSONB(), nullable=True),
            # Denormalized fields for optimized access/unique constrains checks
            sa.Column("status", sa.String(), nullable=False),
            sa.Column(
                "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
            ),
            sa.Column(
                "finished_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=True
            ),
            # All other fields
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        jobs_runtime_cache_table = sa.Table(
            "jobs_runtime_cache",
            metadata,
            sa.Column("owner", sa.String(), primary_key=True),
            sa.Column(
                "last_finished",
                sapg.TIMESTAMP(timezone=True, precision=6),
                nullable=False,
            ),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        return cls(
            jobs=jobs_table,
            jobs_runtime_cache=jobs_runtime_cache_table,
        )


class PostgresJobsStorage(BasePostgresStorage, JobsStorage):
    def __init__(self, pool: Pool, tables: Optional[JobTables] = None) -> None:
        super().__init__(pool)
        self._tables = tables or JobTables.create()

    # Parsing/serialization

    def _job_to_values(self, job: JobRecord) -> Dict[str, Any]:
        payload = job.to_primitive()
        return {
            "id": payload.pop("id"),
            "owner": payload.pop("owner"),
            "name": payload.pop("name", None),
            "cluster_name": payload.pop("cluster_name"),
            "tags": payload.pop("tags", None),
            "status": job.status_history.current.status,
            "created_at": job.status_history.created_at,
            "finished_at": job.status_history.finished_at,
            "payload": payload,
        }

    def _record_to_job(self, record: Record) -> JobRecord:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["owner"] = record["owner"]
        payload["name"] = record["name"]
        payload["cluster_name"] = record["cluster_name"]
        if record["tags"] is not None:
            payload["tags"] = json.loads(record["tags"])
        return JobRecord.from_primitive(payload)

    # Simple operations

    def _make_description(self, values: Mapping[str, Any]) -> str:
        if values["name"] is not None:
            return f"id={values['id']}, owner={values['owner']}, name={values['name']}"
        return f"id={values['id']}"

    async def _select_row(
        self, job_id: str, conn: Optional[Connection] = None
    ) -> Record:
        query = self._tables.jobs.select(self._tables.jobs.c.id == job_id)
        record = await self._fetchrow(query, conn=conn)
        if not record:
            raise JobError(f"no such job {job_id}")
        return record

    async def _insert_values(
        self, values: Mapping[str, Any], conn: Optional[Connection] = None
    ) -> None:
        query = self._tables.jobs.insert().values(values)
        try:
            await self._execute(query, conn=conn)
        except UniqueViolationError as e:
            if e.constraint_name == "jobs_name_owner_uq":
                # We need to retrieve conflicting job from database to
                # build JobStorageJobFoundError
                base_owner = values["owner"].split("/")[0]
                query = (
                    self._tables.jobs.select()
                    .where(self._tables.jobs.c.name == values["name"])
                    .where(
                        func.split_part(self._tables.jobs.c.owner, "/", 1) == base_owner
                    )
                    .where(self._tables.jobs.c.status.in_(JobStatus.active_values()))
                )
                record = await self._fetchrow(query)
                if record:
                    raise JobStorageJobFoundError(
                        job_name=values["name"],
                        job_owner=base_owner,
                        found_job_id=record["id"],
                    )
                else:
                    # Conflicted entry gone. Retry insert. Possible infinite
                    # loop has very low probability
                    await self._insert_values(values)
            # Conflicting id case:
            raise JobStorageTransactionError(
                "Job {" + self._make_description(values) + "} has changed"
            )

    # Public api

    async def set_job(self, job: JobRecord) -> None:
        # Update or Create logic.
        values = self._job_to_values(job)
        job_id = values.pop("id")
        query = (
            self._tables.jobs.update()
            .values(values)
            .where(self._tables.jobs.c.id == job_id)
            .returning(self._tables.jobs.c.id)
        )
        result = await self._fetchrow(query)
        if result:
            # Docs on status messages are placed here:
            # https://www.postgresql.org/docs/current/protocol-message-formats.html
            return
        # There was no row with such id, lets insert it.
        values["id"] = job_id
        await self._insert_values(values)

    async def get_job(self, job_id: str) -> JobRecord:
        record = await self._select_row(job_id)
        return self._record_to_job(record)

    async def drop_job(self, job_id: str) -> None:
        query = (
            self._tables.jobs.delete()
            .where(self._tables.jobs.c.id == job_id)
            .returning(self._tables.jobs.c.id)
        )
        result = await self._fetchrow(query)
        if result is None:
            raise JobError(f"no such job {job_id}")

    @asynccontextmanager
    async def try_create_job(self, job: JobRecord) -> AsyncIterator[JobRecord]:
        # No need to do any checks -- INSERT cannot be executed twice
        yield job
        values = self._job_to_values(job)
        await self._insert_values(values)

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        try:
            async with self._pool.acquire() as conn, conn.transaction(
                isolation="repeatable_read"
            ):
                # The isolation level 'serializable' is not used here because:
                # - we only care about single row synchronization (we just want to
                # protect from concurrent writes between our SELECT and
                # UPDATE queries)
                # - in 'serializable' mode concurrent reads to same pages of index are
                # forbidden (breaks tests)
                # - in 'serializable' sequential search (default for small tables) locks
                # whole table (breaks tests)
                record = await self._select_row(job_id, conn=conn)
                job = self._record_to_job(record)
                yield job
                values = self._job_to_values(job)
                query = (
                    self._tables.jobs.update()
                    .values(values)
                    .where(self._tables.jobs.c.id == job_id)
                )
                await self._execute(query, conn=conn)
        except SerializationError:
            raise JobStorageTransactionError(
                "Job {" + self._make_description(values) + "} has changed"
            )

    def _clause_for_filter(self, job_filter: JobFilter) -> sasql.ClauseElement:
        return JobFilterClauseBuilder.by_job_filter(job_filter, self._tables)

    @asyncgeneratorcontextmanager
    async def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        query = self._tables.jobs.select()
        if job_filter is not None:
            query = query.where(self._clause_for_filter(job_filter))
        if reverse:
            query = query.order_by(desc(self._tables.jobs.c.created_at))
        else:
            query = query.order_by(asc(self._tables.jobs.c.created_at))
        if limit:
            query = query.limit(limit)
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._record_to_job(record)

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        if job_filter is None:
            job_filter = JobFilter()
        if job_filter.ids:
            job_ids = set(job_ids) & job_filter.ids
        if not list(job_ids):
            return []
        job_filter = replace(job_filter, ids=set(job_ids))
        all_jobs = []
        async with self.iter_all_jobs(job_filter) as it:
            async for job in it:
                all_jobs.append(job)
        # Restore ordering
        id_to_job = {job.id: job for job in all_jobs}
        all_jobs = [id_to_job[job_id] for job_id in job_ids if job_id in id_to_job]
        return all_jobs

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        job_filter = JobFilter(
            statuses={JobStatus(item) for item in JobStatus.finished_values()},
            materialized=True,
        )
        for_deletion = []
        async with self.iter_all_jobs(job_filter) as it:
            async for job in it:
                if job.should_be_deleted(delay=delay):
                    for_deletion.append(job)
        return for_deletion

    async def get_tags(self, owner: str) -> List[str]:
        # This methods has the following requirements:
        # - it should return all job tags for the given owner
        # - tags should be sorted by created_at date of the most recent job
        # - tags that have the same created_at date should be sorted alphabetically
        # To achieve these goals we:
        # - Sort and enumerate tags jsonb array for each job using SQL functions
        # defined at alembic migration (..._create_jobs_table.py)
        # - Flatten those array into single result set using postgresql
        # function jsonb_array_elements
        # - Add created_at as the third column
        # Now we can have duplicated tags. To eliminate them, we properly order
        # result set and use DISTINCT ON. Unfortunately, this requires makes us
        # to use "tag_name" as the first ordering key.
        # - Using the result of the previous step as a subquery, we reorder it
        # properly.
        #
        # It's complicated, I know :).

        sorted_tags = sasql.func.sort_json_str_array(self._tables.jobs.c.tags)
        enumerated_tags = sasql.func.enumerate_json_array(sorted_tags)
        tag = sasql.func.jsonb_array_elements(enumerated_tags).alias("tag")
        tag_col = sa.column("value", type_=sapg.JSONB)
        tag_name = tag_col["value"].astext.label("tag_name")

        sub_query = (
            select(
                [
                    tag_name,
                    self._tables.jobs.c.created_at,
                    tag_col["index"].astext.cast(Integer).label("index"),
                ]
            )
            .distinct(tag_name)
            .select_from(self._tables.jobs)
            .select_from(tag)
            .where(self._tables.jobs.c.owner == owner)
            .where(self._tables.jobs.c.tags != "null")
            .order_by(
                tag_name,
                desc(self._tables.jobs.c.created_at),
                tag_col["index"].astext.cast(Integer),
            )
            .alias()
        )
        query = (
            select([sub_query.c.tag_name])
            .select_from(sub_query)
            .order_by(desc(sub_query.c.created_at), sub_query.c.index)
        )
        return [record[0] for record in await self._fetch(query)]

    async def get_aggregated_run_time_by_clusters(
        self, owner: str
    ) -> Dict[str, AggregatedRunTime]:

        aggregated_run_times: Dict[str, RunTimeEntry] = defaultdict(RunTimeEntry)
        cached_run_times: Dict[str, RunTimeEntry] = defaultdict(RunTimeEntry)

        # Collect data for still running jobs
        running_filter = JobFilter(
            owners={owner},
            statuses={JobStatus(value) for value in JobStatus.active_values()},
        )
        async with self.iter_all_jobs(running_filter) as it:
            async for job in it:
                aggregated_run_times[job.cluster_name].increase_by(
                    RunTimeEntry.for_job(job)
                )

        # Collect data from cache
        query = self._tables.jobs_runtime_cache.select().where(
            self._tables.jobs_runtime_cache.c.owner == owner
        )
        cache_record = await self._fetchrow(query)
        not_cached_query = (
            self._tables.jobs.select()
            .where(self._tables.jobs.c.owner == owner)
            .where(self._tables.jobs.c.status.in_(JobStatus.finished_values()))
        )

        if cache_record:
            payload = json.loads(cache_record["payload"])
            for cluster, run_time in payload.items():
                run_time_entry = RunTimeEntry.from_primitive(run_time)
                cached_run_times[cluster].increase_by(run_time_entry)
                aggregated_run_times[cluster].increase_by(run_time_entry)

            not_cached_query = not_cached_query.where(
                self._tables.jobs.c.finished_at > cache_record["last_finished"]
            )

        # This is to avoid race condition when
        # new finished JobRecord can be added
        # with finished_at that is a past for couple of seconds,
        # 5 minutes limit is just to be sure
        include_to_cache_before = datetime.now(timezone.utc) - timedelta(minutes=5)
        cache_last_finished = None  # Also a flag to update cache

        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(not_cached_query, conn=conn):
                job = self._record_to_job(record)
                job_run_time = RunTimeEntry.for_job(job)
                aggregated_run_times[job.cluster_name].increase_by(job_run_time)

                assert (
                    job.finished_at is not None
                ), "Non finished job returned by `not_cached_query` query"

                if job.finished_at < include_to_cache_before:
                    if cache_last_finished:
                        cache_last_finished = max(job.finished_at, cache_last_finished)
                    else:
                        cache_last_finished = job.finished_at
                    cached_run_times[job.cluster_name].increase_by(job_run_time)

        if cache_last_finished:
            # Cache should be updated
            values = {
                "owner": owner,
                "last_finished": cache_last_finished,
                "payload": {
                    cluster_name: job_run_entry.to_primitive()
                    for cluster_name, job_run_entry in cached_run_times.items()
                },
            }
            if cache_record:
                query = (
                    self._tables.jobs_runtime_cache.update()
                    .values(values)
                    .where(self._tables.jobs_runtime_cache.c.owner == owner)
                )
            else:
                query = self._tables.jobs_runtime_cache.insert().values(values)
            try:
                await self._execute(query)
            except UniqueViolationError:
                # Parallel write to cache, we can safely ignore
                pass

        return {
            cluster_name: run_time_entry.to_aggregated_run_time()
            for cluster_name, run_time_entry in aggregated_run_times.items()
        }


class JobFilterClauseBuilder:
    def __init__(self, tables: JobTables):
        self._clauses: List[sasql.ClauseElement] = []
        self._tables = tables

    def filter_statuses(self, statuses: AbstractSet[JobStatus]) -> None:
        self._clauses.append(self._tables.jobs.c.status.in_(statuses))

    def filter_owners(self, owners: AbstractSet[str]) -> None:
        self._clauses.append(self._tables.jobs.c.owner.in_(owners))

    def filter_base_owners(self, base_owners: AbstractSet[str]) -> None:
        self._clauses.append(
            func.split_part(self._tables.jobs.c.owner, "/", 1).in_(base_owners)
        )

    def filter_clusters(self, clusters: ClusterOwnerNameSet) -> None:
        cluster_clauses = []
        clusters_empty_owners = []
        for cluster, owners in clusters.items():
            if not owners:
                clusters_empty_owners.append(cluster)
                continue
            owners_empty_names = []
            for owner, names in owners.items():
                if not names:
                    owners_empty_names.append(owner)
                    continue
                cluster_clauses.append(
                    (self._tables.jobs.c.cluster_name == cluster)
                    & (self._tables.jobs.c.owner == owner)
                    & self._tables.jobs.c.name.in_(names)
                )
            cluster_clauses.append(
                (self._tables.jobs.c.cluster_name == cluster)
                & self._tables.jobs.c.owner.in_(owners_empty_names)
            )
        cluster_clauses.append(
            self._tables.jobs.c.cluster_name.in_(clusters_empty_owners)
        )
        self._clauses.append(or_(*cluster_clauses))

    def filter_name(self, name: str) -> None:
        self._clauses.append(self._tables.jobs.c.name == name)

    def filter_ids(self, ids: AbstractSet[str]) -> None:
        self._clauses.append(self._tables.jobs.c.id.in_(ids))

    def filter_tags(self, tags: AbstractSet[str]) -> None:
        self._clauses.append(self._tables.jobs.c.tags.contains(list(tags)))

    def filter_since(self, since: datetime) -> None:
        self._clauses.append(since <= self._tables.jobs.c.created_at)

    def filter_until(self, until: datetime) -> None:
        self._clauses.append(self._tables.jobs.c.created_at <= until)

    def filter_materialized(self, materialized: bool) -> None:
        self._clauses.append(
            self._tables.jobs.c.payload["materialized"].astext.cast(Boolean)
            == materialized
        )

    def filter_fully_billed(self, fully_billed: bool) -> None:
        self._clauses.append(
            self._tables.jobs.c.payload["fully_billed"].astext.cast(Boolean)
            == fully_billed
        )

    def build(self) -> sasql.ClauseElement:
        return and_(*self._clauses)

    @classmethod
    def by_job_filter(
        cls, job_filter: JobFilter, tables: JobTables
    ) -> sasql.ClauseElement:
        builder = cls(tables)
        if job_filter.statuses:
            builder.filter_statuses(job_filter.statuses)
        if job_filter.owners:
            builder.filter_owners(job_filter.owners)
        if job_filter.base_owners:
            builder.filter_base_owners(job_filter.base_owners)
        if job_filter.clusters:
            builder.filter_clusters(job_filter.clusters)
        if job_filter.name:
            builder.filter_name(job_filter.name)
        if job_filter.ids:
            builder.filter_ids(job_filter.ids)
        if job_filter.tags:
            builder.filter_tags(job_filter.tags)
        if job_filter.materialized is not None:
            builder.filter_materialized(job_filter.materialized)
        if job_filter.fully_billed is not None:
            builder.filter_fully_billed(job_filter.fully_billed)
        builder.filter_since(job_filter.since)
        builder.filter_until(job_filter.until)
        return builder.build()
