import json
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, Optional

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from async_generator import asynccontextmanager
from asyncpg import Connection, SerializationError, UniqueViolationError
from asyncpg.cursor import CursorFactory
from asyncpg.pool import Pool
from asyncpg.protocol.protocol import Record
from sqlalchemy import Boolean, and_, asc, desc, not_, or_

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.job_request import JobError, JobStatus
from platform_api.orchestrator.jobs_storage import JobFilter

from .base import JobsStorage, JobStorageJobFoundError, JobStorageTransactionError


@dataclass(frozen=True)
class JobTables:
    jobs: sa.Table

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
        return cls(
            jobs=jobs_table,
        )


class PostgresJobsStorage(JobsStorage):
    def __init__(self, pool: Pool, tables: Optional[JobTables] = None) -> None:
        self._pool = pool
        self._tables = tables or JobTables.create()

    # Database helpers

    async def _execute(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> str:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.execute(query_string, *params)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> Optional[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.fetchrow(query_string, *params)

    async def _fetch(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> List[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.fetch(query_string, *params)

    def _cursor(self, query: sasql.ClauseElement, conn: Connection) -> CursorFactory:
        query_string, params = asyncpgsa.compile_query(query)
        return conn.cursor(query_string, *params)

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
        values = job.to_primitive()
        values["status"] = job.status_history.current.status
        values["created_at"] = job.status_history.created_at
        values["finished_at"] = job.status_history.finished_at
        return values

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
                query = self._tables.jobs.select().where(
                    self._tables.jobs.c.name == values["name"]
                    and self._tables.jobs.c.owner == values["owner"]
                    and self._tables.jobs.c.status in JobStatus.active_values()
                )
                record = await self._fetchrow(query)
                if record:
                    raise JobStorageJobFoundError(
                        job_name=values["name"],
                        job_owner=values["owner"],
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
        clauses = []
        if job_filter.statuses:
            clauses.append(self._tables.jobs.c.status.in_(job_filter.statuses))
        if job_filter.owners:
            clauses.append(self._tables.jobs.c.owner.in_(job_filter.owners))
        if job_filter.clusters:
            cluster_clauses = []
            clusters_empty_owners = []
            for cluster, owners in job_filter.clusters.items():
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
            clauses.append(or_(*cluster_clauses))
        if job_filter.name:
            clauses.append(self._tables.jobs.c.name == job_filter.name)
        if job_filter.ids:
            clauses.append(self._tables.jobs.c.id.in_(job_filter.ids))
        if job_filter.tags:
            clauses.append(self._tables.jobs.c.tags.contains(list(job_filter.tags)))
        clauses.append(
            (job_filter.since <= self._tables.jobs.c.created_at)
            & (self._tables.jobs.c.created_at <= job_filter.until)
        )
        return and_(*clauses)

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
            job_filter = replace(job_filter, ids=set(job_ids) & job_filter.ids)
        all_jobs = []
        async for job in self.iter_all_jobs(job_filter):
            all_jobs.append(job)
        # Restore ordering
        id_to_job = {job.id: job for job in all_jobs}
        all_jobs = [id_to_job[job_id] for job_id in job_ids if job_id in id_to_job]
        return all_jobs

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        query = (
            self._tables.jobs.select()
            .where(self._tables.jobs.c.status.in_(JobStatus.finished_values()))
            .where(not_(self._tables.jobs.c.payload["is_deleted"].astext.cast(Boolean)))
        )
        for_deletion = []
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                job = self._record_to_job(record)
                if job.should_be_deleted(delay=delay):
                    for_deletion.append(job)
        return for_deletion

    async def get_tags(self, owner: str) -> List[str]:
        raise NotImplementedError

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        raise NotImplementedError
