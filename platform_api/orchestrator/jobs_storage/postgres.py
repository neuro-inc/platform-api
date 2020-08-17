import json
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Dict,
    Optional, Mapping, AsyncIterator, Iterable, List,
)

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from async_generator import asynccontextmanager
from asyncpg import Connection, SerializationError, UniqueViolationError
from asyncpg.pool import Pool
from asyncpg.protocol.protocol import Record

from platform_api.orchestrator.job import JobRecord, AggregatedRunTime
from platform_api.orchestrator.job_request import JobError
from platform_api.orchestrator.job_request import JobStatus
from .base import JobStorageJobFoundError, JobFilter
from .base import JobsStorage, JobStorageTransactionError


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
            sa.Column("is_preemptible", sa.Boolean(), nullable=False),
            sa.Column("is_deleted", sa.Boolean(), nullable=True),
            sa.Column("max_run_time_minutes", sa.String(), nullable=True),
            sa.Column("internal_hostname", sa.String(), nullable=True),
            sa.Column("internal_hostname_named", sa.String(), nullable=True),
            sa.Column("schedule_timeout", sa.Float(), nullable=True),
            sa.Column("restart_policy", sa.String(), nullable=True),
            sa.Column("request", sapg.JSONB(), nullable=False),
            sa.Column("statuses", sapg.JSONB(), nullable=False),
            sa.Column("tags", sapg.JSONB(), nullable=True),
            # Field for optimized access/unique constrains checks
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        )
        return cls(jobs=jobs_table,)


class PostgresJobsStorage(JobsStorage):
    def __init__(self, pool: Pool, tables: Optional[JobTables] = None) -> None:
        self._pool = pool
        self._tables = tables or JobTables.create()

    # Database helpers

    async def _execute(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> None:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        await conn.execute(query_string, *params)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: Optional[Connection] = None
    ) -> Optional[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        conn = conn or self._pool
        return await conn.fetchrow(query_string, *params)

    # Parsing/serialization

    def _job_to_values(self, job: JobRecord) -> Dict[str, Any]:
        values = job.to_primitive()
        values["finished_at"] = job.finished_at
        return values

    def _record_to_job(self, record: Record) -> JobRecord:
        payload = dict(**record)
        payload["request"] = json.loads(payload["request"])
        payload["statuses"] = json.loads(payload["statuses"])
        if payload["tags"] is not None:
            payload["tags"] = json.loads(payload["tags"])
        return JobRecord.from_primitive(payload)

    # Simple operations

    def _make_description(self, values: Mapping[str, Any]) -> str:
        if "name" in values:
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
            .where(self._tables.jobs.c.id == job_id,)
        )
        result = await self._fetchrow(query)
        if result:
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

    def iter_all_jobs(
        self,
        job_filter: Optional[JobFilter] = None,
        *,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> AsyncIterator[JobRecord]:
        raise NotImplementedError

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: Optional[JobFilter] = None
    ) -> List[JobRecord]:
        raise NotImplementedError

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> List[JobRecord]:
        raise NotImplementedError

    async def get_tags(self, owner: str) -> List[str]:
        raise NotImplementedError

    async def get_aggregated_run_time_by_clusters(
        self, job_filter: JobFilter
    ) -> Dict[str, AggregatedRunTime]:
        raise NotImplementedError
