import json
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
)

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from asyncpg import UniqueViolationError
from asyncpg.pool import Pool
from asyncpg.protocol.protocol import Record

from platform_api.orchestrator.job import AggregatedRunTime, JobRecord
from platform_api.orchestrator.job_request import JobError
from platform_api.orchestrator.jobs_storage import JobFilter

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

    async def _execute(self, query: sasql.ClauseElement) -> None:
        query_string, params = asyncpgsa.compile_query(query)
        await self._pool.execute(query_string, *params)

    async def _fetchrow(self, query: sasql.ClauseElement) -> Optional[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        return await self._pool.fetchrow(query_string, *params)

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

    # Public api

    async def set_job(self, job: JobRecord) -> None:
        # Update or Create logic.
        values = self._job_to_values(job)
        job_id = values.pop("id")
        query = (
            self._tables.jobs.update()
            .values(values)
            .where(
                self._tables.jobs.c.id == job_id,
            )
        )
        result = await self._fetchrow(query)
        if result:
            return
        values["id"] = job_id
        query = self._tables.jobs.insert().values(values)
        try:
            await self._execute(query)
        except UniqueViolationError:
            # Parallel insert have happened. Just raise error
            raise JobStorageTransactionError

    async def get_job(self, job_id: str) -> JobRecord:
        query = self._tables.jobs.select(self._tables.jobs.c.id == job_id)
        record = await self._fetchrow(query)
        if not record:
            raise JobError(f"no such job {job_id}")
        return self._record_to_job(record)

    def try_create_job(self, job: JobRecord) -> AsyncContextManager[JobRecord]:
        raise NotImplementedError

    def try_update_job(self, job_id: str) -> AsyncContextManager[JobRecord]:
        raise NotImplementedError

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
