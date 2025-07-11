from collections.abc import AsyncIterator, Iterable, Mapping, Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from asyncpg import SerializationError, UniqueViolationError
from sqlalchemy import Boolean, and_, asc, desc, func, or_
from sqlalchemy.engine import Row
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobError, JobStatus
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from ..base_postgres_storage import BasePostgresStorage
from .base import (
    ClusterOrgProjectNameSet,
    JobsStorage,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)


@dataclass(frozen=True)
class JobTables:
    jobs: sa.Table
    metadata = sa.MetaData()

    @classmethod
    def create(cls) -> "JobTables":
        table_name = "jobs"
        if table_name in cls.metadata:
            return cls(jobs=cls.metadata.tables[table_name])

        jobs_table = sa.Table(
            table_name,
            cls.metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("owner", sa.String(), nullable=False),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("cluster_name", sa.String(), nullable=False),
            sa.Column("org_name", sa.String(), nullable=True),
            sa.Column("project_name", sa.String(), nullable=False),
            sa.Column("org_project_hash", sapg.BYTEA(), nullable=False),
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
            sa.Column("namespace", sa.String(), nullable=True),
        )
        (sa.Index("jobs_owner_index", jobs_table.c.owner),)
        (sa.Index("jobs_name_index", jobs_table.c.name),)
        (sa.Index("jobs_status_index", jobs_table.c.status),)
        (sa.Index("jobs_created_at_index", jobs_table.c.created_at),)
        (sa.Index("jobs_finished_at_index", jobs_table.c.finished_at),)
        (
            sa.Index(
                "jobs_materialized_index",
                sa.cast(jobs_table.c.payload["materialized"].astext, Boolean),
            ),
        )
        sa.Index(
            "jobs_tags_index",
            jobs_table.c.tags,
            postgresql_using="gin",
            postgresql_ops={"tags": "jsonb_path_ops"},
        )
        sa.Index("jobs_orgs_index", jobs_table.c.org_name)
        sa.Index("jobs_org_project_hash_index", jobs_table.c.org_project_hash)
        sa.Index(
            "jobs_name_project_name_uq",
            jobs_table.c.name,
            jobs_table.c.project_name,
            unique=True,
            postgresql_where="""
                (
                    (
                        (status)::text <> 'succeeded'::text
                    ) AND
                    (
                        (status)::text <> 'failed'::text
                    ) AND
                    (
                        (status)::text <> 'cancelled'::text
                    )
                )
            """,
        )

        return cls(
            jobs=jobs_table,
        )


class PostgresJobsStorage(BasePostgresStorage, JobsStorage):
    def __init__(self, engine: AsyncEngine, tables: JobTables | None = None) -> None:
        super().__init__(engine)
        self._tables = tables or JobTables.create()

    # Parsing/serialization

    def _job_to_values(self, job: JobRecord) -> dict[str, Any]:
        payload = job.to_primitive()
        payload.pop("org_project_hash")
        return {
            "id": payload.pop("id"),
            "owner": payload.pop("owner"),
            "name": payload.pop("name", None),
            "cluster_name": payload.pop("cluster_name"),
            "org_name": payload.pop("org_name", None),
            "project_name": payload.pop("project_name"),
            "org_project_hash": job.org_project_hash,
            "tags": payload.pop("tags", None),
            "status": job.status_history.current.status,
            "created_at": job.status_history.created_at,
            "finished_at": job.status_history.finished_at,
            "payload": payload,
            "namespace": job.namespace,
        }

    def _record_to_job(self, record: Row) -> JobRecord:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["owner"] = record["owner"]
        payload["name"] = record["name"]
        payload["cluster_name"] = record["cluster_name"]
        payload["org_name"] = record["org_name"]
        payload["project_name"] = record["project_name"]
        payload["org_project_hash"] = record["org_project_hash"]
        if record["tags"] is not None:
            payload["tags"] = record["tags"]
        payload["namespace"] = record["namespace"]
        return JobRecord.from_primitive(payload)

    # Simple operations

    def _make_description(self, values: Mapping[str, Any]) -> str:
        if values["name"] is not None:
            return (
                f"id={values['id']}, "
                f"project={values['project_name']}, "
                f"name={values['name']}"
            )
        return f"id={values['id']}"

    async def _select_row(
        self, job_id: str, conn: AsyncConnection | None = None
    ) -> Row:
        query = self._tables.jobs.select(self._tables.jobs.c.id == job_id)
        record = await self._fetchrow(query, conn=conn)
        if not record:
            raise JobError(f"no such job {job_id}")
        return record

    async def _insert_values(
        self, values: Mapping[str, Any], conn: AsyncConnection | None = None
    ) -> None:
        query = self._tables.jobs.insert().values(values)
        try:
            await self._execute(query, conn=conn)
        except IntegrityError as exc:
            if isinstance(exc.orig.__cause__, UniqueViolationError):
                e = exc.orig.__cause__
                if e.constraint_name == "jobs_name_project_name_uq":
                    # We need to retrieve a conflicting job from database to
                    # build JobStorageJobFoundError
                    project_name = values["project_name"]
                    query = (
                        self._tables.jobs.select()
                        .where(self._tables.jobs.c.name == values["name"])
                        .where(self._tables.jobs.c.project_name == project_name)
                        .where(
                            self._tables.jobs.c.status.in_(JobStatus.active_values())
                        )
                    )
                    record = await self._fetchrow(query)
                    if record:
                        raise JobStorageJobFoundError(
                            job_name=values["name"],
                            project_name=project_name,
                            found_job_id=record["id"],
                        )
                    # Conflicted entry gone. Retry insert. Possible infinite
                    # loop has very low probability
                    await self._insert_values(values, conn=conn)
                # Conflicting id case:
                raise JobStorageTransactionError(
                    "Job {" + self._make_description(values) + "} has changed"
                )
            raise

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
        async with self._transaction() as conn:
            result = await self._fetchrow(query, conn=conn)
            if result:
                # Docs on status messages are placed here:
                # https://www.postgresql.org/docs/current/protocol-message-formats.html
                return
            # There was no row with such id, lets insert it.
            values["id"] = job_id
            await self._insert_values(values, conn=conn)

    async def get_job(self, job_id: str) -> JobRecord:
        record = await self._select_row(job_id)
        return self._record_to_job(record)

    async def drop_job(self, job_id: str) -> None:
        query = (
            self._tables.jobs.delete()
            .where(self._tables.jobs.c.id == job_id)
            .returning(self._tables.jobs.c.id)
        )
        async with self._transaction() as conn:
            result = await self._fetchrow(query, conn=conn)
        if result is None:
            raise JobError(f"no such job {job_id}")

    @asynccontextmanager
    async def try_create_job(self, job: JobRecord) -> AsyncIterator[JobRecord]:
        # No need to do any checks -- INSERT cannot be executed twice
        yield job
        values = self._job_to_values(job)
        async with self._transaction() as conn:
            await self._insert_values(values, conn=conn)

    @asynccontextmanager
    async def try_update_job(self, job_id: str) -> AsyncIterator[JobRecord]:
        try:
            async with self._engine.execution_options(
                isolation_level="REPEATABLE READ"
            ).begin() as conn:
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
        except DBAPIError as exc:
            if isinstance(exc.orig.__cause__, SerializationError):
                raise JobStorageTransactionError(
                    "Job {" + self._make_description(values) + "} has changed"
                )
            else:
                raise

    def _clause_for_filter(self, job_filter: JobFilter) -> sasql.ClauseElement:
        return JobFilterClauseBuilder.by_job_filter(job_filter, self._tables)

    @asyncgeneratorcontextmanager
    async def iter_all_jobs(
        self,
        job_filter: JobFilter | None = None,
        *,
        reverse: bool = False,
        limit: int | None = None,
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
        async with self._cursor(query) as cursor:
            async for record in cursor:
                yield self._record_to_job(record)

    async def get_jobs_by_ids(
        self, job_ids: Iterable[str], job_filter: JobFilter | None = None
    ) -> list[JobRecord]:
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
        return [id_to_job[job_id] for job_id in job_ids if job_id in id_to_job]

    async def get_jobs_for_deletion(
        self, *, delay: timedelta = timedelta()
    ) -> list[JobRecord]:
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

    async def get_jobs_for_drop(
        self,
        *,
        delay: timedelta = timedelta(),
        limit: int | None = None,
    ) -> list[JobRecord]:
        job_filter = JobFilter(
            statuses={JobStatus(item) for item in JobStatus.finished_values()},
            materialized=False,
        )
        now = datetime.now(UTC)
        query = (
            self._tables.jobs.select()
            .where(self._clause_for_filter(job_filter))
            .where(self._tables.jobs.c.finished_at < now - delay)
        )
        if limit:
            query = query.limit(limit)
        return [self._record_to_job(record) for record in await self._fetch(query)]


class JobFilterClauseBuilder:
    def __init__(self, tables: JobTables):
        self._clauses: list[sasql.ClauseElement] = []
        self._tables = tables

    def filter_statuses(self, statuses: AbstractSet[JobStatus]) -> None:
        self._clauses.append(self._tables.jobs.c.status.in_(statuses))

    def filter_owners(self, owners: AbstractSet[str]) -> None:
        self._clauses.append(self._tables.jobs.c.owner.in_(owners))

    def filter_base_owners(self, base_owners: AbstractSet[str]) -> None:
        self._clauses.append(self._create_base_owner_clause(base_owners))

    def _create_base_owner_clause(
        self, base_owners: AbstractSet[str]
    ) -> sasql.ClauseElement:
        return func.split_part(self._tables.jobs.c.owner, "/", 1).in_(base_owners)

    def filter_clusters(self, clusters: ClusterOrgProjectNameSet) -> None:
        cluster_clauses = []
        clusters_empty_orgs = []
        for cluster, orgs in clusters.items():
            if not orgs:
                clusters_empty_orgs.append(cluster)
                continue
            orgs_empty_projects = []
            for org, projects in orgs.items():
                if not projects:
                    orgs_empty_projects.append(org)
                    continue
                if org is None:
                    org_pred = self._tables.jobs.c.org_name.is_(None)
                else:
                    org_pred = self._tables.jobs.c.org_name == org
                projects_empty_names = []
                for project, names in projects.items():
                    if not names:
                        projects_empty_names.append(project)
                        continue
                    project_pred = self._tables.jobs.c.project_name == project
                    cluster_clauses.append(
                        (self._tables.jobs.c.cluster_name == cluster)
                        & org_pred
                        & project_pred
                        & self._tables.jobs.c.name.in_(names)
                    )
                if projects_empty_names:
                    project_pred = self._tables.jobs.c.project_name.in_(
                        projects_empty_names
                    )
                    cluster_clauses.append(
                        (self._tables.jobs.c.cluster_name == cluster)
                        & org_pred
                        & project_pred
                    )
            not_null_orgs = [org for org in orgs_empty_projects if org is not None]
            if not_null_orgs:
                cluster_clauses.append(
                    (self._tables.jobs.c.cluster_name == cluster)
                    & self._tables.jobs.c.org_name.in_(not_null_orgs)
                )
            if None in orgs_empty_projects:
                cluster_clauses.append(
                    (self._tables.jobs.c.cluster_name == cluster)
                    & self._tables.jobs.c.org_name.is_(None)
                )
        if clusters_empty_orgs:
            cluster_clauses.append(
                self._tables.jobs.c.cluster_name.in_(clusters_empty_orgs)
            )
        self._clauses.append(or_(*cluster_clauses))

    def filter_projects(self, projects: AbstractSet[str]) -> None:
        self._clauses.append(self._tables.jobs.c.project_name.in_(projects))

    def filter_orgs(self, orgs: AbstractSet[str | None]) -> None:
        not_null_orgs = [org for org in orgs if org is not None]
        or_clauses = []
        if not_null_orgs:
            or_clauses.append(self._tables.jobs.c.org_name.in_(not_null_orgs))
        if None in orgs:
            or_clauses.append(self._tables.jobs.c.org_name.is_(None))
        self._clauses.append(or_(*or_clauses))

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

    def _filter_bool_from_payload(self, field: str, filter_val: bool) -> None:
        val_sql = self._tables.jobs.c.payload[field].astext.cast(Boolean)
        if filter_val:
            self._clauses.append(val_sql == filter_val)
        else:
            self._clauses.append(or_(val_sql == filter_val, val_sql.is_(None)))

    def filter_materialized(self, materialized: bool) -> None:
        self._filter_bool_from_payload("materialized", materialized)

    def filter_being_dropped(self, being_dropped: bool) -> None:
        self._filter_bool_from_payload("being_dropped", being_dropped)

    def filter_logs_removed(self, logs_removed: bool) -> None:
        self._filter_bool_from_payload("logs_removed", logs_removed)

    def filter_org_project_hash(self, org_project_hash: bytes | str) -> None:
        if isinstance(org_project_hash, str):
            org_project_hash = bytes.fromhex(org_project_hash)
        self._clauses.append(self._tables.jobs.c.org_project_hash == org_project_hash)

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
        if job_filter.orgs:
            builder.filter_orgs(job_filter.orgs)
        if job_filter.projects:
            builder.filter_projects(job_filter.projects)
        if job_filter.name:
            builder.filter_name(job_filter.name)
        if job_filter.ids:
            builder.filter_ids(job_filter.ids)
        if job_filter.tags:
            builder.filter_tags(job_filter.tags)
        if job_filter.materialized is not None:
            builder.filter_materialized(job_filter.materialized)
        if job_filter.being_dropped is not None:
            builder.filter_being_dropped(job_filter.being_dropped)
        if job_filter.logs_removed is not None:
            builder.filter_logs_removed(job_filter.logs_removed)
        if job_filter.org_project_hash:
            builder.filter_org_project_hash(job_filter.org_project_hash)
        builder.filter_since(job_filter.since)
        builder.filter_until(job_filter.until)
        return builder.build()
