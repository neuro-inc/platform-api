import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
from asyncpg import UniqueViolationError
from neuro_logging import trace, trace_cm
from sqlalchemy import asc, desc
from sqlalchemy.engine import Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from platform_api.orchestrator.base_postgres_storage import BasePostgresStorage
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BillingLogSyncRecord:
    last_entry_id: int


@dataclass(frozen=True)
class BillingLogEntry:
    idempotency_key: str
    job_id: str
    charge: Decimal
    last_billed: datetime
    fully_billed: bool

    id: Optional[int] = None


class BillingLogSyncRecordNotFound(Exception):
    pass


class BillingLogStorage(ABC):
    @abstractmethod
    async def get_or_create_sync_record(self) -> BillingLogSyncRecord:
        pass

    @abstractmethod
    async def update_sync_record(self, record: BillingLogSyncRecord) -> None:
        pass

    class EntriesInserter:
        @abstractmethod
        async def insert(
            self,
            entries: Sequence[BillingLogEntry],
        ) -> int:
            pass

    @abstractmethod
    def entries_inserter(
        self,
    ) -> AbstractAsyncContextManager["BillingLogStorage.EntriesInserter"]:
        pass

    @abstractmethod
    def iter_entries(
        self, *, with_ids_greater: int = 0, limit: Optional[int] = None
    ) -> AbstractAsyncContextManager[AsyncIterator[BillingLogEntry]]:
        pass

    @abstractmethod
    async def drop_entries(self, *, with_ids_le: int) -> None:
        pass

    @abstractmethod
    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        pass


class InMemoryBillingLogStorage(BillingLogStorage):
    def __init__(self) -> None:
        self._entries: list[BillingLogEntry] = []
        self._sync_record: Optional[BillingLogSyncRecord] = None
        self._inserter_lock = asyncio.Lock()
        self._dropped_cnt: int = 0

    async def get_or_create_sync_record(self) -> BillingLogSyncRecord:
        if self._sync_record is None:
            self._sync_record = BillingLogSyncRecord(0)
        return self._sync_record

    async def update_sync_record(self, record: BillingLogSyncRecord) -> None:
        if self._sync_record is None:
            raise BillingLogSyncRecordNotFound
        self._sync_record = record

    class EntriesInserter(BillingLogStorage.EntriesInserter):
        def __init__(self, storage: "InMemoryBillingLogStorage") -> None:
            self._storage = storage

        async def insert(self, entries: Sequence[BillingLogEntry]) -> int:
            next_index = await self._storage.get_last_entry_id() + 1
            for index, entry in enumerate(entries, next_index):
                self._storage._entries.append(replace(entry, id=index))
            return await self._storage.get_last_entry_id()

    @asynccontextmanager
    async def entries_inserter(
        self,
    ) -> AsyncIterator[BillingLogStorage.EntriesInserter]:
        async with self._inserter_lock:
            yield InMemoryBillingLogStorage.EntriesInserter(self)

    @asyncgeneratorcontextmanager
    async def iter_entries(
        self, *, with_ids_greater: int = 0, limit: Optional[int] = None
    ) -> AsyncIterator[BillingLogEntry]:
        with_ids_greater = max(0, with_ids_greater - self._dropped_cnt)
        entries = self._entries[with_ids_greater:]
        if limit is not None:
            entries = entries[:limit]
        for entry in entries:
            yield entry

    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        if job_id is None:
            return self._dropped_cnt + len(self._entries)
        for from_end, entry in enumerate(reversed(self._entries)):
            if entry.job_id == job_id:
                return self._dropped_cnt + len(self._entries) - from_end
        return 0

    async def drop_entries(self, *, with_ids_le: int) -> None:
        to_drop = with_ids_le - self._dropped_cnt
        if to_drop <= 0:
            return
        self._entries = self._entries[to_drop:]
        self._dropped_cnt += to_drop


@dataclass(frozen=True)
class BillingLogTables:
    billing_log: sa.Table
    sync_record: sa.Table

    @classmethod
    def create(cls) -> "BillingLogTables":
        metadata = sa.MetaData()
        billing_log = sa.Table(
            "billing_log",
            metadata,
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("job_id", sa.String(), nullable=False),
            # All other fields
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        sync_record = sa.Table(
            "sync_record",
            metadata,
            sa.Column("type", sa.String(), primary_key=True),
            sa.Column("last_entry_id", sa.Integer()),
        )
        return cls(
            billing_log=billing_log,
            sync_record=sync_record,
        )


class PostgresBillingLogStorage(BasePostgresStorage, BillingLogStorage):
    BILLING_SYNC_RECORD_TYPE = "BillingLogSyncRecord"

    def __init__(
        self, engine: AsyncEngine, tables: Optional[BillingLogTables] = None
    ) -> None:
        super().__init__(engine)
        self._tables = tables or BillingLogTables.create()

    # Parsing/serialization

    def _log_entry_to_values(self, entry: BillingLogEntry) -> dict[str, Any]:
        return {
            "job_id": entry.job_id,
            "payload": {
                "idempotency_key": entry.idempotency_key,
                "charge": str(entry.charge),
                "last_billed": entry.last_billed.isoformat(),
                "fully_billed": entry.fully_billed,
            },
        }

    def _record_to_log_entry(self, record: Row) -> BillingLogEntry:
        payload = record["payload"]
        return BillingLogEntry(
            id=record["id"],
            job_id=record["job_id"],
            idempotency_key=payload["idempotency_key"],
            charge=Decimal(payload["charge"]),
            last_billed=datetime.fromisoformat(payload["last_billed"]),
            fully_billed=payload["fully_billed"],
        )

    def _sync_record_to_values(
        self, sync_record: BillingLogSyncRecord
    ) -> dict[str, Any]:
        return {
            "type": self.BILLING_SYNC_RECORD_TYPE,
            "last_entry_id": sync_record.last_entry_id,
        }

    def _record_to_sync_record(self, record: dict[str, Any]) -> BillingLogSyncRecord:
        assert record["type"] == self.BILLING_SYNC_RECORD_TYPE
        return BillingLogSyncRecord(last_entry_id=record["last_entry_id"])

    # Public functions

    @trace
    async def get_or_create_sync_record(self) -> BillingLogSyncRecord:
        query = self._tables.sync_record.select()
        record = await self._fetchrow(query)
        if not record:
            try:
                empty = BillingLogSyncRecord(0)
                values = self._sync_record_to_values(empty)
                query = self._tables.sync_record.insert().values(values)
                async with self._transaction() as conn:
                    await self._execute(query, conn=conn)
            except IntegrityError as exc:
                if isinstance(exc.orig.__cause__, UniqueViolationError):
                    pass
                raise
            return await self.get_or_create_sync_record()
        else:
            return self._record_to_sync_record(record)

    @trace
    async def update_sync_record(self, record: BillingLogSyncRecord) -> None:
        values = self._sync_record_to_values(record)
        query = (
            self._tables.sync_record.update()
            .values(values)
            .where(self._tables.sync_record.c.type == self.BILLING_SYNC_RECORD_TYPE)
            .returning(self._tables.sync_record.c.type)
        )
        async with self._transaction() as conn:
            result = await self._fetchrow(query, conn=conn)
        if not result:
            raise BillingLogSyncRecordNotFound

    class EntriesInserter(BillingLogStorage.EntriesInserter):
        def __init__(self, storage: "PostgresBillingLogStorage", conn: AsyncConnection):
            self._storage = storage
            self._conn = conn

        async def insert(self, entries: Sequence[BillingLogEntry]) -> int:
            values = [self._storage._log_entry_to_values(entry) for entry in entries]
            query = (
                self._storage._tables.billing_log.insert()
                .values(values)
                .returning(self._storage._tables.billing_log.c.id)
            )
            records = await self._storage._fetch(query, conn=self._conn)
            return records[-1]["id"]

    @asynccontextmanager
    async def entries_inserter(
        self,
    ) -> AsyncIterator[BillingLogStorage.EntriesInserter]:
        tracing_name = PostgresBillingLogStorage.entries_inserter.__qualname__
        async with trace_cm(tracing_name), self._transaction() as conn:
            await self._execute(
                sa.text(
                    f"LOCK TABLE {self._tables.billing_log.name} IN SHARE "
                    "UPDATE EXCLUSIVE MODE"
                ),
                conn=conn,
            )
            yield PostgresBillingLogStorage.EntriesInserter(self, conn)

    @asyncgeneratorcontextmanager
    async def iter_entries(
        self, *, with_ids_greater: int = 0, limit: Optional[int] = None
    ) -> AsyncIterator[BillingLogEntry]:
        tracing_name = PostgresBillingLogStorage.iter_entries.__qualname__
        async with trace_cm(tracing_name):
            query = self._tables.billing_log.select()
            if with_ids_greater:
                query = query.where(self._tables.billing_log.c.id > with_ids_greater)
            query = query.order_by(asc(self._tables.billing_log.c.id))
            if limit:
                query = query.limit(limit)
            async with self._cursor(query) as cursor:
                async for record in cursor:
                    yield self._record_to_log_entry(record)

    @trace
    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        query = self._tables.billing_log.select()
        if job_id:
            query = query.where(self._tables.billing_log.c.job_id == job_id)
        # The + 0 magic is to force postgres to sort result instead
        # of using index backward scan on primary key.
        query = query.order_by(desc(self._tables.billing_log.c.id + 0))
        query = query.limit(1)
        record = await self._fetchrow(query)
        if record:
            return record["id"]
        return 0

    @trace
    async def drop_entries(self, *, with_ids_le: int) -> None:
        query = self._tables.billing_log.delete().where(
            self._tables.billing_log.c.id <= with_ids_le
        )
        async with self._transaction() as conn:
            await self._execute(query, conn=conn)
