import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, List, Optional, Sequence


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


class BillingLogStorage(ABC):
    @abstractmethod
    async def get_or_create_sync_record(self) -> BillingLogSyncRecord:
        pass

    @abstractmethod
    async def update_sync_record(self, record: BillingLogSyncRecord) -> None:
        pass

    @abstractmethod
    async def create_entries(
        self,
        entries: Sequence[BillingLogEntry],
    ) -> int:
        pass

    @abstractmethod
    def iter_entries(
        self, *, with_ids_greater: int = 0, limit: Optional[int] = None
    ) -> AsyncIterator[BillingLogEntry]:
        pass

    @abstractmethod
    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        pass


class InMemoryBillingLogStorage(BillingLogStorage):
    def __init__(self) -> None:
        self._entries: List[BillingLogEntry] = []
        self._sync_record: Optional[BillingLogSyncRecord] = None

    async def get_or_create_sync_record(self) -> BillingLogSyncRecord:
        if self._sync_record is None:
            self._sync_record = BillingLogSyncRecord(0)
        return self._sync_record

    async def update_sync_record(self, record: BillingLogSyncRecord) -> None:
        assert self._sync_record, "Tried to update sync record before creation"
        self._sync_record = record

    async def create_entries(self, entries: Sequence[BillingLogEntry]) -> int:
        next_index = await self.get_last_entry_id() + 1
        for index, entry in enumerate(entries, next_index):
            self._entries.append(replace(entry, id=index))
        return await self.get_last_entry_id()

    async def iter_entries(
        self, *, with_ids_greater: int = 0, limit: Optional[int] = None
    ) -> AsyncIterator[BillingLogEntry]:
        entries = self._entries[with_ids_greater:]
        if limit is not None:
            entries = entries[:limit]
        for entry in entries:
            yield entry

    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        if job_id is None:
            return len(self._entries)
        for from_end, entry in enumerate(reversed(self._entries)):
            if entry.job_id == job_id:
                return len(self._entries) - from_end
        return 0
