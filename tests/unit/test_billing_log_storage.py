from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from platform_api.orchestrator.billing_log.storage import (
    BillingLogEntry,
    BillingLogStorage,
    BillingLogSyncRecord,
    BillingLogSyncRecordNotFound,
    InMemoryBillingLogStorage,
)


class TestBillingLogStorage:
    @pytest.fixture()
    def storage(self) -> BillingLogStorage:
        return InMemoryBillingLogStorage()

    @pytest.mark.asyncio
    async def test_sync_record_retrieval(self, storage: BillingLogStorage) -> None:
        record = await storage.get_or_create_sync_record()
        assert record.last_entry_id == 0
        await storage.update_sync_record(BillingLogSyncRecord(10))
        record = await storage.get_or_create_sync_record()
        assert record.last_entry_id == 10

    @pytest.mark.asyncio
    async def test_sync_record_update_not_existing(
        self, storage: BillingLogStorage
    ) -> None:
        with pytest.raises(BillingLogSyncRecordNotFound):
            await storage.update_sync_record(BillingLogSyncRecord(10))

    def _make_log_entry(self, *, job_id: str, key: str) -> BillingLogEntry:
        return BillingLogEntry(
            idempotency_key=key,
            job_id=job_id,
            charge=Decimal("1.00"),
            fully_billed=False,
            last_billed=datetime.now(tz=timezone.utc),
        )

    @pytest.mark.asyncio
    async def test_create_and_get_entries(self, storage: BillingLogStorage) -> None:
        entries = [
            self._make_log_entry(job_id="test1", key="key1"),
            self._make_log_entry(job_id="test2", key="key2"),
            self._make_log_entry(job_id="test3", key="key3"),
            self._make_log_entry(job_id="test4", key="key4"),
        ]
        last_id = await storage.create_entries(entries)
        assert last_id == 4
        expected_entries = [
            replace(entry, id=index) for index, entry in enumerate(entries, 1)
        ]

        fetched_entries = []
        async for entry in storage.iter_entries(with_ids_greater=0):
            fetched_entries.append(entry)
        assert fetched_entries == expected_entries

        fetched_entries = []
        async for entry in storage.iter_entries(with_ids_greater=1):
            fetched_entries.append(entry)
        assert fetched_entries == expected_entries[1:]

        fetched_entries = []
        async for entry in storage.iter_entries(with_ids_greater=1, limit=2):
            fetched_entries.append(entry)
        assert fetched_entries == expected_entries[1:3]

    @pytest.mark.asyncio
    async def test_get_last_id(self, storage: BillingLogStorage) -> None:
        last_id = await storage.create_entries(
            [
                self._make_log_entry(job_id="test1", key="key1"),
                self._make_log_entry(job_id="test2", key="key2"),
                self._make_log_entry(job_id="test1", key="key3"),
                self._make_log_entry(job_id="test2", key="key4"),
            ]
        )
        assert last_id == 4

        assert 4 == await storage.get_last_entry_id()

        assert 3 == await storage.get_last_entry_id("test1")
        assert 4 == await storage.get_last_entry_id("test2")

        # Job without entries:
        assert 0 == await storage.get_last_entry_id("test3")
