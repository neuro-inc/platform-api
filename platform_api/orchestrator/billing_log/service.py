import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncContextManager, AsyncIterator, Optional, Sequence

from platform_api.admin_client import AdminClient
from platform_api.orchestrator.billing_log.storage import (
    BillingLogEntry,
    BillingLogStorage,
    BillingLogSyncRecord,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.utils.update_notifier import Notifier


logger = logging.getLogger(__name__)


class BillingLogService:
    def __init__(
        self,
        storage: BillingLogStorage,
        new_entry: Notifier,
        entry_done: Notifier,
    ) -> None:
        self._storage = storage
        self._new_entry_notifier = new_entry
        self._entry_done_notifier = entry_done

        self._notifier_cm: Optional[AsyncContextManager[Any]] = None
        self._last_entry_id = 0
        self._progress_cond = asyncio.Condition()

    async def __aenter__(self) -> "BillingLogService":
        def _listener() -> None:
            asyncio.create_task(self._on_processed())

        self._notifier_cm = self._entry_done_notifier.listen_to_updates(_listener)

        record = await self._storage.get_or_create_sync_record()
        self._last_entry_id = record.last_entry_id

        await self._notifier_cm.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        assert self._notifier_cm, "__aenter__ should be called first"
        await self._notifier_cm.__aexit__(*args)

    async def _on_processed(self) -> None:
        record = await self._storage.get_or_create_sync_record()
        self._last_entry_id = record.last_entry_id
        async with self._progress_cond:
            self._progress_cond.notify_all()

    class Inserter:
        def __init__(
            self, inserter: BillingLogStorage.EntriesInserter, notifier: Notifier
        ):
            self._inserter = inserter
            self._notifier = notifier

        async def insert(self, entries: Sequence[BillingLogEntry]) -> int:
            last_id = await self._inserter.insert(entries)
            await self._notifier.notify()
            return last_id

    @asynccontextmanager
    async def entries_inserter(self) -> AsyncIterator["BillingLogService.Inserter"]:
        async with self._storage.entries_inserter() as inserter:
            yield BillingLogService.Inserter(inserter, self._new_entry_notifier)

    async def wait_until_processed(self, last_entry_id: int) -> None:
        while self._last_entry_id < last_entry_id:
            async with self._progress_cond:
                await self._progress_cond.wait()

    async def get_last_entry_id(self, job_id: Optional[str] = None) -> int:
        return await self._storage.get_last_entry_id(job_id)


class BillingLogWorker:
    def __init__(
        self,
        storage: BillingLogStorage,
        new_entry: Notifier,
        entry_done: Notifier,
        admin_client: AdminClient,
        jobs_service: JobsService,
        min_retry_interval_s: float = 1,
        max_retry_interval_s: float = 60,
        wait_timeout_s: float = 15,
    ) -> None:
        self._storage = storage
        self._new_entry_notifier = new_entry
        self._entry_done_notifier = entry_done
        self._admin_client = admin_client
        self._jobs_service = jobs_service
        self._unchecked_notify = asyncio.Event()

        self._retry_interval = max_retry_interval_s
        self._min_retry_interval_s = min_retry_interval_s
        self._max_retry_interval_s = max_retry_interval_s

        self._wait_timeout_s = wait_timeout_s

        self._task: Optional[asyncio.Task[Any]] = None
        self._notifier_cm: Optional[AsyncContextManager[Any]] = None

    async def __aenter__(self) -> "BillingLogWorker":
        self._unchecked_notify.set()  # Run checks initially
        self._task = asyncio.create_task(self._main_loop())
        self._notifier_cm = self._new_entry_notifier.listen_to_updates(
            self._set_has_pending
        )
        await self._notifier_cm.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        assert self._task, "__aenter__ should be called first"
        assert self._notifier_cm, "__aenter__ should be called first"
        await self._notifier_cm.__aexit__(*args)
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    def _set_has_pending(self) -> None:
        self._unchecked_notify.set()

    def _reset_retry_interval(self) -> None:
        self._retry_interval = self._min_retry_interval_s

    async def _sleep_retry(self) -> None:
        await asyncio.sleep(self._retry_interval)
        self._retry_interval = min(self._max_retry_interval_s, self._retry_interval * 2)

    async def _main_loop(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._unchecked_notify.wait(), timeout=self._wait_timeout_s
                )
            except asyncio.TimeoutError:
                pass
            else:
                self._unchecked_notify.clear()

            try:
                await self._process()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Processing billing entries failed")
                await self._sleep_retry()
                self._unchecked_notify.set()
            else:
                self._reset_retry_interval()

    async def _process(self) -> None:
        synced = False
        while not synced:
            synced = await self._process_one()

    async def _process_one(self) -> bool:
        sync_record = await self._storage.get_or_create_sync_record()
        entries = [
            entry
            async for entry in self._storage.iter_entries(
                with_ids_greater=sync_record.last_entry_id, limit=1
            )
        ]
        if not entries:
            return True

        last_entry_id = sync_record.last_entry_id
        for entry in entries:
            await self._perform_one(entry)
            assert entry.id is not None
            last_entry_id = entry.id

        new_record = BillingLogSyncRecord(last_entry_id=last_entry_id)
        await self._storage.update_sync_record(new_record)
        await self._entry_done_notifier.notify()
        return False

    async def _perform_one(self, entry: BillingLogEntry) -> None:
        job = await self._jobs_service.get_job(entry.job_id)

        await self._admin_client.change_user_credits(
            cluster_name=job.cluster_name,
            username=job.owner,
            credits_delta=-entry.charge,
            idempotency_key=entry.idempotency_key,
        )
        await self._jobs_service.update_job_billing(
            job_id=job.id,
            last_billed=entry.last_billed,
            new_charge=entry.charge,
            fully_billed=entry.fully_billed,
        )
