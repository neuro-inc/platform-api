import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Callable, List, Mapping, Tuple

import pytest
from neuro_auth_client import User
from typing_extensions import Protocol

from platform_api.admin_client import AdminClient
from platform_api.orchestrator.billing_log.service import (
    BillingLogService,
    BillingLogWorker,
)
from platform_api.orchestrator.billing_log.storage import (
    BillingLogEntry,
    BillingLogStorage,
    InMemoryBillingLogStorage,
)
from platform_api.orchestrator.job_request import JobRequest
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.utils.update_notifier import InMemoryNotifier, Notifier


class MockAdminClient(AdminClient):
    def __init__(self) -> None:
        self.change_log: List[Tuple[str, str, Decimal]] = []

    async def change_user_credits(
        self, cluster_name: str, username: str, delta: Decimal
    ) -> None:
        self.change_log.append((cluster_name, username, delta))


class BillingServiceFactory(Protocol):
    def __call__(self, **kwargs: Any) -> BillingLogService:
        ...


class BillingWorkerFactory(Protocol):
    def __call__(self, **kwargs: Any) -> BillingLogWorker:
        ...


class TestBillingLogProcessing:
    @pytest.fixture()
    def admin_client(self) -> MockAdminClient:
        return MockAdminClient()

    @pytest.fixture()
    def storage(self) -> BillingLogStorage:
        return InMemoryBillingLogStorage()

    @pytest.fixture()
    def new_entry(self) -> Notifier:
        return InMemoryNotifier()

    @pytest.fixture()
    def entry_done(self) -> Notifier:
        return InMemoryNotifier()

    @pytest.fixture()
    def service_factory(
        self,
        storage: BillingLogStorage,
        new_entry: Notifier,
        entry_done: Notifier,
    ) -> BillingServiceFactory:
        def _factory(**kwargs: Any) -> BillingLogService:
            ctr_kwargs: Mapping[str, Any] = {
                "storage": storage,
                "new_entry": new_entry,
                "entry_done": entry_done,
                **kwargs,
            }
            return BillingLogService(**ctr_kwargs)

        return _factory

    @pytest.fixture()
    async def service(
        self, service_factory: BillingServiceFactory
    ) -> AsyncIterator[BillingLogService]:
        async with service_factory() as service:
            yield service

    @pytest.fixture()
    def worker_factory(
        self,
        storage: BillingLogStorage,
        new_entry: Notifier,
        entry_done: Notifier,
        admin_client: AdminClient,
        jobs_service: JobsService,
    ) -> BillingWorkerFactory:
        def _factory(**kwargs: Any) -> BillingLogWorker:
            ctr_kwargs: Mapping[str, Any] = {
                "storage": storage,
                "new_entry": new_entry,
                "entry_done": entry_done,
                "admin_client": admin_client,
                "jobs_service": jobs_service,
                **kwargs,
            }
            return BillingLogWorker(**ctr_kwargs)

        return _factory

    @pytest.fixture()
    def worker(self, worker_factory: BillingWorkerFactory) -> BillingLogWorker:
        return worker_factory()

    @pytest.mark.asyncio
    async def test_syncs_old_entries(
        self,
        test_user: User,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        admin_client: MockAdminClient,
        service: BillingLogService,
        worker: BillingLogWorker,
    ) -> None:
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        last_id = await service.add_entries(
            [
                BillingLogEntry(
                    idempotency_key="key",
                    job_id=job.id,
                    charge=Decimal("1.00"),
                    fully_billed=True,
                    last_billed=datetime.now(tz=timezone.utc),
                )
            ]
        )

        async with worker:
            await asyncio.wait_for(service.wait_until_processed(last_id), timeout=1)
            updated_job = await jobs_service.get_job(job.id)
            assert updated_job.total_price_credits == Decimal("1.00")
            assert updated_job.fully_billed
            assert len(admin_client.change_log) == 1
            assert admin_client.change_log[0][0] == job.cluster_name
            assert admin_client.change_log[0][1] == job.owner
            assert -admin_client.change_log[0][2] == Decimal("1.00")

    @pytest.mark.asyncio
    async def test_syncs_new_entries(
        self,
        test_user: User,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        admin_client: MockAdminClient,
        worker: BillingLogWorker,
        service: BillingLogService,
    ) -> None:
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        async with worker:
            last_id = await service.add_entries(
                [
                    BillingLogEntry(
                        idempotency_key="key",
                        job_id=job.id,
                        charge=Decimal("1.00"),
                        fully_billed=True,
                        last_billed=datetime.now(tz=timezone.utc),
                    )
                ]
            )
            await asyncio.wait_for(service.wait_until_processed(last_id), timeout=1)
            updated_job = await jobs_service.get_job(job.id)
            assert updated_job.total_price_credits == Decimal("1.00")
            assert updated_job.fully_billed
            assert len(admin_client.change_log) == 1
            assert admin_client.change_log[0][0] == job.cluster_name
            assert admin_client.change_log[0][1] == job.owner
            assert -admin_client.change_log[0][2] == Decimal("1.00")

    @pytest.mark.asyncio
    async def test_syncs_by_timeout(
        self,
        test_user: User,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        admin_client: MockAdminClient,
        service_factory: BillingServiceFactory,
        worker_factory: BillingWorkerFactory,
    ) -> None:
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )
        worker = worker_factory(wait_timeout_s=0.1)
        service = service_factory(
            new_entry=InMemoryNotifier()  # Disconnect from worker
        )

        async with worker:
            last_id = await service.add_entries(
                [
                    BillingLogEntry(
                        idempotency_key="key",
                        job_id=job.id,
                        charge=Decimal("1.00"),
                        fully_billed=True,
                        last_billed=datetime.now(tz=timezone.utc),
                    )
                ]
            )
            await asyncio.wait_for(service.wait_until_processed(last_id), timeout=1)
            updated_job = await jobs_service.get_job(job.id)
            assert updated_job.total_price_credits == Decimal("1.00")
            assert updated_job.fully_billed
            assert len(admin_client.change_log) == 1
            assert admin_client.change_log[0][0] == job.cluster_name
            assert admin_client.change_log[0][1] == job.owner
            assert -admin_client.change_log[0][2] == Decimal("1.00")

    @pytest.mark.asyncio
    async def test_syncs_concurrent(
        self,
        test_user: User,
        jobs_service: JobsService,
        job_request_factory: Callable[[], JobRequest],
        admin_client: MockAdminClient,
        worker: BillingLogWorker,
        service: BillingLogService,
    ) -> None:
        job, _ = await jobs_service.create_job(
            job_request_factory(), test_user, cluster_name="test-cluster"
        )

        async with worker:
            tasks = [
                asyncio.create_task(
                    service.add_entries(
                        [
                            BillingLogEntry(
                                idempotency_key=f"key{index}",
                                job_id=job.id,
                                charge=Decimal("1.00"),
                                fully_billed=False,
                                last_billed=datetime.now(tz=timezone.utc),
                            )
                        ]
                    )
                )
                for index in range(10)
            ]
            last_id = max(await asyncio.gather(*tasks))
            await asyncio.wait_for(service.wait_until_processed(last_id), timeout=1)
            updated_job = await jobs_service.get_job(job.id)
            assert updated_job.total_price_credits == Decimal("10.00")
            assert not updated_job.fully_billed
            assert len(admin_client.change_log) == 10
            for admin_request in admin_client.change_log:
                assert admin_request[0] == job.cluster_name
                assert admin_request[1] == job.owner
                assert -admin_request[2] == Decimal("1.00")
