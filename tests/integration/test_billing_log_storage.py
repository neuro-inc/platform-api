import pytest
from asyncpg import Pool

from platform_api.orchestrator.billing_log.storage import (
    BillingLogStorage,
    PostgresBillingLogStorage,
)
from tests.unit.test_billing_log_storage import TestBillingLogStorage


class TestPostgresBillingLogStorage(TestBillingLogStorage):
    @pytest.fixture()
    def storage(self, postgres_pool: Pool) -> BillingLogStorage:  # type: ignore
        return PostgresBillingLogStorage(postgres_pool)
