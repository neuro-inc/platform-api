import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.orchestrator.billing_log.storage import (
    BillingLogStorage,
    PostgresBillingLogStorage,
)
from tests.unit.test_billing_log_storage import TestBillingLogStorage


class TestPostgresBillingLogStorage(TestBillingLogStorage):
    @pytest.fixture()
    def storage(  # type: ignore
        self,
        sqalchemy_engine: AsyncEngine,
    ) -> BillingLogStorage:
        return PostgresBillingLogStorage(sqalchemy_engine)
