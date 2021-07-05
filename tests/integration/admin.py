from dataclasses import dataclass
from decimal import Decimal
from typing import AsyncIterator, NamedTuple, Optional, Sequence

import aiohttp.web
import pytest
from yarl import URL

from .api import ApiRunner
from .conftest import ApiAddress


@dataclass(frozen=True)
class AdminChargeRequest:
    idempotency_key: Optional[str]
    cluster_name: str
    username: str
    amount: Decimal


@dataclass(frozen=True)
class AdminDebtRequest:
    idempotency_key: Optional[str]
    cluster_name: str
    username: str
    amount: Decimal


class AdminServer(NamedTuple):
    address: ApiAddress
    app: aiohttp.web.Application

    @property
    def url(self) -> URL:
        return URL(f"http://{self.address.host}:{self.address.port}/api/v1/")

    @property
    def requests(self) -> Sequence[AdminChargeRequest]:
        return [request for request in self.app["requests"]]


@pytest.fixture
async def mock_admin_server() -> AsyncIterator[AdminServer]:
    async def _handle_quota_patch(request: aiohttp.web.Request) -> aiohttp.web.Response:
        cluster_name = request.match_info["cname"]
        username = request.match_info["uname"]
        payload = await request.json()
        amount = Decimal(payload["additional_quota"]["credits"])
        idempotency_key = request.query.get("idempotency_key")
        app["requests"].append(
            AdminChargeRequest(
                idempotency_key=idempotency_key,
                cluster_name=cluster_name,
                username=username,
                amount=amount,
            )
        )
        return aiohttp.web.Response()

    async def _handle_add_debt(request: aiohttp.web.Request) -> aiohttp.web.Response:
        cluster_name = request.match_info["cname"]
        payload = await request.json()
        username = payload["user_name"]
        amount = Decimal(payload["credits"])
        idempotency_key = request.query.get("idempotency_key")
        app["requests"].append(
            AdminDebtRequest(
                idempotency_key=idempotency_key,
                cluster_name=cluster_name,
                username=username,
                amount=amount,
            )
        )
        return aiohttp.web.Response()

    def _create_app() -> aiohttp.web.Application:
        app = aiohttp.web.Application()
        app["requests"] = []
        app.router.add_routes(
            (
                aiohttp.web.patch(
                    "/api/v1/clusters/{cname}/users/{uname}/quota", _handle_quota_patch
                ),
                aiohttp.web.post("/api/v1/clusters/{cname}/debts", _handle_add_debt),
            )
        )
        return app

    app = _create_app()
    runner = ApiRunner(app, port=8085)
    api_address = await runner.run()
    yield AdminServer(address=api_address, app=app)
    await runner.close()


@pytest.fixture
def admin_url(
    mock_admin_server: AdminServer,
) -> URL:
    return mock_admin_server.url
