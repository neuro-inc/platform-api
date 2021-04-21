from contextlib import asynccontextmanager
from decimal import Decimal
from typing import AsyncIterator

import aiohttp.web
import pytest
from yarl import URL

from platform_api.admin_client import AdminClient

from .conftest import ApiRunner


async def create_admin_app(
    cluster_name: str, username: str, amount: Decimal, key: str
) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    async def handle(request: aiohttp.web.Request) -> aiohttp.web.Response:

        assert request.match_info["cname"] == cluster_name
        assert request.match_info["uname"] == username
        payload = await request.json()
        assert payload["additional_quota"]["credits"] == str(amount)
        assert request.query.get("idempotency_key") == key
        return aiohttp.web.Response()

    app.add_routes(
        (aiohttp.web.patch("/api/v1/clusters/{cname}/users/{uname}/quota", handle),)
    )

    return app


@asynccontextmanager
async def create_admin_api(
    cluster_name: str, username: str, amount: Decimal, key: str
) -> AsyncIterator[URL]:
    app = await create_admin_app(cluster_name, username, amount, key)
    runner = ApiRunner(app, port=8085)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()


class TestAdminClient:
    @pytest.mark.asyncio
    async def test_patch_user_credits(
        self,
    ) -> None:
        cluster_name = "test-cluster"
        username = "username"
        amount = Decimal("20.11")
        key = "key"
        async with create_admin_api(cluster_name, username, amount, key) as url:
            async with AdminClient(base_url=url) as client:
                await client.change_user_credits(cluster_name, username, amount, key)
