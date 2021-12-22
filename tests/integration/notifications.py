from collections.abc import AsyncIterator
from typing import Any, NamedTuple

import aiohttp.web
import pytest
from yarl import URL

from platform_api.config import NotificationsConfig

from .api import ApiRunner
from .conftest import ApiAddress


class NotificationsServer(NamedTuple):
    address: ApiAddress
    app: aiohttp.web.Application

    @property
    def url(self) -> URL:
        return URL(f"http://{self.address.host}:{self.address.port}")

    @property
    def requests(self) -> tuple[tuple[str, Any]]:
        return tuple(request for request in self.app["requests"])  # type: ignore


@pytest.fixture
async def mock_notifications_server() -> AsyncIterator[NotificationsServer]:
    async def _notify(request: aiohttp.web.Request) -> aiohttp.web.Response:
        type = request.match_info["type"]
        payload = await request.json()
        app["requests"].append((type, payload))
        raise aiohttp.web.HTTPCreated

    def _create_app() -> aiohttp.web.Application:
        app = aiohttp.web.Application()
        app["requests"] = []
        app.router.add_routes(
            [aiohttp.web.post("/api/v1/notifications/{type}", _notify)]
        )
        return app

    app = _create_app()
    runner = ApiRunner(app, port=8083)
    api_address = await runner.run()
    yield NotificationsServer(address=api_address, app=app)
    await runner.close()


@pytest.fixture
def notifications_config(
    mock_notifications_server: NotificationsServer,
) -> NotificationsConfig:
    return NotificationsConfig(url=mock_notifications_server.url, token="token")
