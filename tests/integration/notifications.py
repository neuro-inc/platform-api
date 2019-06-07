from typing import AsyncIterator, Tuple

import aiohttp.web
import pytest
from yarl import URL

from platform_api.config import NotificationsConfig

from .api import ApiRunner
from .conftest import ApiAddress


@pytest.fixture
async def mock_notifications_server() -> AsyncIterator[
    Tuple[ApiAddress, aiohttp.web.Application]
]:
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
    yield api_address, app
    await runner.close()


@pytest.fixture
def notifications_config(
    mock_notifications_server: Tuple[ApiAddress, aiohttp.web.Application]
) -> NotificationsConfig:
    api_address = mock_notifications_server[0]
    return NotificationsConfig(url=URL(f"http://{api_address.host}:{api_address.port}"))
