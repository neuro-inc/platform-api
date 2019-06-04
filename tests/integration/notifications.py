from typing import Any, AsyncIterator, Callable, List

import pytest
from aiohttp import web

from platform_api.api import create_app
from platform_api.config import Config, NotificationsConfig

from . import _TestServer, _TestServerFactory
from .conftest import ApiRunner
from .test_api import ApiConfig, get_cluster_configs


@pytest.fixture
def received_notifications() -> List[Any]:
    return []


@pytest.fixture
async def notificationsapi_server(
    aiohttp_server: _TestServerFactory, received_notifications: List[Any]
) -> _TestServer:
    async def handler(request: web.Request) -> web.Response:
        await request.read()
        data = await request.json()
        received_notifications.append({type: request.match_info["type"], data: data})
        raise web.HTTPCreated()

    app = web.Application()
    app.router.add_get("/notifications/{type}", handler)
    srv = await aiohttp_server(app)
    return srv


@pytest.fixture
def config_with_notifications(
    config_factory: Callable[..., Config], notificationsapi_server: _TestServer
) -> Config:
    return config_factory(
        notifications=NotificationsConfig(url=notificationsapi_server.make_url("/"))
    )


@pytest.fixture
async def api_with_notifications(
    config_with_notifications: Config
) -> AsyncIterator[ApiConfig]:
    app = await create_app(config_with_notifications, get_cluster_configs())
    runner = ApiRunner(app, port=8081)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port)
    yield api_config
    await runner.close()
