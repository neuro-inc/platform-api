from typing import NamedTuple

import aiohttp
import aiohttp.web
import pytest

from platform_api.api import create_app
from platform_api.config import Config, ServerConfig


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self):
        return f'http://{self.host}:{self.port}/api/v1'

    @property
    def model_base_url(self):
        return self.endpoint + '/models/'

    @property
    def ping_url(self):
        return self.endpoint + '/ping'


@pytest.fixture
def config():
    server_config = ServerConfig()
    return Config(server=server_config)


@pytest.fixture
async def api(config):
    app = await create_app(config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host='0.0.0.0', port=8080)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest.fixture
async def client():
    async with aiohttp.ClientSession() as session:
        yield session


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api, client):
        async with client.get(api.ping_url) as response:
            assert response.status == 200


@pytest.fixture
async def model_train():
    r = {"container":  {"image": "truskovskyi/test"}}
    return r


class TestModels:
    @pytest.mark.asyncio
    async def test_post(self, api, client, model_train):
        url = api.model_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == 200

    @pytest.mark.asyncio
    async def test_get(self, api, client):
        url = api.model_base_url
        async with client.get(url) as response:
            assert response.status == 200
