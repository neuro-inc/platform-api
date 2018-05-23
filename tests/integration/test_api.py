import asyncio
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
        return self.endpoint + '/models'

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

    async def long_pooling(self, api, client, job_id: str, status: str, interval_s: int=2, max_attempts: int=30):
        url = api.model_base_url + f'/{job_id}'
        for _ in range(max_attempts):
            async with client.get(url) as response:
                assert response.status == 200
                result = await response.json()
                if result['status'] == status:
                    return
                print(result)
                await asyncio.sleep(interval_s)
        else:
            raise RuntimeError('too long')

    @pytest.mark.asyncio
    async def test_create_model(self, api, client, model_train):
        url = api.model_base_url + '/train'
        async with client.post(url, json=model_train) as response:
            assert response.status == 201
            result = await response.json()
            assert result['status'] in ['pending']
            job_id = result['job_id']

        await self.long_pooling(api=api, client=client, job_id=job_id, status='succeeded')

        url = api.model_base_url + f'/{job_id}'
        async with client.delete(url, json=model_train) as response:
            assert response.status == 200
