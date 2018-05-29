import asyncio
from pathlib import PurePath
from typing import NamedTuple

import aiohttp
import aiohttp.web
import pytest

from platform_api.api import create_app
from platform_api.config import Config, ServerConfig, StorageConfig


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
def config(kube_config):
    server_config = ServerConfig()
    storage_config = StorageConfig(host_mount_path=PurePath('/tmp'))  # type: ignore
    return Config(
        server=server_config,
        storage=storage_config,
        orchestrator_config=kube_config)


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
    return {
        'container':  {
            'image': 'ubuntu',
            'command': 'true',
        },
        'dataset_storage_uri': 'storage://',
        'result_storage_uri': 'storage://result',
    }


class TestModels:

    async def long_pooling(
            self, api, client, job_id: str, status: str,
            interval_s: int=2, max_attempts: int=60):
        url = api.model_base_url + f'/{job_id}'
        for _ in range(max_attempts):
            async with client.get(url) as response:
                assert response.status == 200
                result = await response.json()
                if result['status'] == status:
                    return
                await asyncio.sleep(interval_s)
        else:
            raise RuntimeError('too long')

    @pytest.mark.asyncio
    async def test_create_model(self, api, client, model_train):
        url = api.model_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == 202
            result = await response.json()
            assert result['status'] in ['pending']
            job_id = result['job_id']

        await self.long_pooling(api=api, client=client, job_id=job_id, status='succeeded')

    @pytest.mark.asyncio
    async def test_incorrect_request(self, api, client):
        json_model_train = {"wrong_key": "wrong_value"}
        url = api.model_base_url
        async with client.post(url, json=json_model_train) as response:
            assert response.status == 400
            data = await response.json()
            assert ''''container': DataError(is required)''' in data['error']

    @pytest.mark.asyncio
    async def test_broken_docker_image(self, api, client):
        payload = {
            'container':  {
                'image': 'some_broken_image',
                'command': 'true',
            },
            'dataset_storage_uri': 'storage://',
            'result_storage_uri': 'storage://result',
        }

        url = api.model_base_url
        async with client.post(url, json=payload) as response:
            assert response.status == 202
            data = await response.json()
            job_id = data['job_id']

        await self.long_pooling(api=api, client=client, job_id=job_id, status='failed')


class StatusHandler:
    @pytest.mark.asyncio
    async def test_test123(self, api, client):
        assert 1 == 1
