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
    def statuses_base_url(self):
        return self.endpoint + '/statuses'

    @property
    def jobs_base_url(self):
        return self.endpoint + '/jobs'

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
        orchestrator=kube_config)


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


class JobsClient:
    def __init__(self):
        pass

    async def get_all_jobs(self):
        url = api.jobs_base_url
        async with client.get(url) as response:
            assert response.status == 200
            result = await response.json()
        return result['jobs']

    async def long_pooling_by_status_id(
            self, api, client, status_id: str, status: str,
            interval_s: int=2, max_attempts: int=60):
        url = api.statuses_base_url + f'/{status_id}'
        for _ in range(max_attempts):
            async with client.get(url) as response:
                assert response.status == 200
                result = await response.json()
                if result['status'] == status:
                    return
                await asyncio.sleep(interval_s)
        else:
            raise RuntimeError('too long')

    async def long_pooling_by_job_id(
            self, api, client, job_id: str, status: str,
            interval_s: int=2, max_attempts: int=60):
        url = api.jobs_base_url + f'/{job_id}/status'
        for _ in range(max_attempts):
            async with client.get(url) as response:
                assert response.status == 200
                result = await response.json()
                if result['status'] == status:
                    return
                await asyncio.sleep(interval_s)
        else:
            raise RuntimeError('too long')

    async def delete_job(
            self, api, client, job_id: str):
        url = api.jobs_base_url + f'/delete/{job_id}'
        async with client.delete(url) as response:
            assert response.status == 200
            result = await response.json()
            print(f"delete result {result}")


@pytest.fixture
async def jobs_client():
    return JobsClient()


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
            'resources': {
                'cpu': 0.1,
                'memory_mb': 16,
            },
        },
        'dataset_storage_uri': 'storage://',
        'result_storage_uri': 'storage://result',
    }


class TestModels:

    @pytest.mark.asyncio
    async def test_create_model(self, api, client, model_train, jobs_client):
        url = api.model_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == 202
            result = await response.json()
            assert result['status'] in ['pending']
            status_id = result['status_id']
            job_id = result['job_id']
        await jobs_client.long_pooling_by_status_id(
            api=api, client=client, status_id=status_id, status='succeeded')
        await jobs_client.delete_job(api=api, client=client, job_id=job_id)

    @pytest.mark.asyncio
    async def test_env_var_sourcing(self, api, client, jobs_client):
        cmd = 'bash -c \'[ "$NP_RESULT_PATH" == "/var/storage/result" ]\''
        payload = {
            'container':  {
                'image': 'ubuntu',
                'command': cmd,
                'resources': {
                    'cpu': 0.1,
                    'memory_mb': 16,
                },
            },
            'dataset_storage_uri': 'storage://',
            'result_storage_uri': 'storage://result',
        }
        url = api.model_base_url
        async with client.post(url, json=payload) as response:
            assert response.status == 202
            result = await response.json()
            assert result['status'] in ['pending']
            status_id = result['status_id']
            job_id = result['job_id']
        await jobs_client.long_pooling_by_status_id(
            api=api, client=client, status_id=status_id, status='succeeded')
        await jobs_client.delete_job(api=api, client=client, job_id=job_id)

    @pytest.mark.asyncio
    async def test_incorrect_request(self, api, client):
        json_model_train = {"wrong_key": "wrong_value"}
        url = api.model_base_url
        async with client.post(url, json=json_model_train) as response:
            assert response.status == 400
            data = await response.json()
            assert ''''container': DataError(is required)''' in data['error']

    @pytest.mark.asyncio
    async def test_broken_docker_image(self, api, client, jobs_client):
        payload = {
            'container':  {
                'image': 'some_broken_image',
                'command': 'true',
                'resources': {
                    'cpu': 0.1,
                    'memory_mb': 16,
                },
            },
            'dataset_storage_uri': 'storage://',
            'result_storage_uri': 'storage://result',
        }

        url = api.model_base_url
        async with client.post(url, json=payload) as response:
            assert response.status == 202
            data = await response.json()
            status_id = data['status_id']

        await jobs_client.long_pooling_by_status_id(
            api=api, client=client, status_id=status_id, status='failed')


class TestStatuses:
    @pytest.mark.asyncio
    async def test_test_not_exist_status(self, api, client):
        status_id = 'not-such-status_id'
        url = api.statuses_base_url + f'/{status_id}'
        async with client.get(url) as response:
            assert response.status == 404
            data = await response.json()
            assert f'not such status_id {status_id}' == data['error']


class TestJobs:
    @pytest.mark.asyncio
    async def get_all_jobs(self):
        pass

    @pytest.mark.asyncio
    async def test_delete_job(self, api, client, model_train, jobs_client):
        url = api.model_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == 202
            result = await response.json()
            assert result['status'] in ['pending']
            job_id = result['job_id']

        # await jobs_client.long_pooling_by_job_id(api=api, client=client, job_id=job_id, status='succeeded')
        # url = api.jobs_base_url
        # async with client.get(url) as response:
        #     assert response.status == 200
        #     result = await response.json()
        #     print(result)
        #
        # url = api.jobs_base_url + f'/{job_id}/status'
        # async with client.get(url) as response:
        #     assert response.status == 200
        #     result = await response.json()
        #     print(result)


    # @pytest.mark.asyncio
    # async def test_get_jobs_return_corrects_id(self, api, client, model_train):
    #     jobs_ids = []
    #     n_jobs = 3
    #     for _ in range(n_jobs):
    #         url = api.model_base_url
    #         async with client.post(url, json=model_train) as response:
    #             assert response.status == 202
    #             result = await response.json()
    #             assert result['status'] in ['pending']
    #             jobs_ids.append(result['job_id'])
    #
    #     url = api.jobs_base_url
    #     async with client.get(url) as response:
    #         assert response.status == 200
    #         result = await response.json()
    #     assert set(jobs_ids) <= {x['job_id'] for x in result['jobs']}

        # for job_id in jobs_ids:
        #     url = api.jobs_base_url + f'/delete/{job_id}'
        #     async with client.delete(url) as response:
        #         assert response.status == 200
        #         result = await response.json()
        #         assert result['status'] == 'pending'


    #
    # @pytest.mark.asyncio
    # async def test_delete_not_exist(self, api, client):
    #     job_id = 'kdfghlksjd-jhsdbljh-3456789!@'
    #     url = api.jobs_base_url + f'/delete/{job_id}'
    #     async with client.delete(url) as response:
    #         assert response.status == 400
    #         result = await response.json()
    #         assert result['error'] == f'not such job_id {job_id}'
