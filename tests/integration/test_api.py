import asyncio
from pathlib import PurePath
from typing import NamedTuple
from unittest import mock

import aiohttp
import aiohttp.web
import pytest
from aiohttp.web import HTTPAccepted, HTTPBadRequest, HTTPNoContent, HTTPOk

from platform_api.api import create_app
from platform_api.config import Config, DatabaseConfig, ServerConfig, StorageConfig


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self):
        return f"http://{self.host}:{self.port}/api/v1"

    @property
    def model_base_url(self):
        return self.endpoint + "/models"

    @property
    def jobs_base_url(self):
        return self.endpoint + "/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"

    @property
    def ping_url(self):
        return self.endpoint + "/ping"


@pytest.fixture
def config(kube_config, redis_config, auth_config):
    server_config = ServerConfig()
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
    database_config = DatabaseConfig(redis=redis_config)  # type: ignore
    return Config(
        server=server_config,
        storage=storage_config,
        orchestrator=kube_config,
        database=database_config,
        auth=auth_config,
    )


@pytest.fixture
async def api(config):
    app = await create_app(config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8080)
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

    async def get_all_jobs(self, api, client):
        url = api.jobs_base_url
        async with client.get(url) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        return result["jobs"]

    async def long_pooling_by_job_id(
        self,
        api,
        client,
        job_id: str,
        status: str,
        interval_s: int = 2,
        max_attempts: int = 60,
    ):
        url = api.generate_job_url(job_id)
        for _ in range(max_attempts):
            async with client.get(url) as response:
                response_text = await response.text()
                assert response.status == HTTPOk.status_code, response_text
                result = await response.json()
                if result["status"] == status:
                    return result
                await asyncio.sleep(interval_s)
        else:
            raise RuntimeError("too long")

    async def delete_job(self, api, client, job_id: str):
        url = api.generate_job_url(job_id)
        async with client.delete(url) as response:
            assert response.status == HTTPNoContent.status_code


@pytest.fixture
async def jobs_client():
    return JobsClient()


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api, client):
        async with client.get(api.ping_url) as response:
            assert response.status == HTTPOk.status_code


@pytest.fixture
async def model_train():
    return {
        "container": {
            "image": "ubuntu",
            "command": "true",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "http": {"port": 1234},
        },
        "dataset_storage_uri": "storage://",
        "result_storage_uri": "storage://result",
    }


class TestModels:
    @pytest.mark.asyncio
    async def test_create_model(
        self, api, client, model_train, jobs_client, regular_user
    ):
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=model_train
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
            expected_url = f"http://{job_id}.jobs.platform.neuromation.io"
            assert result["http_url"] == expected_url

        await jobs_client.long_pooling_by_job_id(
            api=api, client=client, job_id=job_id, status="succeeded"
        )
        await jobs_client.delete_job(api=api, client=client, job_id=job_id)

    @pytest.mark.asyncio
    async def test_env_var_sourcing(self, api, client, jobs_client, regular_user):
        cmd = 'bash -c \'[ "$NP_RESULT_PATH" == "/var/storage/result" ]\''
        payload = {
            "container": {
                "image": "ubuntu",
                "command": cmd,
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": "storage://",
            "result_storage_uri": "storage://result",
        }
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
        await jobs_client.long_pooling_by_job_id(
            api=api, client=client, job_id=job_id, status="succeeded"
        )
        await jobs_client.delete_job(api=api, client=client, job_id=job_id)

    @pytest.mark.asyncio
    async def test_incorrect_request(self, api, client, regular_user):
        json_model_train = {"wrong_key": "wrong_value"}
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=json_model_train
        ) as response:
            assert response.status == HTTPBadRequest.status_code
            data = await response.json()
            assert """'container': DataError(is required)""" in data["error"]

    @pytest.mark.asyncio
    async def test_broken_docker_image(self, api, client, jobs_client, regular_user):
        payload = {
            "container": {
                "image": "some_broken_image",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": "storage://",
            "result_storage_uri": "storage://result",
        }

        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            data = await response.json()
            job_id = data["job_id"]
        await jobs_client.long_pooling_by_job_id(
            api=api, client=client, job_id=job_id, status="failed"
        )


class TestJobs:
    @pytest.mark.asyncio
    async def test_get_all_jobs_clear(self, jobs_client, api, client):
        jobs = await jobs_client.get_all_jobs(api=api, client=client)
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_jobs_return_corrects_id(
        self, jobs_client, api, client, model_train, regular_user
    ):
        jobs_ids = []
        n_jobs = 2
        for _ in range(n_jobs):
            url = api.model_base_url
            async with client.post(
                url, headers=regular_user.headers, json=model_train
            ) as response:
                assert response.status == HTTPAccepted.status_code
                result = await response.json()
                assert result["status"] in ["pending"]
                job_id = result["job_id"]
                await jobs_client.long_pooling_by_job_id(
                    api=api, client=client, job_id=job_id, status="succeeded"
                )
                jobs_ids.append(job_id)

        jobs = await jobs_client.get_all_jobs(api=api, client=client)
        assert set(jobs_ids) <= {x["id"] for x in jobs}
        # clean
        for job in jobs:
            await jobs_client.delete_job(api=api, client=client, job_id=job["id"])

    @pytest.mark.asyncio
    async def test_delete_job(
        self, api, client, model_train, jobs_client, regular_user
    ):
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=model_train
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
            await jobs_client.long_pooling_by_job_id(
                api=api, client=client, job_id=job_id, status="succeeded"
            )
        await jobs_client.delete_job(api=api, client=client, job_id=job_id)

        jobs = await jobs_client.get_all_jobs(api=api, client=client)
        assert len(jobs) == 1
        assert jobs[0]["status"] == "succeeded"
        assert jobs[0]["id"] == job_id

    @pytest.mark.asyncio
    async def test_delete_not_exist(self, api, client):
        job_id = "kdfghlksjd-jhsdbljh-3456789!@"
        url = api.jobs_base_url + f"/{job_id}"
        async with client.delete(url) as response:
            assert response.status == HTTPBadRequest.status_code
            result = await response.json()
            assert result["error"] == f"no such job {job_id}"

    @pytest.mark.asyncio
    async def test_job_log(self, api, client, regular_user):
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        payload = {
            "container": {
                "image": "ubuntu",
                "command": command,
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": "storage://",
            "result_storage_uri": "storage://result",
        }
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]

        job_log_url = api.jobs_base_url + f"/{job_id}/log"
        async with client.get(job_log_url) as response:
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_create_validation_failure(self, api, client):
        request_payload = {}
        async with client.post(api.jobs_base_url, json=request_payload) as response:
            assert response.status == HTTPBadRequest.status_code
            response_payload = await response.json()
            assert response_payload == {"error": mock.ANY}
            assert "is required" in response_payload["error"]

    @pytest.mark.asyncio
    async def test_create_with_custom_volumes(
        self, jobs_client, api, client, regular_user
    ):
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": "storage://",
                        "dst_path": "/var/storage",
                        "read_only": False,
                    }
                ],
            }
        }

        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPAccepted.status_code, response_text
            response_payload = await response.json()
            assert response_payload == {
                "id": mock.ANY,
                "status": "pending",
                "history": {
                    "status": "pending",
                    "reason": None,
                    "description": None,
                    "created_at": mock.ANY,
                },
                "container": {
                    "command": "true",
                    "env": {},
                    "image": "ubuntu",
                    "resources": {"cpu": 0.1, "memory_mb": 16},
                    "volumes": [
                        {
                            "dst_path": "/var/storage",
                            "read_only": False,
                            "src_storage_uri": "storage:",
                        }
                    ],
                },
            }
            job_id = response_payload["id"]

        response_payload = await jobs_client.long_pooling_by_job_id(
            api=api, client=client, job_id=job_id, status="succeeded"
        )

        assert response_payload == {
            "id": job_id,
            "status": "succeeded",
            "history": {
                "status": "succeeded",
                "reason": None,
                "description": None,
                "created_at": mock.ANY,
                "started_at": mock.ANY,
                "finished_at": mock.ANY,
            },
            "container": {
                "command": "true",
                "env": {},
                "image": "ubuntu",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "dst_path": "/var/storage",
                        "read_only": False,
                        "src_storage_uri": "storage:",
                    }
                ],
            },
        }

    @pytest.mark.asyncio
    async def test_job_failed(self, jobs_client, api, client, regular_user):
        command = 'bash -c "echo Failed!; false"'
        payload = {
            "container": {
                "image": "ubuntu",
                "command": command,
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": "storage://",
            "result_storage_uri": "storage://result",
        }
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] == "pending"
            job_id = result["job_id"]

        response_payload = await jobs_client.long_pooling_by_job_id(
            api=api, client=client, job_id=job_id, status="failed"
        )

        assert response_payload == {
            "id": job_id,
            "status": "failed",
            "history": {
                "status": "failed",
                "reason": "Error",
                "description": "Failed!\n\nExit code: 1",
                "created_at": mock.ANY,
                "started_at": mock.ANY,
                "finished_at": mock.ANY,
            },
            "container": {
                "command": 'bash -c "echo Failed!; false"',
                "env": {
                    "NP_DATASET_PATH": "/var/storage",
                    "NP_RESULT_PATH": "/var/storage/result",
                },
                "image": "ubuntu",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "dst_path": "/var/storage",
                        "read_only": True,
                        "src_storage_uri": "storage:",
                    },
                    {
                        "dst_path": "/var/storage/result",
                        "read_only": False,
                        "src_storage_uri": "storage://result",
                    },
                ],
            },
        }
