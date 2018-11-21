import asyncio
from pathlib import PurePath
from typing import NamedTuple
from unittest import mock

import aiohttp
import aiohttp.web
import pytest
from aiohttp.web import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPNoContent,
    HTTPOk,
    HTTPUnauthorized,
)
from neuro_auth_client import Permission

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
    def __init__(self, api_config, client, headers):
        self._api_config = api_config
        self._client = client
        self._headers = headers

    async def get_all_jobs(self):
        url = self._api_config.jobs_base_url
        async with self._client.get(url, headers=self._headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        return result["jobs"]

    async def get_job_by_id(self, job_id: str):
        url = self._api_config.generate_job_url(job_id)
        async with self._client.get(url, headers=self._headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        return result

    async def long_polling_by_job_id(
        self, job_id: str, status: str, interval_s: int = 2, max_attempts: int = 60
    ):
        for _ in range(max_attempts):
            response = await self.get_job_by_id(job_id)
            if response["status"] == status:
                return response
            await asyncio.sleep(interval_s)
        else:
            raise RuntimeError("too long")

    async def delete_job(self, job_id: str):
        url = self._api_config.generate_job_url(job_id)
        async with self._client.delete(url, headers=self._headers) as response:
            assert response.status == HTTPNoContent.status_code


@pytest.fixture
async def jobs_client(api, client, regular_user):
    return JobsClient(api, client, headers=regular_user.headers)


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api, client):
        async with client.get(api.ping_url) as response:
            assert response.status == HTTPOk.status_code


@pytest.fixture
async def model_request_factory():
    def _factory(owner: str):
        return {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "http": {"port": 1234},
            },
            "dataset_storage_uri": f"storage://{owner}",
            "result_storage_uri": f"storage://{owner}/result",
        }

    return _factory


@pytest.fixture
async def model_train(model_request_factory, regular_user):
    return model_request_factory(regular_user.name)


class TestModels:
    @pytest.mark.asyncio
    async def test_create_model_unauthorized(self, api, client, model_train):
        url = api.model_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == HTTPUnauthorized.status_code

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
            job_id = result["job_id"]
            assert result["status"] in ["pending"]
            expected_url = f"http://{job_id}.jobs.platform.neuromation.io"
            assert result["http_url"] == expected_url
            expected_internal_hostname = f"{job_id}.default"
            assert result["internal_hostname"] == expected_internal_hostname

        retrieved_job = await jobs_client.get_job_by_id(job_id=job_id)
        assert retrieved_job["internal_hostname"] == expected_internal_hostname

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_model_with_ssh_and_http(
        self, api, client, model_train, jobs_client, regular_user
    ):
        url = api.model_base_url
        model_train["container"]["ssh"] = {"port": 7867}
        async with client.post(
            url, headers=regular_user.headers, json=model_train
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
            expected_url = f"ssh://{job_id}.ssh.platform.neuromation.io:22"
            assert result["ssh_server"] == expected_url

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_model_with_ssh_only(
        self, api, client, model_train, jobs_client, regular_user
    ):
        url = api.model_base_url
        model_train["container"]["ssh"] = {"port": 7867}
        model_train["container"].pop("http", None)
        async with client.post(
            url, headers=regular_user.headers, json=model_train
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
            expected_url = f"ssh://{job_id}.ssh.platform.neuromation.io:22"
            assert result["ssh_server"] == expected_url

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_unknown_gpu_model(
        self, jobs_client, api, client, regular_user, kube_node_gpu
    ):
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "gpu": 1,
                    "gpu_model": "unknown",
                },
            },
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }

        async with client.post(
            api.model_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPBadRequest.status_code, response_text
            data = await response.json()
            assert """'gpu_model': DataError(value doesn't match""" in data["error"]

    @pytest.mark.asyncio
    async def test_create_gpu_model(
        self, jobs_client, api, client, regular_user, kube_node_gpu, kube_client
    ):
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "gpu": 1,
                    "gpu_model": "gpumodel",
                },
            },
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }

        async with client.post(
            api.model_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["job_id"]

        await kube_client.wait_pod_scheduled(job_id, kube_node_gpu)

    @pytest.mark.asyncio
    async def test_env_var_sourcing(self, api, client, jobs_client, regular_user):
        np_result_path = f"/var/storage/{regular_user.name}/result"
        cmd = f'bash -c \'[ "$NP_RESULT_PATH" == "{np_result_path}" ]\''
        payload = {
            "container": {
                "image": "ubuntu",
                "command": cmd,
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["job_id"]
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

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
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }

        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            data = await response.json()
            job_id = data["job_id"]
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="failed")

    @pytest.mark.asyncio
    async def test_forbidden_storage_uris(self, api, client, jobs_client, regular_user):
        payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": f"storage://",
            "result_storage_uri": f"storage://result",
        }

        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_forbidden_image(self, api, client, jobs_client, regular_user):
        payload = {
            "container": {
                "image": f"registry.dev.neuromation.io/anotheruser/image:tag",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }

        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_allowed_image(self, api, client, jobs_client, regular_user):
        payload = {
            "container": {
                "image": f"registry.dev.neuromation.io/{regular_user.name}/image:tag",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            },
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }

        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["job_id"]
        await jobs_client.delete_job(job_id=job_id)


class TestJobs:
    @pytest.mark.asyncio
    async def test_create_job_unauthorized_no_token(self, api, client, model_train):
        url = api.jobs_base_url
        async with client.post(url, json=model_train) as response:
            assert response.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_create_job_unauthorized_invalid_token(
        self, api, client, model_train
    ):
        url = api.jobs_base_url
        headers = {"Authorization": "Bearer INVALID"}
        async with client.post(url, headers=headers, json=model_train) as response:
            assert response.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_get_all_jobs_clear(self, jobs_client):
        jobs = await jobs_client.get_all_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_all_jobs_shared(
        self,
        jobs_client,
        api,
        client,
        model_request_factory,
        regular_user_factory,
        auth_client,
    ):
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.model_base_url
        model_request = model_request_factory(owner.name)
        async with client.post(
            url, headers=owner.headers, json=model_request
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            job_id = result["job_id"]

        url = api.jobs_base_url
        async with client.get(url, headers=owner.headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            result = await response.json()
            job_ids = {item["id"] for item in result["jobs"]}
            assert job_ids == {job_id}

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPOk.status_code
            result = await response.json()
            assert not result["jobs"]

        permission = Permission(uri=f"job://{owner.name}/{job_id}", action="read")
        await auth_client.grant_user_permissions(
            follower.name, [permission], token=owner.token
        )

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPOk.status_code

    @pytest.mark.asyncio
    async def test_get_shared_job(
        self,
        jobs_client,
        api,
        client,
        model_request_factory,
        regular_user_factory,
        auth_client,
    ):
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.model_base_url
        model_request = model_request_factory(owner.name)
        async with client.post(
            url, headers=owner.headers, json=model_request
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            job_id = result["job_id"]

        url = f"{api.jobs_base_url}/{job_id}"
        async with client.get(url, headers=owner.headers) as response:
            assert response.status == HTTPOk.status_code

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPForbidden.status_code

        permission = Permission(uri=f"job://{owner.name}/{job_id}", action="read")
        await auth_client.grant_user_permissions(
            follower.name, [permission], token=owner.token
        )

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPOk.status_code

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
                await jobs_client.long_polling_by_job_id(
                    job_id=job_id, status="succeeded"
                )
                jobs_ids.append(job_id)

        jobs = await jobs_client.get_all_jobs()
        assert set(jobs_ids) <= {x["id"] for x in jobs}
        # clean
        for job in jobs:
            await jobs_client.delete_job(job_id=job["id"])

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
            await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

        jobs = await jobs_client.get_all_jobs()
        assert len(jobs) == 1
        assert jobs[0]["status"] == "succeeded"
        assert jobs[0]["id"] == job_id

    @pytest.mark.asyncio
    async def test_delete_not_exist(self, api, client, regular_user):
        job_id = "kdfghlksjd-jhsdbljh-3456789!@"
        url = api.jobs_base_url + f"/{job_id}"
        async with client.delete(url, headers=regular_user.headers) as response:
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
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
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
        async with client.get(job_log_url, headers=regular_user.headers) as response:
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_create_validation_failure(self, api, client, regular_user):
        request_payload = {}
        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
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
                        "src_storage_uri": f"storage://{regular_user.name}",
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
            job_id = response_payload["id"]
            assert response_payload == {
                "id": mock.ANY,
                "owner": regular_user.name,
                "internal_hostname": f"{job_id}.default",
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
                            "src_storage_uri": f"storage://{regular_user.name}",
                        }
                    ],
                },
            }

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="succeeded"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "internal_hostname": f"{job_id}.default",
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
                        "src_storage_uri": f"storage://{regular_user.name}",
                    }
                ],
            },
            "is_preemptible": False,
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
            "dataset_storage_uri": f"storage://{regular_user.name}",
            "result_storage_uri": f"storage://{regular_user.name}/result",
        }
        url = api.model_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] == "pending"
            job_id = result["job_id"]

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="failed"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "status": "failed",
            "internal_hostname": f"{job_id}.default",
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
                    "NP_DATASET_PATH": f"/var/storage/{regular_user.name}",
                    "NP_RESULT_PATH": f"/var/storage/{regular_user.name}/result",
                },
                "image": "ubuntu",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "dst_path": f"/var/storage/{regular_user.name}",
                        "read_only": True,
                        "src_storage_uri": f"storage://{regular_user.name}",
                    },
                    {
                        "dst_path": f"/var/storage/{regular_user.name}/result",
                        "read_only": False,
                        "src_storage_uri": f"storage://{regular_user.name}/result",
                    },
                ],
            },
            "is_preemptible": False,
        }

    @pytest.mark.asyncio
    async def test_create_unknown_gpu_model(
        self, jobs_client, api, client, regular_user, kube_node_gpu
    ):
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "gpu": 1,
                    "gpu_model": "unknown",
                },
            }
        }

        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPBadRequest.status_code, response_text
            data = await response.json()
            assert """'gpu_model': DataError(value doesn't match""" in data["error"]

    @pytest.mark.asyncio
    async def test_create_gpu_model(
        self, jobs_client, api, client, regular_user, kube_node_gpu, kube_client
    ):
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "gpu": 1,
                    "gpu_model": "gpumodel",
                },
            }
        }

        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPAccepted.status_code, response_text
            response_payload = await response.json()
            job_id = response_payload["id"]
            assert response_payload == {
                "id": mock.ANY,
                "owner": regular_user.name,
                "internal_hostname": f"{job_id}.default",
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
                    "resources": {
                        "cpu": 0.1,
                        "memory_mb": 16,
                        "gpu": 1,
                        "gpu_model": "gpumodel",
                    },
                    "volumes": [],
                },
                "is_preemptible": False,
            }

        await kube_client.wait_pod_scheduled(job_id, kube_node_gpu)
