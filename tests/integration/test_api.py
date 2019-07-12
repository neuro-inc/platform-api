import json
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Iterator
from unittest import mock

import aiohttp
import aiohttp.web
import multidict
import pytest
from aiohttp.web import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPForbidden,
    HTTPInternalServerError,
    HTTPOk,
    HTTPUnauthorized,
)
from neuro_auth_client import Permission
from neuro_auth_client.client import Quota

from tests.conftest import random_str

from .api import ApiConfig, JobsClient
from .auth import _AuthClient, _User
from .conftest import MyKubeClient


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api: ApiConfig, client: aiohttp.ClientSession) -> None:
        async with client.get(api.ping_url) as response:
            assert response.status == HTTPOk.status_code

    @pytest.mark.asyncio
    async def test_config_unauthorized(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        url = api.config_url
        async with client.get(url) as resp:
            assert resp.status == HTTPOk.status_code
            result = await resp.json()
            assert result == {}

    @pytest.mark.asyncio
    async def test_config(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.config_url
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code
            result = await resp.json()
            assert result == {
                "registry_url": "https://registry.dev.neuromation.io",
                "storage_url": "https://neu.ro/api/v1/storage",
                "users_url": "https://neu.ro/api/v1/users",
                "monitoring_url": "https://neu.ro/api/v1/monitoring",
                "resource_presets": [
                    {
                        "name": "gpu-small",
                        "cpu": 7,
                        "memory_mb": 30720,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "gpu-large",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                    },
                    {"name": "cpu-small", "cpu": 2, "memory_mb": 2048},
                    {"name": "cpu-large", "cpu": 3, "memory_mb": 14336},
                ],
            }

    @pytest.mark.asyncio
    async def test_config_with_oauth(
        self,
        api_with_oauth: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
    ) -> None:
        url = api_with_oauth.config_url
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code
            result = await resp.json()
            assert result == {
                "registry_url": "https://registry.dev.neuromation.io",
                "storage_url": "https://neu.ro/api/v1/storage",
                "users_url": "https://neu.ro/api/v1/users",
                "monitoring_url": "https://neu.ro/api/v1/monitoring",
                "resource_presets": [
                    {
                        "name": "gpu-small",
                        "cpu": 7,
                        "memory_mb": 30720,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "gpu-large",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                    },
                    {"name": "cpu-small", "cpu": 2, "memory_mb": 2048},
                    {"name": "cpu-large", "cpu": 3, "memory_mb": 14336},
                ],
                "auth_url": "https://platform-auth0-url/authorize",
                "token_url": "https://platform-auth0-url/oauth/token",
                "client_id": "client_id",
                "audience": "https://platform-dev-url",
                "success_redirect_url": "https://platform-default-url",
                "headless_callback_url": "https://dev.neu.ro/oauth/show-code",
                "callback_urls": [
                    "http://127.0.0.1:54540",
                    "http://127.0.0.1:54541",
                    "http://127.0.0.1:54542",
                ],
            }


class TestJobs:
    @pytest.mark.asyncio
    async def test_create_job_with_ssh_and_http(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["container"]["ssh"] = {"port": 7867}
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]
            expected_url = "ssh://nobody@ssh-auth.platform.neuromation.io:22"
            assert result["ssh_server"] == expected_url

        retrieved_job = await jobs_client.get_job_by_id(job_id=job_id)
        assert not retrieved_job["container"]["http"]["requires_auth"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_ssh_only(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["container"]["ssh"] = {"port": 7867}
        job_submit["container"].pop("http", None)
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]
            expected_url = "ssh://nobody@ssh-auth.platform.neuromation.io:22"
            assert result["ssh_server"] == expected_url

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_incorrect_request(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        json_job_submit = {"wrong_key": "wrong_value"}
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=json_job_submit
        ) as response:
            assert response.status == HTTPBadRequest.status_code
            data = await response.json()
            assert """'container': DataError(is required)""" in data["error"]

    @pytest.mark.asyncio
    async def test_broken_docker_image(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        payload = {
            "container": {
                "image": "some_broken_image",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": f"storage://{regular_user.name}",
                        "dst_path": "/var/storage",
                        "read_only": False,
                    },
                    {
                        "src_storage_uri": f"storage://{regular_user.name}/result",
                        "dst_path": "/var/storage/result",
                        "read_only": True,
                    },
                ],
            }
        }

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            data = await response.json()
            job_id = data["id"]
        await jobs_client.long_polling_by_job_id(job_id=job_id, status="failed")

    @pytest.mark.asyncio
    async def test_forbidden_storage_uri(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": f"storage://",
                        "dst_path": "/var/storage",
                        "read_only": False,
                    }
                ],
            }
        }

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_forbidden_image(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        payload = {
            "container": {
                "image": f"registry.dev.neuromation.io/anotheruser/image:tag",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            }
        }

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_allowed_image(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        payload = {
            "container": {
                "image": f"registry.dev.neuromation.io/{regular_user.name}/image:tag",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            }
        }

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_unauthorized_no_token(
        self, api: ApiConfig, client: aiohttp.ClientSession, job_submit: Dict[str, Any]
    ) -> None:
        url = api.jobs_base_url
        async with client.post(url, json=job_submit) as response:
            assert response.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_create_job_unauthorized_invalid_token(
        self, api: ApiConfig, client: aiohttp.ClientSession, job_submit: Dict[str, Any]
    ) -> None:
        url = api.jobs_base_url
        headers = {"Authorization": "Bearer INVALID"}
        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_create_job_invalid_job_name(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["is_preemptible"] = True
        job_submit["name"] = "Invalid_job_name!"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPBadRequest.status_code
            payload = await response.json()
            e = (
                "{'name': DataError({0: DataError(value should be None), "
                "1: DataError(does not match pattern ^[a-z](?:-?[a-z0-9])*$)})}"
            )
            assert payload == {"error": e}

    @pytest.mark.asyncio
    async def test_create_job_missing_cluster_name(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user_with_missing_cluster_name: _User,
    ) -> None:
        job_name = f"test-job-name-{random_str()}"
        url = api.jobs_base_url
        job_submit["is_preemptible"] = True
        job_submit["name"] = job_name
        user = regular_user_with_missing_cluster_name
        async with client.post(url, headers=user.headers, json=job_submit) as response:
            assert response.status == HTTPInternalServerError.status_code
            payload = await response.json()
            e = (
                f"Unexpected exception: Cluster '{user.cluster_name}' not found. "
                "Path with query: /api/v1/jobs."
            )
            assert payload == {"error": e}

    @pytest.mark.asyncio
    async def test_create_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        job_name = f"test-job-name-{random_str()}"
        url = api.jobs_base_url
        job_submit["is_preemptible"] = True
        job_submit["name"] = job_name
        job_submit["container"]["http"]["requires_auth"] = True
        job_submit["schedule_timeout"] = 90
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] in ["pending"]
            assert payload["name"] == job_name
            assert payload["container"]["entrypoint"] == "/bin/echo"
            assert payload["container"]["command"] == "1 2 3"
            assert payload["http_url"] == f"http://{job_id}.jobs.neu.ro"
            assert (
                payload["http_url_named"]
                == f"http://{job_name}--{regular_user.name}.jobs.neu.ro"
            )
            expected_internal_hostname = f"{job_id}.platformapi-tests"
            assert payload["internal_hostname"] == expected_internal_hostname
            assert payload["is_preemptible"]
            assert payload["description"] == "test job submitted by neuro job submit"
            assert payload["schedule_timeout"] == 90

        retrieved_job = await jobs_client.get_job_by_id(job_id=job_id)
        assert retrieved_job["internal_hostname"] == expected_internal_hostname
        assert retrieved_job["name"] == job_name
        assert retrieved_job["container"]["http"]["requires_auth"]
        assert retrieved_job["schedule_timeout"] == 90

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_without_name_http_url_named_not_sent(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["http_url"] == f"http://{job_id}.jobs.neu.ro"
            assert "http_url_named" not in payload

        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_multiple_jobs_with_same_name_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user: _User,
        jobs_client: JobsClient,
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers
        job_name = "test-job-name"
        job_submit["name"] = job_name
        job_submit["container"]["command"] = "sleep 100500"

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code
            payload = await response.json()
            job_id = payload["id"]

        await jobs_client.long_polling_by_job_id(job_id, status="running")

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPBadRequest.status_code
            payload = await response.json()
            assert payload == {
                "error": (
                    f"Failed to create job: job with name '{job_name}' "
                    f"and owner '{regular_user.name}' already exists: '{job_id}'"
                )
            }

        # cleanup
        await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_create_job_gpu_quota_allows(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
    ) -> None:
        quota = Quota(total_gpu_run_time_minutes=100)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        job_request["container"]["resources"]["gpu"] = 1
        async with client.post(url, headers=user.headers, json=job_request) as response:
            assert response.status == HTTPAccepted.status_code

    @pytest.mark.asyncio
    async def test_create_job_non_gpu_quota_allows(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
    ) -> None:
        quota = Quota(total_non_gpu_run_time_minutes=100)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            assert response.status == HTTPAccepted.status_code

    @pytest.mark.asyncio
    async def test_create_job_gpu_quota_exceeded(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: JobsClient,
        regular_user_factory: Callable[..., Any],
    ) -> None:
        quota = Quota(total_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        job_request["container"]["resources"]["gpu"] = 1
        async with client.post(url, headers=user.headers, json=job_request) as response:
            assert response.status == HTTPBadRequest.status_code
            data = await response.json()
            assert data == {"error": f"GPU quota exceeded for user '{user.name}'"}

    @pytest.mark.asyncio
    async def test_create_job_non_gpu_quota_exceeded(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: JobsClient,
        regular_user_factory: Callable[..., Any],
    ) -> None:
        quota = Quota(total_non_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            assert response.status == HTTPBadRequest.status_code
            data = await response.json()
            assert data == {"error": f"non-GPU quota exceeded for user '{user.name}'"}

    @pytest.mark.asyncio
    async def test_create_multiple_jobs_with_same_name_after_first_finished(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user: _User,
        jobs_client: JobsClient,
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers
        job_submit["name"] = "test-job-name"
        job_submit["container"]["command"] = "sleep 100500"

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code
            payload = await response.json()
            job_id = payload["id"]

        await jobs_client.long_polling_by_job_id(job_id, status="running")
        await jobs_client.delete_job(job_id)
        await jobs_client.long_polling_by_job_id(job_id, status="succeeded")

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code

    @pytest.mark.asyncio
    async def test_get_all_jobs_clear(self, jobs_client: JobsClient) -> None:
        jobs = await jobs_client.get_all_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_wrong_status(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
        job_request_factory: Callable[[], Dict[str, Any]],
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url

        filters = {"status": "abrakadabra"}
        async with client.get(url, headers=headers, params=filters) as response:
            assert response.status == HTTPBadRequest.status_code

        filters2 = [("status", "running"), ("status", "abrakadabra")]
        async with client.get(url, headers=headers, params=filters2) as response:
            assert response.status == HTTPBadRequest.status_code

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_status_only_single_status_pending(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
        job_request_factory: Callable[[], Dict[str, Any]],
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers
        job_request = job_request_factory()
        job_request["container"]["resources"]["memory_mb"] = 100_500
        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code
            result = await resp.json()
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id, status="pending")

        filters = {"status": "pending"}
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        filters = {"status": "running"}
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == set()

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_status_only(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
        job_request_factory: Callable[[], Dict[str, Any]],
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers
        job_request = job_request_factory()
        job_request["container"]["command"] = "sleep 20m"
        job_ids_list = []
        for _ in range(5):
            async with client.post(url, headers=headers, json=job_request) as resp:
                assert resp.status == HTTPAccepted.status_code
                result = await resp.json()
                job_ids_list.append(result["id"])

        job_ids_killed = set(job_ids_list[:2])
        job_ids_alive = set(job_ids_list[2:])
        job_ids_all = set(job_ids_list)

        for job_id in job_ids_all:
            await jobs_client.long_polling_by_job_id(job_id, status="running")

        for job_id in job_ids_killed:
            await jobs_client.delete_job(job_id=job_id)
            await jobs_client.long_polling_by_job_id(job_id, status="succeeded")

        # two statuses, actually filter out values
        filters = [("status", "pending"), ("status", "running")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_alive

        # no filter
        jobs = await jobs_client.get_all_jobs()
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_all

        # all statuses, same as no filter1
        filters = [
            ("status", "pending"),
            ("status", "running"),
            ("status", "failed"),
            ("status", "succeeded"),
        ]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_all

        # single status, actually filter out values
        filters2 = {"status": "succeeded"}
        jobs = await jobs_client.get_all_jobs(filters2)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_killed

        # cleanup
        for job_id in job_ids_alive:
            await jobs_client.delete_job(job_id=job_id)

    @pytest.fixture
    async def run_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client_factory: Callable[[_User], JobsClient],
    ) -> AsyncIterator[Callable[[_User, Dict[str, Any], bool], Awaitable[str]]]:
        cleanup_pairs = []

        async def _impl(
            user: _User, job_request: Dict[str, Any], do_kill: bool = False
        ) -> str:
            url = api.jobs_base_url
            headers = user.headers
            jobs_client = jobs_client_factory(user)
            async with client.post(url, headers=headers, json=job_request) as resp:
                assert resp.status == HTTPAccepted.status_code, str(job_request)
                data = await resp.json()
                job_id = data["id"]
                await jobs_client.long_polling_by_job_id(job_id, "running")
                if do_kill:
                    await jobs_client.delete_job(job_id)
                    await jobs_client.long_polling_by_job_id(job_id, "succeeded")
                else:
                    cleanup_pairs.append((jobs_client, job_id))
            return job_id

        yield _impl

        for jobs_client, job_id in cleanup_pairs:
            await jobs_client.delete_job(job_id=job_id, assert_success=False)

    @pytest.fixture
    async def share_job(
        self, auth_client: _AuthClient
    ) -> AsyncIterator[Callable[[_User, _User, Any], Awaitable[None]]]:
        async def _impl(owner: _User, follower: _User, job_id: str) -> None:
            permission = Permission(uri=f"job://{owner.name}/{job_id}", action="read")
            await auth_client.grant_user_permissions(
                follower.name, [permission], token=owner.token
            )

        yield _impl

    @pytest.fixture
    def create_job_request_with_name(
        self, job_request_factory: Callable[[], Dict[str, Any]]
    ) -> Iterator[Callable[[str], Dict[str, Any]]]:
        def _impl(job_name: str) -> Dict[str, Any]:
            job_request = job_request_factory()
            job_request["container"]["command"] = "sleep 30m"
            job_request["name"] = job_name
            return job_request

        yield _impl

    @pytest.fixture
    def create_job_request_no_name(
        self, job_request_factory: Callable[[], Dict[str, Any]]
    ) -> Iterator[Callable[[], Dict[str, Any]]]:
        def _impl() -> Dict[str, Any]:
            job_request = job_request_factory()
            job_request["container"]["command"] = "sleep 30m"
            return job_request

        yield _impl

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[None]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_req_no_name = create_job_request_no_name()
        job_req_with_name = create_job_request_with_name(job_name)
        usr = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr)

        job_usr_with_name_killed = await run_job(usr, job_req_with_name, do_kill=True)
        job_usr_no_name_killed = await run_job(usr, job_req_no_name, do_kill=True)
        job_usr_with_name = await run_job(usr, job_req_with_name, do_kill=False)
        job_usr_no_name = await run_job(usr, job_req_no_name, do_kill=False)

        # filter: job name
        filters = [("name", job_name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr_with_name_killed, job_usr_with_name}

        # filter: multiple statuses
        filters = [("status", "running"), ("status", "succeeded")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr_with_name_killed,
            job_usr_no_name_killed,
            job_usr_with_name,
            job_usr_no_name,
        }

        # filter: name + status
        filters = [("name", job_name), ("status", "succeeded")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr_with_name_killed}

        # filter: name + multiple statuses
        filters = [("name", job_name), ("status", "running"), ("status", "succeeded")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr_with_name_killed, job_usr_with_name}

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_self_owner_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[None]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_req_no_name = create_job_request_no_name()
        job_req_with_name = create_job_request_with_name(job_name)
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()

        jobs_client_usr1 = jobs_client_factory(usr1)

        job_usr1_with_name_killed = await run_job(usr1, job_req_with_name, do_kill=True)
        job_usr1_no_name_killed = await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr1_with_name = await run_job(usr1, job_req_with_name, do_kill=False)
        job_usr1_no_name = await run_job(usr1, job_req_no_name, do_kill=False)

        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: self owner
        filters = [("owner", usr1.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr1_with_name_killed,
            job_usr1_no_name_killed,
            job_usr1_with_name,
            job_usr1_no_name,
        }

        # filter: self owner + job name
        filters = [("name", job_name), ("owner", usr1.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1_with_name_killed, job_usr1_with_name}

        # filter: self owner + status
        filters = [("owner", usr1.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1_with_name, job_usr1_no_name}

        # filter: self owner + name + status
        filters = [("owner", usr1.name), ("name", job_name), ("status", "succeeded")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1_with_name_killed}

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_another_owner_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[None]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_req_no_name = create_job_request_no_name()
        job_req_with_name = create_job_request_with_name(job_name)
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr1)

        await run_job(usr1, job_req_with_name, do_kill=True)  # job_usr1_with_name_kiled
        await run_job(usr1, job_req_no_name, do_kill=True)  # job_usr1_no_name_killed
        await run_job(usr1, job_req_with_name, do_kill=False)  # job_usr1_with_name
        await run_job(usr1, job_req_no_name, do_kill=False)  # job_usr1_no_name

        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: another owner
        filters = [("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr2_with_name_killed,
            job_usr2_no_name_killed,
            job_usr2_with_name,
            job_usr2_no_name,
        }

        # filter: another owner + job name
        filters = [("name", job_name), ("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr2_with_name_killed, job_usr2_with_name}

        # filter: another owner + status
        filters = [("owner", usr2.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr2_with_name, job_usr2_no_name}

        # filter: another owner + name + status
        filters = [("owner", usr2.name), ("name", job_name), ("status", "succeeded")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr2_with_name_killed}

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_multiple_owners_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[None]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_req_no_name = create_job_request_no_name()
        job_req_with_name = create_job_request_with_name(job_name)
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr1)

        job_usr1_with_name_killed = await run_job(usr1, job_req_with_name, do_kill=True)
        job_usr1_no_name_killed = await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr1_with_name = await run_job(usr1, job_req_with_name, do_kill=False)
        job_usr1_no_name = await run_job(usr1, job_req_no_name, do_kill=False)

        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: multiple owners
        filters = [("owner", usr1.name), ("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr1_no_name,
            job_usr1_no_name_killed,
            job_usr1_with_name,
            job_usr1_with_name_killed,
            job_usr2_with_name_killed,
            job_usr2_no_name_killed,
            job_usr2_with_name,
            job_usr2_no_name,
        }

        # filter: multiple owners + job name
        filters = [("owner", usr1.name), ("owner", usr2.name), ("name", job_name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr2_with_name,
            job_usr2_with_name_killed,
            job_usr1_with_name,
            job_usr1_with_name_killed,
        }

        # filter: multiple owners + status
        filters = [("owner", usr1.name), ("owner", usr2.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr2_with_name,
            job_usr2_no_name,
            job_usr1_with_name,
            job_usr1_no_name,
        }

        # filter: multiple owners + name + status
        filters = [
            ("owner", usr1.name),
            ("owner", usr2.name),
            ("name", job_name),
            ("status", "succeeded"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1_with_name_killed, job_usr2_with_name_killed}

        # filter: multiple owners + name + multiple statuses
        filters = [
            ("owner", usr1.name),
            ("owner", usr2.name),
            ("name", job_name),
            ("status", "running"),
            ("status", "succeeded"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {
            job_usr1_with_name,
            job_usr1_with_name_killed,
            job_usr2_with_name,
            job_usr2_with_name_killed,
        }

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_owner_and_status_invalid_name(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers

        # filter by name only
        filters = {"name": "InValid_Name.txt"}
        async with client.get(url, headers=headers, params=filters) as resp:
            assert resp.status == HTTPBadRequest.status_code

        # filter by name and status
        filters2 = [("status", "running"), ("name", "InValid_Name.txt")]
        async with client.get(url, headers=headers, params=filters2) as resp:
            assert resp.status == HTTPBadRequest.status_code

    @pytest.mark.asyncio
    async def test_get_all_jobs_shared(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        regular_user_factory: Callable[[], Any],
        auth_client: _AuthClient,
    ) -> None:
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(
            url, headers=owner.headers, json=job_request
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            job_id = result["id"]

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
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        regular_user_factory: Callable[[], Any],
        auth_client: _AuthClient,
    ) -> None:
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(
            url, headers=owner.headers, json=job_request
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            job_id = result["id"]

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
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user: _User,
    ) -> None:
        jobs_ids = []
        n_jobs = 2
        for _ in range(n_jobs):
            url = api.jobs_base_url
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as response:
                assert response.status == HTTPAccepted.status_code
                result = await response.json()
                assert result["status"] in ["pending"]
                job_id = result["id"]
                await jobs_client.long_polling_by_job_id(
                    job_id=job_id, status="succeeded"
                )
                jobs_ids.append(job_id)

        jobs = await jobs_client.get_all_jobs()
        assert set(jobs_ids) <= {x["id"] for x in jobs}
        # clean:test_job_log
        for job in jobs:
            await jobs_client.delete_job(job_id=job["id"])

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "filters",
        [
            multidict.MultiDict([("name", f"test-job-{random_str()}")]),
            multidict.MultiDict(
                [
                    ("name", f"test-job-{random_str()}"),
                    ("status", "running"),
                    ("status", "pending"),
                    ("status", "failed"),
                    ("status", "succeeded"),
                ]
            ),
        ],
    )
    async def test_get_jobs_by_name_preserves_chronological_order_without_statuses(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user: _User,
        filters: Dict[str, Any],
    ) -> None:
        # unique job name generated per test-run is stored in "filters"
        job_submit["name"] = filters.get("name")
        job_submit["container"]["command"] = "sleep 30m"

        jobs_ids = []
        n_jobs = 5
        for i in range(n_jobs):
            async with client.post(
                api.jobs_base_url, headers=regular_user.headers, json=job_submit
            ) as response:
                assert response.status == HTTPAccepted.status_code, f"{i}-th job"
                result = await response.json()
                assert result["status"] == "pending"
                job_id = result["id"]
                jobs_ids.append(job_id)
                await jobs_client.long_polling_by_job_id(job_id, status="running")
                # let only the last job be running
                if i < n_jobs - 1:
                    await jobs_client.delete_job(job_id)
                    await jobs_client.long_polling_by_job_id(job_id, status="succeeded")

        jobs_ls = await jobs_client.get_all_jobs(params=filters)
        jobs_ls = [job["id"] for job in jobs_ls]
        assert set(jobs_ids) == set(jobs_ls), "content differs"
        assert jobs_ids == jobs_ls, "order differs"

        # cleanup all:
        for job_id in jobs_ids:
            await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_get_job_by_hostname_self_owner(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_name2 = "test-job-name2"
        usr = await regular_user_factory()
        jobs_client = jobs_client_factory(usr)

        job_id = await run_job(usr, create_job_request_with_name(job_name))
        await run_job(usr, create_job_request_with_name(job_name2))

        hostname = f"{job_name}--{usr.name}.jobs.neu.ro"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        hostname = f"{job_id}.jobs.neu.ro"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        # other base domain name
        hostname = f"{job_name}--{usr.name}.example.org"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        hostname = f"{job_id}.example.org"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        # non-existing names
        hostname = f"nonexisting--{usr.name}.jobs.neu.ro"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        assert not jobs

        hostname = f"{job_name}--nonexisting.jobs.neu.ro"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        assert not jobs

        hostname = "nonexisting.jobs.neu.ro"
        jobs = await jobs_client.get_all_jobs({"hostname": hostname})
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_job_by_hostname_another_owner(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[None]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_name2 = "test-job-name2"
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr1)

        job_id = await run_job(usr2, create_job_request_with_name(job_name))
        await run_job(usr2, create_job_request_with_name(job_name2))

        # usr2 shares a job with usr1
        await share_job(usr2, usr1, job_id)

        # shared job of another owner
        hostname = f"{job_name}--{usr2.name}.jobs.neu.ro"
        jobs = await jobs_client_usr1.get_all_jobs({"hostname": hostname})
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_id}

        # unshared job of another owner
        hostname = f"{job_name2}--{usr2.name}.jobs.neu.ro"
        jobs = await jobs_client_usr1.get_all_jobs({"hostname": hostname})
        assert not jobs

        # non-existing job of another owner
        hostname = f"nonexisting--{usr2.name}.jobs.neu.ro"
        jobs = await jobs_client_usr1.get_all_jobs({"hostname": hostname})
        assert not jobs

    @pytest.mark.asyncio
    async def test_get_job_by_hostname_and_status(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_name2 = "test-job-name2"
        usr = await regular_user_factory()
        jobs_client = jobs_client_factory(usr)

        job_id = await run_job(usr, create_job_request_with_name(job_name))
        await run_job(usr, create_job_request_with_name(job_name2))

        for hostname in (
            f"{job_name}--{usr.name}.jobs.neu.ro",
            f"{job_id}.jobs.neu.ro",
        ):
            filters = [("hostname", hostname), ("status", "running")]
            jobs = await jobs_client.get_all_jobs(filters)
            job_ids = {job["id"] for job in jobs}
            assert job_ids == {job_id}

            filters = [("hostname", hostname), ("status", "succeeded")]
            jobs = await jobs_client.get_all_jobs(filters)
            job_ids = {job["id"] for job in jobs}
            assert job_ids == set()

            filters = [
                ("hostname", hostname),
                ("status", "running"),
                ("status", "succeeded"),
            ]
            jobs = await jobs_client.get_all_jobs(filters)
            job_ids = {job["id"] for job in jobs}
            assert job_ids == {job_id}

    @pytest.mark.asyncio
    async def test_get_job_by_hostname_invalid_request(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        run_job: Callable[..., Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        url = api.jobs_base_url
        job_name = "test-job-name"
        usr = await regular_user_factory()

        await run_job(usr, create_job_request_with_name(job_name))

        hostname = f"{job_name}--{usr.name}.jobs.neu.ro"
        for params in (
            {"hostname": hostname, "name": job_name},
            {"hostname": hostname, "owner": usr.name},
        ):
            async with client.get(url, headers=usr.headers, params=params) as response:
                response_text = await response.text()
                assert response.status == HTTPBadRequest.status_code, response_text
                result = await response.json()
                assert result["error"] == "Invalid request"

        for params in (
            {"hostname": f"test_job--{usr.name}.jobs.neu.ro"},
            {"hostname": f"{job_name}--test_user.jobs.neu.ro"},
        ):
            async with client.get(url, headers=usr.headers, params=params) as response:
                response_text = await response.text()
                assert response.status == HTTPBadRequest.status_code, response_text

    @pytest.mark.asyncio
    async def test_delete_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]
            await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

        jobs = await jobs_client.get_all_jobs()
        assert len(jobs) == 1
        assert jobs[0]["status"] == "succeeded"
        assert jobs[0]["id"] == job_id

    @pytest.mark.asyncio
    async def test_delete_already_deleted(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["container"]["command"] = "sleep 1000000000"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]
            await jobs_client.long_polling_by_job_id(job_id=job_id, status="running")
        await jobs_client.delete_job(job_id=job_id)
        # delete again (same result expected)
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_delete_not_exist(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        job_id = "kdfghlksjd-jhsdbljh-3456789!@"
        url = api.jobs_base_url + f"/{job_id}"
        async with client.delete(url, headers=regular_user.headers) as response:
            assert response.status == HTTPBadRequest.status_code
            result = await response.json()
            assert result["error"] == f"no such job {job_id}"

    @pytest.mark.asyncio
    async def test_job_log(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        command = 'bash -c "for i in {1..5}; do echo $i; sleep 1; done"'
        payload = {
            "container": {
                "image": "ubuntu",
                "command": command,
                "resources": {"cpu": 0.1, "memory_mb": 16},
            }
        }
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        job_log_url = api.jobs_base_url + f"/{job_id}/log"
        async with client.get(job_log_url, headers=regular_user.headers) as response:
            assert response.content_type == "text/plain"
            assert response.charset == "utf-8"
            assert response.headers["Transfer-Encoding"] == "chunked"
            assert "Content-Encoding" not in response.headers
            actual_payload = await response.read()
            expected_payload = "\n".join(str(i) for i in range(1, 6)) + "\n"
            assert actual_payload == expected_payload.encode()

    @pytest.mark.asyncio
    async def test_create_validation_failure(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        request_payload: Dict[str, Any] = {}
        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            assert response.status == HTTPBadRequest.status_code
            response_payload = await response.json()
            assert response_payload == {"error": mock.ANY}
            assert "is required" in response_payload["error"]

    @pytest.mark.asyncio
    async def test_create_with_custom_volumes(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
    ) -> None:
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
            },
            "is_preemptible": True,
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
                "cluster_name": "default",
                "internal_hostname": f"{job_id}.platformapi-tests",
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
                "ssh_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
                "ssh_auth_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
                "is_preemptible": True,
            }

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="succeeded"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "cluster_name": "default",
            "internal_hostname": f"{job_id}.platformapi-tests",
            "status": "succeeded",
            "history": {
                "status": "succeeded",
                "reason": None,
                "description": None,
                "exit_code": 0,
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
            "ssh_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
            "ssh_auth_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
            "is_preemptible": True,
        }

    @pytest.mark.asyncio
    async def test_job_failed(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
    ) -> None:
        command = 'bash -c "echo Failed!; false"'
        payload = {
            "container": {
                "image": "ubuntu",
                "command": command,
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
            }
        }
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] == "pending"
            job_id = result["id"]

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="failed"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "cluster_name": "default",
            "status": "failed",
            "internal_hostname": f"{job_id}.platformapi-tests",
            "history": {
                "status": "failed",
                "reason": "Error",
                "description": "Failed!\n",
                "created_at": mock.ANY,
                "started_at": mock.ANY,
                "finished_at": mock.ANY,
                "exit_code": 1,
            },
            "container": {
                "command": 'bash -c "echo Failed!; false"',
                "image": "ubuntu",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "env": {},
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
            "ssh_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
            "ssh_auth_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
            "is_preemptible": False,
        }

    @pytest.mark.asyncio
    async def test_job_create_unknown_gpu_model(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        kube_node_gpu: str,
    ) -> None:
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
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        kube_node_gpu: str,
        kube_client: MyKubeClient,
    ) -> None:
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
                "cluster_name": "default",
                "internal_hostname": f"{job_id}.platformapi-tests",
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
                "ssh_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
                "ssh_auth_server": "ssh://nobody@ssh-auth.platform.neuromation.io:22",
                "is_preemptible": False,
            }

        await kube_client.wait_pod_scheduled(job_id, kube_node_gpu)

    @pytest.mark.asyncio
    async def test_job_top(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        jobs_client: JobsClient,
        infinite_job: str,
    ) -> None:
        await jobs_client.long_polling_by_job_id(job_id=infinite_job, status="running")
        job_top_url = api.jobs_base_url + f"/{infinite_job}/top"
        num_request = 2
        records = []
        async with client.ws_connect(job_top_url, headers=regular_user.headers) as ws:
            # TODO move this ws communication to JobClient
            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
                else:
                    records.append(json.loads(msg.data))

                if len(records) == num_request:
                    # TODO (truskovskiyk 09/12/18) do not use protected prop
                    # https://github.com/aio-libs/aiohttp/issues/3443
                    proto = ws._writer.protocol
                    assert proto.transport is not None
                    proto.transport.close()
                    break

        assert records
        for message in records:
            assert message == {
                "cpu": mock.ANY,
                "memory": mock.ANY,
                "timestamp": mock.ANY,
            }

    @pytest.mark.asyncio
    async def test_job_top_silently_wait_when_job_pending(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:
        command = 'bash -c "for i in {1..10}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        job_top_url = api.jobs_base_url + f"/{job_id}/top"
        async with client.ws_connect(job_top_url, headers=regular_user.headers) as ws:
            while True:
                job = await jobs_client.get_job_by_id(job_id=job_id)
                assert job["status"] == "pending"

                # silently waiting for a job becomes running
                msg = await ws.receive()
                job = await jobs_client.get_job_by_id(job_id=job_id)
                assert job["status"] == "running"
                assert msg.type == aiohttp.WSMsgType.TEXT

                break

        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_job_top_close_when_job_succeeded(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        jobs_client: JobsClient,
        job_submit: Dict[str, Any],
    ) -> None:

        command = 'bash -c "for i in {1..2}; do echo $i; sleep 1; done"'
        job_submit["container"]["command"] = command
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")

        job_top_url = api.jobs_base_url + f"/{job_id}/top"
        async with client.ws_connect(job_top_url, headers=regular_user.headers) as ws:
            msg = await ws.receive()
            job = await jobs_client.get_job_by_id(job_id=job_id)

            assert msg.type == aiohttp.WSMsgType.CLOSE
            assert job["status"] == "succeeded"

        await jobs_client.delete_job(job_id=job_id)
