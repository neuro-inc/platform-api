import json
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
)
from unittest import mock

import aiohttp.web
import multidict
import pytest
from aiohttp.web import (
    HTTPAccepted,
    HTTPBadRequest,
    HTTPConflict,
    HTTPForbidden,
    HTTPNoContent,
    HTTPOk,
    HTTPUnauthorized,
)
from aiohttp.web_exceptions import HTTPCreated, HTTPNotFound
from neuro_auth_client import Cluster as AuthCluster, Permission, Quota
from yarl import URL

from platform_api.config import Config
from platform_api.orchestrator.jobs_service import NEURO_PASSED_CONFIG
from tests.conftest import random_str
from tests.integration.secrets import SecretsClient
from tests.integration.test_config_client import create_config_api

from .api import ApiConfig, AuthApiConfig, JobsClient
from .auth import AuthClient, _User
from .conftest import MyKubeClient
from .diskapi import DiskAPIClient


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


@pytest.fixture
def cluster_configs_payload() -> List[Dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
            "storage": {
                "nfs": {"server": "127.0.0.1", "export_path": "/nfs/export/path"},
                "url": "https://dev.neu.ro/api/v1/storage",
            },
            "registry": {
                "url": "https://registry-dev.neu.ro",
                "email": "registry@neuromation.io",
            },
            "orchestrator": {
                "kubernetes": {
                    "url": "http://127.0.0.1:8443",
                    "ca_data": "certificate",
                    "auth_type": "token",
                    "token": "auth_token",
                    "namespace": "default",
                    "jobs_ingress_class": "nginx",
                    "jobs_ingress_oauth_url": "https://neu.ro/oauth/authorize",
                    "node_label_gpu": "cloud.google.com/gke-accelerator",
                    "node_label_preemptible": "cloud.google.com/gke-preemptible",
                },
                "job_hostname_template": "{job_id}.jobs.neu.ro",
                "resource_pool_types": [
                    {"name": "node-pool1"},
                    {"name": "node-pool1", "gpu": 0},
                    {"name": "node-pool1", "gpu": 1, "gpu_model": "nvidia-tesla-v100"},
                ],
                "is_http_ingress_secure": True,
            },
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
            "secrets": {"url": "https://dev.neu.ro/api/v1/secrets"},
            "metrics": {"url": "https://metrics.dev.neu.ro"},
        }
    ]


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api: ApiConfig, client: aiohttp.ClientSession) -> None:
        async with client.get(api.ping_url) as response:
            assert response.status == HTTPOk.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_ping_unknown_origin(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            api.ping_url, headers={"Origin": "http://unknown"}
        ) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert "Access-Control-Allow-Origin" not in response.headers

    @pytest.mark.asyncio
    async def test_ping_allowed_origin(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            api.ping_url, headers={"Origin": "https://neu.ro"}
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Expose-Headers"]

    @pytest.mark.asyncio
    async def test_ping_options_no_headers(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(api.ping_url) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin header is not specified in the request"
            )

    @pytest.mark.asyncio
    async def test_ping_options_unknown_origin(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            api.ping_url,
            headers={
                "Origin": "http://unknown",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin 'http://unknown' is not allowed"
            )

    @pytest.mark.asyncio
    async def test_ping_options(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            api.ping_url,
            headers={
                "Origin": "https://neu.ro",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Allow-Methods"] == "GET"

    @pytest.mark.asyncio
    async def test_config_unauthorized(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        url = api.config_url
        async with client.get(url) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {}

    @pytest.mark.asyncio
    async def test_clusters_sync(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        cluster_configs_payload: List[Dict[str, Any]],
        cluster_user: _User,
    ) -> None:
        # pass config with 1 cluster
        # record count doesnt't change, because there's a default cluster
        # which gets deleted
        async with create_config_api(cluster_configs_payload):
            url = api.clusters_sync_url
            async with client.post(url, headers=cluster_user.headers) as resp:
                assert resp.status == HTTPOk.status_code, await resp.text()
                result = await resp.json()
                assert result == {"old_record_count": 2, "new_record_count": 1}

        # pass empty cluster config - all clusters should be deleted
        async with create_config_api([]):
            url = api.clusters_sync_url
            async with client.post(url, headers=cluster_user.headers) as resp:
                assert resp.status == HTTPOk.status_code, await resp.text()
                result = await resp.json()
                assert result == {"old_record_count": 1, "new_record_count": 0}

    @pytest.mark.asyncio
    async def test_config(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
    ) -> None:
        url = api.config_url
        regular_user = await regular_user_factory(
            auth_clusters=[
                AuthCluster(name="test-cluster"),
                AuthCluster(name="testcluster2"),
            ]
        )
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            expected_cluster_payload = {
                "name": "test-cluster",
                "registry_url": "https://registry.dev.neuromation.io",
                "storage_url": "https://neu.ro/api/v1/storage",
                "users_url": "https://neu.ro/api/v1/users",
                "monitoring_url": "https://neu.ro/api/v1/monitoring",
                "secrets_url": "https://neu.ro/api/v1/secrets",
                "metrics_url": "https://neu.ro/api/v1/metrics",
                "resource_presets": [
                    {
                        "name": "gpu-small",
                        "cpu": 7,
                        "memory_mb": 30720,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "gpu-large",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "gpu-large-p",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "is_preemptible": True,
                        "is_preemptible_node_required": True,
                    },
                    {
                        "name": "cpu-micro",
                        "cpu": 0.1,
                        "memory_mb": 100,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "cpu-small",
                        "cpu": 2,
                        "memory_mb": 2048,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "cpu-large",
                        "cpu": 3,
                        "memory_mb": 14336,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "tpu",
                        "cpu": 3,
                        "memory_mb": 14336,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                        "tpu": {"type": "v2-8", "software_version": "1.14"},
                    },
                ],
            }
            expected_payload: Dict[str, Any] = {
                "admin_url": "http://localhost:8080/apis/admin/v1",
                "clusters": [
                    expected_cluster_payload,
                    {**expected_cluster_payload, **{"name": "testcluster2"}},
                ],
                **expected_cluster_payload,
            }
            assert result == expected_payload

    @pytest.mark.asyncio
    async def test_config_with_oauth(
        self,
        api_with_oauth: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
    ) -> None:
        url = api_with_oauth.config_url
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            expected_cluster_payload = {
                "name": "test-cluster",
                "registry_url": "https://registry.dev.neuromation.io",
                "storage_url": "https://neu.ro/api/v1/storage",
                "users_url": "https://neu.ro/api/v1/users",
                "monitoring_url": "https://neu.ro/api/v1/monitoring",
                "secrets_url": "https://neu.ro/api/v1/secrets",
                "metrics_url": "https://neu.ro/api/v1/metrics",
                "resource_presets": [
                    {
                        "name": "gpu-small",
                        "cpu": 7,
                        "memory_mb": 30720,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "gpu-large",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "gpu-large-p",
                        "cpu": 7,
                        "memory_mb": 61440,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "is_preemptible": True,
                        "is_preemptible_node_required": True,
                    },
                    {
                        "name": "cpu-micro",
                        "cpu": 0.1,
                        "memory_mb": 100,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "cpu-small",
                        "cpu": 2,
                        "memory_mb": 2048,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "cpu-large",
                        "cpu": 3,
                        "memory_mb": 14336,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                    },
                    {
                        "name": "tpu",
                        "cpu": 3,
                        "memory_mb": 14336,
                        "is_preemptible": False,
                        "is_preemptible_node_required": False,
                        "tpu": {"type": "v2-8", "software_version": "1.14"},
                    },
                ],
            }
            expected_payload: Dict[str, Any] = {
                "auth_url": "https://platform-auth0-url/authorize",
                "token_url": "https://platform-auth0-url/oauth/token",
                "logout_url": "https://platform-auth0-url/v2/logout",
                "client_id": "client_id",
                "audience": "https://platform-dev-url",
                "success_redirect_url": "https://platform-default-url",
                "headless_callback_url": "https://dev.neu.ro/oauth/show-code",
                "callback_urls": [
                    "http://127.0.0.1:54540",
                    "http://127.0.0.1:54541",
                    "http://127.0.0.1:54542",
                ],
                "admin_url": "http://localhost:8080/apis/admin/v1",
                "clusters": [expected_cluster_payload],
                **expected_cluster_payload,
            }
            assert result == expected_payload


class TestJobs:
    @pytest.mark.asyncio
    async def test_create_job_with_http(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["restart_policy"] = "on-failure"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        retrieved_job = await jobs_client.get_job_by_id(job_id=job_id)
        assert not retrieved_job["container"]["http"]["requires_auth"]

        job_response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="succeeded"
        )
        await jobs_client.delete_job(job_id=job_id)
        assert job_response_payload["restart_policy"] == "on-failure"

    @pytest.mark.asyncio
    async def test_create_job_without_http(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["container"].pop("http", None)
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_pass_config(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["pass_config"] = True
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            assert result["pass_config"]
            assert NEURO_PASSED_CONFIG in result["container"]["env"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_wait_for_jobs_quota(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["wait_for_jobs_quota"] = True
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_privileged_flag(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["privileged"] = True
        # Only privileged container can do this:
        job_submit["container"]["command"] = "/bin/bash -c 'mount -t tmpfs none /mnt'"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")
        await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_tty(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        job_submit["container"]["tty"] = True
        job_submit["container"]["command"] = "test -t 0"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            assert result["container"]["tty"] is True
            job_id = result["id"]

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="succeeded"
        )
        await jobs_client.delete_job(job_id=job_id)

        assert response_payload["container"]["tty"] is True
        assert response_payload["history"]["exit_code"] == 0

    @pytest.fixture
    async def _run_job_with_secrets(
        self, api: ApiConfig, client: aiohttp.ClientSession, jobs_client: JobsClient
    ) -> Callable[..., Awaitable[None]]:
        async def _run(
            job_submit: Dict[str, Any],
            user: _User,
            *,
            secret_env: Optional[Dict[str, str]] = None,
            secret_volumes: Optional[Dict[str, str]] = None,
        ) -> None:
            job_id = ""
            try:
                url = api.jobs_base_url
                async with client.post(
                    url, headers=user.headers, json=job_submit
                ) as resp:
                    assert resp.status == HTTPAccepted.status_code, await resp.text()
                    result = await resp.json()
                    job_id = result["id"]
                    assert result["status"] in ["pending"]
                    if secret_env:
                        assert result["container"]["secret_env"] == secret_env
                    if secret_volumes:
                        assert result["container"]["secret_volumes"] == secret_volumes

                result = await jobs_client.long_polling_by_job_id(
                    job_id=job_id,
                    status="succeeded",
                    headers=user.headers,
                )
                if secret_env:
                    assert result["container"]["secret_env"] == secret_env
                if secret_volumes:
                    assert result["container"]["secret_volumes"] == secret_volumes
            finally:
                if job_id:
                    await jobs_client.delete_job(job_id, headers=user.headers)

        return _run

    @pytest.mark.asyncio
    async def test_create_job_with_secret_env_single_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        key, value = "key1", "value1"
        await regular_secrets_client.create_secret(key, value)

        user = regular_user
        secret_env = {
            "ENV_SECRET": f"secret://{user.cluster_name}/{user.name}/{key}",
        }
        job_submit["container"]["secret_env"] = secret_env

        cmd = f'bash -c \'echo "$ENV_SECRET" && [ "$ENV_SECRET" == "{value}" ]\''
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_env=secret_env)

    @pytest.mark.asyncio
    async def test_create_job_with_secret_volume_single_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        secret_name, secret_value = "key1", "value1"
        secret_path = "/etc/foo/file.txt"

        await regular_secrets_client.create_secret(secret_name, secret_value)

        user = regular_user
        secret_uri = f"secret://{user.cluster_name}/{user.name}/{secret_name}"
        secret_volumes = [
            {"src_secret_uri": secret_uri, "dst_path": secret_path},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        cmd = f'bash -c \'[ "$(cat {secret_path})" == "{secret_value}" ]\''
        job_submit["container"]["command"] = cmd

        job_id = ""
        try:
            url = api.jobs_base_url
            async with client.post(url, headers=user.headers, json=job_submit) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["secret_volumes"] == secret_volumes

            response_payload = await jobs_client.long_polling_by_job_id(
                job_id=job_id, status="succeeded"
            )
            assert response_payload["container"]["secret_volumes"] == secret_volumes

        finally:
            if job_id:
                await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_disk_volume_single_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_disk_api_client: DiskAPIClient,
    ) -> None:
        disk_path = "/mnt/disk"

        disk = await regular_disk_api_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": disk_path,
                "read_only": False,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        cmd = (
            f'bash -c \'echo "value" > {disk_path}/test.txt '
            f"&& cat {disk_path}/test.txt'"
        )
        job_submit["container"]["command"] = cmd

        job_id = ""
        try:
            url = api.jobs_base_url
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            response_payload = await jobs_client.long_polling_by_job_id(
                job_id=job_id, status="succeeded"
            )
            assert response_payload["container"]["disk_volumes"] == disk_volumes

        finally:
            if job_id:
                await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_one_disk_volume_multiple_mounts_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_disk_api_client: DiskAPIClient,
    ) -> None:
        disk_path1 = "/mnt/disk1"
        disk_path2 = "/mnt/disk2"

        disk = await regular_disk_api_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": disk_path1,
                "read_only": False,
            },
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": disk_path2,
                "read_only": False,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        cmd = (
            f'bash -c \'echo "value1" > {disk_path1}/test.txt '
            f"&& cat {disk_path2}/test.txt'"
        )
        job_submit["container"]["command"] = cmd

        job_id = ""
        try:
            url = api.jobs_base_url
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            response_payload = await jobs_client.long_polling_by_job_id(
                job_id=job_id, status="succeeded"
            )
            assert response_payload["container"]["disk_volumes"] == disk_volumes

        finally:
            if job_id:
                await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_create_job_with_multiple_disk_volumes_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_disk_api_client: DiskAPIClient,
    ) -> None:
        disk_path1 = "/mnt/disk1"
        disk_path2 = "/mnt/disk2"

        disk1 = await regular_disk_api_client.create_disk(storage=1024 * 1024)
        disk2 = await regular_disk_api_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk1.to_uri()),
                "dst_path": disk_path1,
                "read_only": False,
            },
            {
                "src_disk_uri": str(disk2.to_uri()),
                "dst_path": disk_path2,
                "read_only": False,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        cmd = (
            f'bash -c \'echo "value1" > {disk_path1}/test.txt '
            f'&& echo "value1" > {disk_path2}/test.txt '
            f"&& cat {disk_path1}/test.txt "
            f"&& cat {disk_path2}/test.txt'"
        )

        job_submit["container"]["command"] = cmd

        job_id = ""
        try:
            url = api.jobs_base_url
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            response_payload = await jobs_client.long_polling_by_job_id(
                job_id=job_id, status="succeeded"
            )
            assert response_payload["container"]["disk_volumes"] == disk_volumes

        finally:
            if job_id:
                await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_disk_volume_data_persisted_between_jobs(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_disk_api_client: DiskAPIClient,
    ) -> None:
        disk_path = "/mnt/disk1"
        file_name = "test.txt"
        value = "value"

        disk = await regular_disk_api_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": disk_path,
                "read_only": False,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        cmd1 = f"bash -c 'echo \"{value}\" > {disk_path}/{file_name}'"
        cmd2 = f'bash -c \'[ "$(cat {disk_path}/{file_name})" == "{value}" ]\''

        job1_id, job2_id = "", ""
        try:
            url = api.jobs_base_url
            job_submit["container"]["command"] = cmd1
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job1_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            await jobs_client.long_polling_by_job_id(job_id=job1_id, status="succeeded")

            job_submit["container"]["command"] = cmd2
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job2_id = result["id"]
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            await jobs_client.long_polling_by_job_id(job_id=job2_id, status="succeeded")

        finally:
            if job1_id:
                await jobs_client.delete_job(job1_id)
            if job2_id:
                await jobs_client.delete_job(job2_id)

    @pytest.mark.asyncio
    async def test_disk_volume_race_between_jobs_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_disk_api_client: DiskAPIClient,
    ) -> None:
        disk_path = "/mnt/disk1"
        file_name = "test.txt"
        jobs_in_race = 5
        value = "value"
        expected_string = value * jobs_in_race

        disk = await regular_disk_api_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": disk_path,
                "read_only": False,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        cmd1 = f"bash -c 'echo -n \"{value}\" >> {disk_path}/{file_name}'"
        cmd2 = (
            f'bash -c \'[ "$(cat {disk_path}/{file_name})" == "{expected_string}" ]\''
        )

        job_ids = []
        try:
            url = api.jobs_base_url
            job_submit["container"]["command"] = cmd1
            for _ in range(jobs_in_race):
                async with client.post(
                    url, headers=regular_user.headers, json=job_submit
                ) as resp:
                    assert resp.status == HTTPAccepted.status_code, await resp.text()
                    result = await resp.json()
                    job_ids.append(result["id"])
                    assert result["status"] in ["pending"]
                    assert result["container"]["disk_volumes"] == disk_volumes

            for job_id in job_ids:
                await jobs_client.long_polling_by_job_id(
                    job_id=job_id, status="succeeded"
                )

            job_submit["container"]["command"] = cmd2
            async with client.post(
                url, headers=regular_user.headers, json=job_submit
            ) as resp:
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                checker_job_id = result["id"]
                job_ids.append(checker_job_id)
                assert result["status"] in ["pending"]
                assert result["container"]["disk_volumes"] == disk_volumes

            await jobs_client.long_polling_by_job_id(
                job_id=checker_job_id, status="succeeded"
            )

        finally:
            for job_id in job_ids:
                await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_create_job_disk_volumes_unexisting_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        disk_uri = f"disk://{user.cluster_name}/{user.name}/disk-1"
        disk_volumes = [
            {"src_disk_uri": disk_uri, "dst_path": "/mnt/disk"},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = "Missing disks: 'disk-1'"
            assert err in msg["error"], msg["error"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("read_only", [True, False])
    async def test_create_job_with_other_user_disk_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        test_cluster_name: str,
        regular_user_factory: Callable[..., Awaitable[_User]],
        disk_client_factory: Callable[..., AsyncContextManager[DiskAPIClient]],
        read_only: bool,
    ) -> None:
        cluster = test_cluster_name
        usr_1 = await regular_user_factory(cluster_name=cluster)
        usr_2 = await regular_user_factory(cluster_name=cluster)

        async with disk_client_factory(usr_1) as disk_client:
            disk = await disk_client.create_disk(storage=1024 * 1024)

        disk_volumes = [
            {
                "src_disk_uri": str(disk.to_uri()),
                "dst_path": "/mnt/disk",
                "read_only": read_only,
            },
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        url = api.jobs_base_url
        async with client.post(url, headers=usr_2.headers, json=job_submit) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            result = await resp.json()
            perm = {
                "uri": str(disk.to_uri()),
                "action": "read" if read_only else "write",
            }
            assert perm in result["missing"]

    @pytest.mark.asyncio
    async def test_create_job_with_disk_volume_wrong_scheme_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        wrong_scheme = "wrong-scheme"
        disk_uri_good = f"disk://{user.cluster_name}/{user.name}/key_1"
        disk_uri_wrong = f"{wrong_scheme}://{user.cluster_name}/{user.name}/key_2"

        disk_volumes = [
            {"src_disk_uri": disk_uri_good, "dst_path": "/container/path_1"},
            {"src_disk_uri": disk_uri_wrong, "dst_path": "/container/path_2"},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid URI scheme: '{wrong_scheme}' != 'disk'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_disk_volume_wrong_cluster_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        wrong_cluster = "wrong-cluster-name"
        disk_uri_good = f"disk://{user.cluster_name}/{user.name}/key_1"
        disk_uri_wrong = f"disk://{wrong_cluster}/{user.name}/key_2"

        disk_volumes = [
            {"src_disk_uri": disk_uri_good, "dst_path": "/container/path_1"},
            {"src_disk_uri": disk_uri_wrong, "dst_path": "/container/path_2"},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes

        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid URI cluster: '{wrong_cluster}' != '{user.cluster_name}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_disk_volume_invalid_mount_with_dots_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        disk_uri = f"disk://{user.cluster_name}/{user.name}/key_1"
        invalid_path = "/container/path_1/../path_2"
        disk_volumes = [
            {"src_disk_uri": disk_uri, "dst_path": invalid_path},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid path: '{invalid_path}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_disk_volume_invalid_mount_relative_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        disk_uri = f"disk://{user.cluster_name}/{user.name}/disk-1"
        invalid_path = "container/path_1"
        disk_volumes = [
            {"src_disk_uri": disk_uri, "dst_path": invalid_path},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Mount path must be absolute: '{invalid_path}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_disk_volumes_same_mount_points_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        disk_uri_1 = f"disk://{user.cluster_name}/{user.name}/disk-1"
        disk_uri_2 = f"disk://{user.cluster_name}/{user.name}/disk-2"
        disk_volumes = [
            {"src_disk_uri": disk_uri_1, "dst_path": "/container/path"},
            {"src_disk_uri": disk_uri_2, "dst_path": "/container/path"},
        ]
        job_submit["container"]["disk_volumes"] = disk_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = "destination path '/container/path' was encountered multiple times"
            assert err in msg["error"], msg["error"]

    @pytest.mark.asyncio
    async def test_create_job_with_secret_volumes_different_dirs_same_filenames_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        user = regular_user
        key_1, key_2 = "key_1", "key_2"
        secret_value_1 = "value1"
        secret_value_2 = "value2"
        await regular_secrets_client.create_secret(key_1, secret_value_1)
        await regular_secrets_client.create_secret(key_2, secret_value_2)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"

        secret_path_a = "/etc/path_a/file.txt"
        secret_path_b = "/etc/path_b/file.txt"

        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": secret_path_a},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_b},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "$(cat {secret_path_a})" == "{secret_value_1}" ]',
                f'[ "$(cat {secret_path_b})" == "{secret_value_2}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_volumes=secret_volumes)

    @pytest.mark.asyncio
    async def test_create_job_with_secret_env_and_secret_volumes_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        """Mount multiple different secrets as both secret env and
        secret volumes in a single job.
        """
        user = regular_user

        key_1, key_2, key_3 = "key_1", "key_2", "key_3"
        key_a, key_b, key_c = "key_a", "key_b", "key_c"
        value_1, value_2, value_3 = "value_1", "value_2", "value_3"
        value_a, value_b, value_c = "value_a", "value_b", "value_c"
        await regular_secrets_client.create_secret(key_1, value_1)
        await regular_secrets_client.create_secret(key_2, value_2)
        await regular_secrets_client.create_secret(key_3, value_3)
        await regular_secrets_client.create_secret(key_a, value_a)
        await regular_secrets_client.create_secret(key_b, value_b)
        await regular_secrets_client.create_secret(key_c, value_c)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"
        secret_uri_3 = f"secret://{user.cluster_name}/{user.name}/{key_3}"
        secret_uri_a = f"secret://{user.cluster_name}/{user.name}/{key_a}"
        secret_uri_b = f"secret://{user.cluster_name}/{user.name}/{key_b}"
        secret_uri_c = f"secret://{user.cluster_name}/{user.name}/{key_c}"

        env_var_a = "ENV_SECRET_A"
        env_var_b = "ENV_SECRET_B"
        env_var_c = "ENV_SECRET_C"
        secret_env = {
            env_var_a: secret_uri_a,
            env_var_b: secret_uri_b,
            env_var_c: secret_uri_c,
        }
        job_submit["container"]["secret_env"] = secret_env

        sec_path_1 = "/container/file_1.txt"
        sec_path_2 = "/container/file_2.txt"
        sec_path_3 = "/container/file_3.txt"
        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": sec_path_1},
            {"src_secret_uri": secret_uri_2, "dst_path": sec_path_2},
            {"src_secret_uri": secret_uri_3, "dst_path": sec_path_3},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "${env_var_a}" == "{value_a}" ]',
                f'[ "${env_var_b}" == "{value_b}" ]',
                f'[ "${env_var_c}" == "{value_c}" ]',
                f'[ "$(cat {sec_path_1})" == "{value_1}" ]',
                f'[ "$(cat {sec_path_2})" == "{value_2}" ]',
                f'[ "$(cat {sec_path_3})" == "{value_3}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(
            job_submit, user, secret_env=secret_env, secret_volumes=secret_volumes
        )

    @pytest.mark.asyncio
    async def test_create_job_with_secret_same_secret_in_env_and_volumes_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        """Same secret key is allowed to be mounted as a secret volume
        and a secret env simultaneously.
        """
        user = regular_user
        key_common, key_env, key_vol = "key_1", "key_2", "key_3"
        secret_value_common = "value1"
        secret_value_env = "value2"
        secret_value_vol = "value3"
        await regular_secrets_client.create_secret(key_common, secret_value_common)
        await regular_secrets_client.create_secret(key_env, secret_value_env)
        await regular_secrets_client.create_secret(key_vol, secret_value_vol)

        secret_uri_common = f"secret://{user.cluster_name}/{user.name}/{key_common}"
        secret_uri_env = f"secret://{user.cluster_name}/{user.name}/{key_env}"
        secret_uri_vol = f"secret://{user.cluster_name}/{user.name}/{key_vol}"

        env_var_common = "ENV_SECRET_A"
        env_var_env = "ENV_SECRET_B"
        sec_path_common = "/container/file_a.txt"
        sec_path_vol = "/container/file_b.txt"

        secret_env = {
            env_var_common: secret_uri_common,
            env_var_env: secret_uri_env,
        }
        job_submit["container"]["secret_env"] = secret_env
        secret_volumes = [
            {"src_secret_uri": secret_uri_common, "dst_path": sec_path_common},
            {"src_secret_uri": secret_uri_vol, "dst_path": sec_path_vol},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "${env_var_common}" == "{secret_value_common}" ]',
                f'[ "${env_var_env}" == "{secret_value_env}" ]',
                f'[ "$(cat {sec_path_common})" == "{secret_value_common}" ]',
                f'[ "$(cat {sec_path_vol})" == "{secret_value_vol}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(
            job_submit, user, secret_env=secret_env, secret_volumes=secret_volumes
        )

    @pytest.mark.asyncio
    async def test_create_job_with_secret_same_secret_env_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        user = regular_user
        key_1, key_2, key_3 = "key_1", "key_2", "key_3"
        secret_value_1 = "value1"
        secret_value_2 = "value2"
        secret_value_3 = "value3"
        await regular_secrets_client.create_secret(key_1, secret_value_1)
        await regular_secrets_client.create_secret(key_2, secret_value_2)
        await regular_secrets_client.create_secret(key_3, secret_value_3)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"

        env_var_a = "ENV_SECRET_A"
        env_var_b = "ENV_SECRET_B"
        env_var_c = "ENV_SECRET_C"

        secret_env = {
            env_var_a: secret_uri_1,
            env_var_b: secret_uri_2,
            env_var_c: secret_uri_2,
        }
        job_submit["container"]["secret_env"] = secret_env

        asserts = " && ".join(
            [
                f'[ "${env_var_a}" == "{secret_value_1}" ]',
                f'[ "${env_var_b}" == "{secret_value_2}" ]',
                f'[ "${env_var_c}" == "{secret_value_2}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_env=secret_env)

    @pytest.mark.asyncio
    async def test_create_job_with_secret_same_secret_volumes_different_dirs_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        user = regular_user
        key_1, key_2, key_3 = "key_1", "key_2", "key_3"
        secret_value_1 = "value1"
        secret_value_2 = "value2"
        secret_value_3 = "value3"
        await regular_secrets_client.create_secret(key_1, secret_value_1)
        await regular_secrets_client.create_secret(key_2, secret_value_2)
        await regular_secrets_client.create_secret(key_3, secret_value_3)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"

        secret_path_a = "/etc/path_a/file.txt"
        secret_path_b = "/etc/path_b/file.txt"
        secret_path_c = "/etc/path_c/file.txt"

        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": secret_path_a},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_b},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_c},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "$(cat {secret_path_a})" == "{secret_value_1}" ]',
                f'[ "$(cat {secret_path_b})" == "{secret_value_2}" ]',
                f'[ "$(cat {secret_path_c})" == "{secret_value_2}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_volumes=secret_volumes)

    @pytest.mark.asyncio
    async def test_create_job_with_secret_same_secret_volumes_different_filenames_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        user = regular_user
        key_1, key_2, key_3 = "key_1", "key_2", "key_3"
        secret_value_1 = "value1"
        secret_value_2 = "value2"
        secret_value_3 = "value3"
        await regular_secrets_client.create_secret(key_1, secret_value_1)
        await regular_secrets_client.create_secret(key_2, secret_value_2)
        await regular_secrets_client.create_secret(key_3, secret_value_3)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"

        secret_path_a = "/etc/path/file_a.txt"
        secret_path_b = "/etc/path/file_b.txt"
        secret_path_c = "/etc/path/file_c.txt"

        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": secret_path_a},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_b},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_c},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "$(cat {secret_path_a})" == "{secret_value_1}" ]',
                f'[ "$(cat {secret_path_b})" == "{secret_value_2}" ]',
                f'[ "$(cat {secret_path_c})" == "{secret_value_2}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_volumes=secret_volumes)

    @pytest.mark.asyncio
    async def test_create_job_with_secret_volumes_relative_directory_ok(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        user = regular_user
        key_1, key_2 = "key_1", "key_2"
        secret_value_1 = "value1"
        secret_value_2 = "value2"
        await regular_secrets_client.create_secret(key_1, secret_value_1)
        await regular_secrets_client.create_secret(key_2, secret_value_2)

        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/{key_1}"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/{key_2}"

        secret_path_a = "/foo/file_a.txt"
        secret_path_b = "/foo/bar/file_b.txt"

        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": secret_path_a},
            {"src_secret_uri": secret_uri_2, "dst_path": secret_path_b},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "$(cat {secret_path_a})" == "{secret_value_1}" ]',
                f'[ "$(cat {secret_path_b})" == "{secret_value_2}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(job_submit, user, secret_volumes=secret_volumes)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_missing_all_user_secrets_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        secret_kind: str,
    ) -> None:
        user = regular_user
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/key2"
        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/key1"

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_2": secret_uri_2,
                "ENV_SECRET_1": secret_uri_1,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_2, "dst_path": "/container/path2"},
                {"src_secret_uri": secret_uri_1, "dst_path": "/container/path1"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        url = api.jobs_base_url
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            result = await resp.json()
            assert result["error"] == "Missing secrets: 'key1', 'key2'"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_missing_all_requested_secrets_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        secret_kind: str,
    ) -> None:
        """This test checks the case when the user's k8s secret
        'user--{user_name}--secrets' is present, but requested secret
        key not found
        """
        user = regular_user
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/key2"
        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/key1"

        await regular_secrets_client.create_secret("key3", "value1")

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_2": secret_uri_2,
                "ENV_SECRET_1": secret_uri_1,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_2, "dst_path": "/container/path2"},
                {"src_secret_uri": secret_uri_1, "dst_path": "/container/path1"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        url = api.jobs_base_url
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            result = await resp.json()
            assert result["error"] == "Missing secrets: 'key1', 'key2'"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_env_missing_some_requested_secrets_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        regular_secrets_client: SecretsClient,
        secret_kind: str,
    ) -> None:
        user = regular_user
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/key2"
        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/key1"

        await regular_secrets_client.create_secret("key1", "value1")

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_2": secret_uri_2,
                "ENV_SECRET_1": secret_uri_1,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_2, "dst_path": "/container/path2"},
                {"src_secret_uri": secret_uri_1, "dst_path": "/container/path1"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        url = api.jobs_base_url
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            result = await resp.json()
            assert result["error"] == "Missing secrets: 'key2'"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_env_use_other_user_secret_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        test_cluster_name: str,
        regular_user_factory: Callable[..., Awaitable[_User]],
        secrets_client_factory: Callable[..., AsyncContextManager[SecretsClient]],
        secret_kind: str,
    ) -> None:
        cluster = test_cluster_name
        usr_1 = await regular_user_factory(cluster_name=cluster)
        usr_2 = await regular_user_factory(cluster_name=cluster)

        key_1, key_2 = "key_1", "key_2"

        async with secrets_client_factory(usr_1) as sec_client:
            await sec_client.create_secret(key_1, "value1")

        async with secrets_client_factory(usr_1) as sec_client:
            await sec_client.create_secret(key_2, "value2")

        secret_uri_1 = f"secret://{cluster}/{usr_1.name}/{key_1}"
        secret_uri_2 = f"secret://{cluster}/{usr_2.name}/{key_2}"

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_2": secret_uri_2,
                "ENV_SECRET_1": secret_uri_1,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_2, "dst_path": "/container/path2"},
                {"src_secret_uri": secret_uri_1, "dst_path": "/container/path1"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        url = api.jobs_base_url
        async with client.post(url, headers=usr_1.headers, json=job_submit) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            result = await resp.json()
            assert result == {"missing": [{"uri": secret_uri_2, "action": "read"}]}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_env_use_other_user_secret_success(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        test_cluster_name: str,
        regular_user_factory: Callable[..., Awaitable[_User]],
        secrets_client_factory: Callable[..., AsyncContextManager[SecretsClient]],
        secret_kind: str,
        share_secret: Callable[..., Awaitable[None]],
        _run_job_with_secrets: Callable[..., Awaitable[None]],
    ) -> None:
        cluster_name = test_cluster_name
        usr_1 = await regular_user_factory(cluster_name=cluster_name)
        usr_2 = await regular_user_factory(cluster_name=cluster_name)

        key_1, key_2, key_3 = "key_1", "key_2", "key_3"
        key_a, key_b, key_c = "key_a", "key_b", "key_c"
        value_1, value_2, value_3 = "value_1", "value_2", "value_3"
        value_a, value_b, value_c = "value_a", "value_b", "value_c"
        async with secrets_client_factory(usr_1) as sec_client:
            await sec_client.create_secret(key_1, value_1)
            await sec_client.create_secret(key_2, value_2)
            await sec_client.create_secret(key_3, value_3)
            await sec_client.create_secret(key_a, value_a)
            await sec_client.create_secret(key_b, value_b)
            await sec_client.create_secret(key_c, value_c)

        for key in (key_1, key_2, key_3, key_a, key_b, key_c):
            await share_secret(usr_1, usr_2, key)

        secret_uri_1 = f"secret://{cluster_name}/{usr_1.name}/{key_1}"
        secret_uri_2 = f"secret://{cluster_name}/{usr_1.name}/{key_2}"
        secret_uri_3 = f"secret://{cluster_name}/{usr_1.name}/{key_3}"
        secret_uri_a = f"secret://{cluster_name}/{usr_1.name}/{key_a}"
        secret_uri_b = f"secret://{cluster_name}/{usr_1.name}/{key_b}"
        secret_uri_c = f"secret://{cluster_name}/{usr_1.name}/{key_c}"

        env_var_a = "ENV_SECRET_A"
        env_var_b = "ENV_SECRET_B"
        env_var_c = "ENV_SECRET_C"
        secret_env = {
            env_var_a: secret_uri_a,
            env_var_b: secret_uri_b,
            env_var_c: secret_uri_c,
        }
        job_submit["container"]["secret_env"] = secret_env

        sec_path_1 = "/container/file_1.txt"
        sec_path_2 = "/container/file_2.txt"
        sec_path_3 = "/container/file_3.txt"
        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": sec_path_1},
            {"src_secret_uri": secret_uri_2, "dst_path": sec_path_2},
            {"src_secret_uri": secret_uri_3, "dst_path": sec_path_3},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes

        asserts = " && ".join(
            [
                f'[ "${env_var_a}" == "{value_a}" ]',
                f'[ "${env_var_b}" == "{value_b}" ]',
                f'[ "${env_var_c}" == "{value_c}" ]',
                f'[ "$(cat {sec_path_1})" == "{value_1}" ]',
                f'[ "$(cat {sec_path_2})" == "{value_2}" ]',
                f'[ "$(cat {sec_path_3})" == "{value_3}" ]',
            ]
        )
        cmd = f"bash -c '{asserts}'"
        job_submit["container"]["command"] = cmd

        await _run_job_with_secrets(
            job_submit, usr_2, secret_env=secret_env, secret_volumes=secret_volumes
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_env_wrong_scheme_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        secret_kind: str,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        wrong_scheme = "wrong-scheme"
        secret_uri_good = f"secret://{user.cluster_name}/{user.name}/key_1"
        secret_uri_wrong = f"{wrong_scheme}://{user.cluster_name}/{user.name}/key_2"

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_A": secret_uri_good,
                "ENV_SECRET_B": secret_uri_wrong,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_wrong, "dst_path": "/container/path_1"},
                {"src_secret_uri": secret_uri_good, "dst_path": "/container/path_2"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid URI scheme: '{wrong_scheme}' != 'secret'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    @pytest.mark.parametrize("secret_kind", ["secret_env", "secret_volumes"])
    async def test_create_job_with_secret_env_wrong_cluster_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
        secret_kind: str,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        wrong_cluster = "wrong-cluster-name"
        secret_uri_good = f"secret://{user.cluster_name}/{user.name}/key_1"
        secret_uri_wrong = f"secret://{wrong_cluster}/{user.name}/key_2"

        if secret_kind == "secret_env":
            secret_env = {
                "ENV_SECRET_A": secret_uri_good,
                "ENV_SECRET_B": secret_uri_wrong,
            }
            job_submit["container"]["secret_env"] = secret_env
        else:
            secret_volumes = [
                {"src_secret_uri": secret_uri_good, "dst_path": "/container/path_1"},
                {"src_secret_uri": secret_uri_wrong, "dst_path": "/container/path_2"},
            ]
            job_submit["container"]["secret_volumes"] = secret_volumes

        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid URI cluster: '{wrong_cluster}' != '{user.cluster_name}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_secret_volume_invalid_mount_with_dots_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        secret_uri = f"secret://{user.cluster_name}/{user.name}/key_1"
        invalid_path = "/container/path_1/../path_2"
        secret_volumes = [
            {"src_secret_uri": secret_uri, "dst_path": invalid_path},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Invalid path: '{invalid_path}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_secret_volume_invalid_mount_relative_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        secret_uri = f"secret://{user.cluster_name}/{user.name}/key_1"
        invalid_path = "container/path_1"
        secret_volumes = [
            {"src_secret_uri": secret_uri, "dst_path": invalid_path},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = f"Mount path must be absolute: '{invalid_path}'"
            assert err in msg["error"], msg

    @pytest.mark.asyncio
    async def test_create_job_with_and_secret_volumes_same_mount_points_fail(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        user = regular_user
        secret_uri_1 = f"secret://{user.cluster_name}/{user.name}/key_1"
        secret_uri_2 = f"secret://{user.cluster_name}/{user.name}/key_2"
        secret_volumes = [
            {"src_secret_uri": secret_uri_1, "dst_path": "/container/path"},
            {"src_secret_uri": secret_uri_2, "dst_path": "/container/path"},
        ]
        job_submit["container"]["secret_volumes"] = secret_volumes
        async with client.post(url, headers=user.headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            msg = await resp.json()
            err = "destination path '/container/path' was encountered multiple times"
            assert err in msg["error"], msg["error"]

    @pytest.mark.asyncio
    async def test_create_job_set_max_run_time(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        job_submit["max_run_time_minutes"] = 10
        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] in ["pending"]
            assert result["max_run_time_minutes"] == 10

    @pytest.mark.asyncio
    async def test_get_job_run_time_seconds(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url
        job_submit["container"]["command"] = "sleep 3"
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            assert result["status"] in ["pending"]
            job_id = result["id"]

        await jobs_client.long_polling_by_job_id(job_id, "succeeded")

        url = api.generate_job_url(job_id)
        async with client.get(url, headers=headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            run_time = result["history"]["run_time_seconds"]
            # since jobs_poller works with delay 1 sec for each transition,
            # so we should give it time to actually kill the job
            assert 3 - 2 < run_time < 3 + 2

    @pytest.mark.asyncio
    async def test_create_job_volume_wrong_storage_scheme(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url
        job_submit["container"]["volumes"] = [
            {
                "src_storage_uri": f"wrong-scheme://{regular_user.cluster_name}/"
                f"{regular_user.name}",
                "dst_path": "/var/storage",
                "read_only": False,
            }
        ]
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            payload = await resp.json()
            err = "Invalid URI scheme: 'wrong-scheme'"
            assert err in payload["error"]

    @pytest.mark.asyncio
    async def test_create_job_volume_wrong_cluster_name(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url
        job_submit["container"]["volumes"] = [
            {
                "src_storage_uri": "storage://wrong-cluster/" f"{regular_user.name}",
                "dst_path": "/var/storage",
                "read_only": False,
            }
        ]
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            payload = await resp.json()
            err = "Invalid URI cluster: 'wrong-cluster' != 'test-cluster'"
            assert err in payload["error"]

    @pytest.mark.asyncio
    async def test_create_job_volume_wrong_path_with_dots(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url
        job_submit["container"]["volumes"] = [
            {
                "src_storage_uri": f"storage://{regular_user.cluster_name}/"
                f"{regular_user.name}",
                "dst_path": "/var/storage/../another",
                "read_only": False,
            }
        ]
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            payload = await resp.json()
            err = "Invalid path: '/var/storage/../another'"
            assert err in payload["error"]

    @pytest.mark.asyncio
    async def test_create_job_volume_wrong_path_not_absolute(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        headers = regular_user.headers
        url = api.jobs_base_url
        job_submit["container"]["volumes"] = [
            {
                "src_storage_uri": f"storage://{regular_user.cluster_name}/"
                f"{regular_user.name}",
                "dst_path": "var/storage",
                "read_only": False,
            }
        ]
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()
            payload = await resp.json()
            err = "Mount path must be absolute: 'var/storage'"
            assert err in payload["error"]

    @pytest.mark.asyncio
    async def test_incorrect_request(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        json_job_submit = {"wrong_key": "wrong_value"}
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=json_job_submit
        ) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
            data = await response.json()
            assert """'container': DataError(is required)""" in data["error"]

    @pytest.mark.asyncio
    async def test_broken_docker_image(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
        cluster_name: str,
    ) -> None:
        payload = {
            "container": {
                "image": "some_broken_image",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}",
                        "dst_path": "/var/storage",
                        "read_only": False,
                    },
                    {
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}/result",
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
            assert response.status == HTTPAccepted.status_code, await response.text()
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
        cluster_name: str,
    ) -> None:
        payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": f"storage://{cluster_name}",
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
            data = await response.json()
            assert data == {
                "missing": [{"action": "write", "uri": f"storage://{cluster_name}"}]
            }

    @pytest.mark.asyncio
    async def test_forbidden_image(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
        cluster_name: str,
    ) -> None:
        payload = {
            "container": {
                "image": "registry.dev.neuromation.io/anotheruser/image:tag",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
            }
        }

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            data = await response.json()
            assert data == {
                "missing": [
                    {
                        "action": "read",
                        "uri": f"image://{cluster_name}/anotheruser/image",
                    }
                ]
            }

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
            assert response.status == HTTPBadRequest.status_code, await response.text()
            payload = await response.json()
            e = (
                r"{'name': DataError({0: DataError(value should be None), "
                r"1: DataError(does not match pattern \A[a-z](?:-?[a-z0-9])*\Z)})}"
            )
            assert payload == {"error": e}

    @pytest.mark.asyncio
    async def test_create_job_user_has_unknown_cluster_name(
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
            assert response.status == HTTPForbidden.status_code, await response.text()
            payload = await response.json()
            assert payload == {"error": "No clusters"}

    @pytest.mark.asyncio
    async def test_create_job_unknown_cluster_name(
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
        job_submit["cluster_name"] = "unknown"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            payload = await response.json()
            assert payload == {
                "error": "User is not allowed to submit jobs to the specified cluster"
            }

    @pytest.mark.asyncio
    async def test_create_job_no_clusters(
        self,
        api: ApiConfig,
        auth_api: AuthApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        admin_token: str,
        regular_user: _User,
    ) -> None:
        admin_user = _User(name="admin", token=admin_token)
        user = regular_user

        url = auth_api.auth_for_user_url(user.name)
        payload = {"name": user.name, "cluster_name": "unknowncluster"}
        async with client.put(url, headers=admin_user.headers, json=payload) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            payload = await response.json()
            assert payload == {"error": "No clusters"}

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
        job_submit["is_preemptible_node_required"] = False
        job_submit["name"] = job_name
        job_submit["container"]["entrypoint"] = "/bin/echo"
        job_submit["container"]["command"] = "false"
        job_submit["container"]["http"]["requires_auth"] = True
        job_submit["schedule_timeout"] = 90
        job_submit["cluster_name"] = "test-cluster"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] in ["pending"]
            assert payload["name"] == job_name
            assert payload["container"]["entrypoint"] == "/bin/echo"
            assert payload["container"]["command"] == "false"
            assert payload["http_url"] == f"http://{job_id}.jobs.neu.ro"
            assert (
                payload["http_url_named"]
                == f"http://{job_name}--{regular_user.name}.jobs.neu.ro"
            )
            expected_internal_hostname = f"{job_id}.platformapi-tests"
            assert payload["internal_hostname"] == expected_internal_hostname
            assert payload["is_preemptible"]
            assert not payload["is_preemptible_node_required"]
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
    async def test_create_job_from_preset(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        job_name = f"test-job-name-{random_str()}"
        preset_name = "cpu-micro"
        url = URL(api.jobs_base_url).with_query("from_preset")
        job_submit.update(**job_submit["container"])
        del job_submit["container"]
        del job_submit["resources"]
        job_submit["name"] = job_name
        job_submit["preset_name"] = preset_name
        job_submit["entrypoint"] = "/bin/echo"
        job_submit["command"] = "false"
        job_submit["http"]["requires_auth"] = True
        job_submit["schedule_timeout"] = 90
        job_submit["cluster_name"] = "test-cluster"
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            payload = await resp.json()
            job_id = payload["id"]
            assert payload["status"] in ["pending"]
            assert payload["name"] == job_name
            assert payload["preset_name"] == preset_name

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
            assert resp.status == HTTPAccepted.status_code, await resp.text()
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
            assert response.status == HTTPAccepted.status_code, await response.text()
            payload = await response.json()
            job_id = payload["id"]

        await jobs_client.long_polling_by_job_id(job_id, status="running")

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
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
    async def test_create_job_with_tags(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        regular_user: _User,
        jobs_client: JobsClient,
    ) -> None:
        headers = regular_user.headers
        job_submit["tags"] = ["tag2", "tag1", "tag3", "tag1"]

        url = api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            payload = await response.json()
            job_id = payload["id"]
            assert payload["tags"] == ["tag1", "tag2", "tag3"]

        url = api.generate_job_url(job_id)
        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            payload = await response.json()
            assert payload["tags"] == ["tag1", "tag2", "tag3"]

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
            assert response.status == HTTPAccepted.status_code, await response.text()

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
            assert response.status == HTTPAccepted.status_code, await response.text()

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
            assert response.status == HTTPBadRequest.status_code, await response.text()
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
            assert response.status == HTTPBadRequest.status_code, await response.text()
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
            assert response.status == HTTPAccepted.status_code, await response.text()
            payload = await response.json()
            job_id = payload["id"]

        await jobs_client.long_polling_by_job_id(job_id, status="running")
        await jobs_client.delete_job(job_id)
        await jobs_client.long_polling_by_job_id(job_id, status="cancelled")

        async with client.post(url, headers=headers, json=job_submit) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            payload = await response.json()
            job_id = payload["id"]
            await jobs_client.delete_job(job_id)

    @pytest.mark.asyncio
    async def test_get_all_jobs_clear(self, jobs_client: JobsClient) -> None:
        jobs = await jobs_client.get_all_jobs()
        assert jobs == []

    @pytest.mark.asyncio
    async def test_get_all_jobs_bad_args(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client: JobsClient,
        regular_user: _User,
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers.copy()
        headers["Accept"] = "application/x-ndjson"

        params = [("reverse", "spam")]
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
            data = await response.json()
            assert 'Required "0", "1", "false" or "true"' in data["error"]

        params = [("limit", "spam")]
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
            data = await response.json()
            assert "invalid literal for int" in data["error"]

        params = [("limit", "0")]
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
            data = await response.json()
            assert "limit should be > 0" in data["error"]

    @pytest.mark.asyncio
    async def test_get_all_jobs_not_streamed(
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
        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job1_id = result["id"]
        job_request = job_request_factory()
        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job2_id = result["id"]

        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert response.headers["Content-Type"] == "application/json; charset=utf-8"
            result = await response.json()

        jobs = result["jobs"]
        assert isinstance(jobs, list)
        for job in jobs:
            assert isinstance(job, dict)
            for key in job:
                assert isinstance(key, str)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job1_id, job2_id}

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
            assert response.status == HTTPBadRequest.status_code, await response.text()

        filters2 = [("status", "running"), ("status", "abrakadabra")]
        async with client.get(url, headers=headers, params=filters2) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()

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
            assert resp.status == HTTPAccepted.status_code, await resp.text()
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
    async def test_get_all_jobs_filter_by_tags(
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

        job_request["tags"] = ["tag1", "tag2"]
        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job_1 = result["id"]

        job_request["tags"] = ["tag2", "tag3"]
        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job_2 = result["id"]

        jobs = await jobs_client.get_all_jobs()
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_1, job_2}

        filters: Any
        filters = {"tag": "tag1"}
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_1}

        filters = {"tag": "tag2"}
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_1, job_2}

        filters = [("tag", "tag1"), ("tag", "tag2")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_1}

        filters = [("tag", "tag1"), ("tag", "tag3")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert not job_ids

        filters = [("tag", "tag3"), ("tag", "tag-non-existing")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert not job_ids

        filters = {"tag": "tag-non-existing"}
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert not job_ids

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
                assert resp.status == HTTPAccepted.status_code, await resp.text()
                result = await resp.json()
                job_ids_list.append(result["id"])

        job_ids_killed = set(job_ids_list[:2])
        job_ids_alive = set(job_ids_list[2:])
        job_ids_all = set(job_ids_list)

        for job_id in job_ids_all:
            await jobs_client.long_polling_by_job_id(job_id, status="running")

        for job_id in job_ids_killed:
            await jobs_client.delete_job(job_id=job_id)
            await jobs_client.long_polling_by_job_id(job_id, status="cancelled")

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
            ("status", "cancelled"),
        ]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_all

        # single status, actually filter out values
        filters2 = {"status": "cancelled"}
        jobs = await jobs_client.get_all_jobs(filters2)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == job_ids_killed

        # cleanup
        for job_id in job_ids_alive:
            await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_date_range(
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

        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job_1 = result["id"]
            t1 = result["history"]["created_at"]

        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job_2 = result["id"]
            t2 = result["history"]["created_at"]

        async with client.post(url, headers=headers, json=job_request) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            job_3 = result["id"]
            t3 = result["history"]["created_at"]

        job_ids = {job["id"] for job in await jobs_client.get_all_jobs()}
        assert job_ids == {job_1, job_2, job_3}

        filters = {"since": t2}
        job_ids = {job["id"] for job in await jobs_client.get_all_jobs(filters)}
        assert job_ids == {job_2, job_3}

        filters = {"until": t2}
        job_ids = {job["id"] for job in await jobs_client.get_all_jobs(filters)}
        assert job_ids == {job_1, job_2}

        filters = {"since": t1, "until": t2}
        job_ids = {job["id"] for job in await jobs_client.get_all_jobs(filters)}
        assert job_ids == {job_1, job_2}

        filters = {"since": t1, "until": t3}
        job_ids = {job["id"] for job in await jobs_client.get_all_jobs(filters)}
        assert job_ids == {job_1, job_2, job_3}

    @pytest.fixture
    async def run_job(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        jobs_client_factory: Callable[[_User], JobsClient],
    ) -> AsyncIterator[Callable[[_User, Dict[str, Any], bool, bool], Awaitable[str]]]:
        cleanup_pairs = []

        async def _impl(
            user: _User,
            job_request: Dict[str, Any],
            do_kill: bool = False,
            do_wait: bool = True,
        ) -> str:
            url = api.jobs_base_url
            headers = user.headers
            jobs_client = jobs_client_factory(user)
            async with client.post(url, headers=headers, json=job_request) as resp:
                assert resp.status == HTTPAccepted.status_code, (
                    await resp.text(),
                    str(job_request),
                )
                data = await resp.json()
                job_id = data["id"]
                if do_wait:
                    await jobs_client.long_polling_by_job_id(job_id, "running")
                if do_kill:
                    await jobs_client.delete_job(job_id)
                    await jobs_client.long_polling_by_job_id(job_id, "cancelled")
                else:
                    cleanup_pairs.append((jobs_client, job_id))
            return job_id

        yield _impl

        for jobs_client, job_id in cleanup_pairs:
            await jobs_client.delete_job(job_id=job_id, assert_success=False)

    @pytest.fixture
    async def share_job(
        self, auth_client: AuthClient, cluster_name: str
    ) -> AsyncIterator[Callable[[_User, _User, Any], Awaitable[None]]]:
        async def _impl(
            owner: _User, follower: _User, job_id: str, action: str = "read"
        ) -> None:
            permission = Permission(
                uri=f"job://{cluster_name}/{owner.name}/{job_id}", action=action
            )
            await auth_client.grant_user_permissions(
                follower.name, [permission], token=owner.token
            )

        yield _impl

    @pytest.fixture
    async def share_secret(
        self, auth_client: AuthClient, cluster_name: str
    ) -> AsyncIterator[Callable[[_User, _User, Any], Awaitable[None]]]:
        async def _impl(
            owner: _User, follower: _User, secret_name: str, action: str = "read"
        ) -> None:
            permission = Permission(
                uri=f"secret://{cluster_name}/{owner.name}/{secret_name}", action=action
            )
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
        run_job: Callable[..., Awaitable[str]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_req_no_name = create_job_request_no_name()
        job_req_with_name = create_job_request_with_name(job_name)
        usr = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr)

        # Run jobs in mixed order
        job_usr_with_name_killed = await run_job(usr, job_req_with_name, do_kill=True)
        job_usr_no_name_killed = await run_job(usr, job_req_no_name, do_kill=True)
        job_usr_no_name = await run_job(usr, job_req_no_name, do_kill=False)
        job_usr_with_name = await run_job(usr, job_req_with_name, do_kill=False)

        # filter: job name
        filters = [("name", job_name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr_with_name_killed, job_usr_with_name]

        # filter: multiple statuses
        filters = [
            ("status", "running"),
            ("status", "cancelled"),
            ("status", "succeeded"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr_with_name_killed,
            job_usr_no_name_killed,
            job_usr_no_name,
            job_usr_with_name,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr_with_name,
            job_usr_no_name,
            job_usr_no_name_killed,
            job_usr_with_name_killed,
        ]

        # filter: name + status
        filters = [("name", job_name), ("status", "cancelled")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr_with_name_killed]

        # filter: name + multiple statuses
        filters = [
            ("name", job_name),
            ("status", "running"),
            ("status", "succeeded"),
            ("status", "cancelled"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr_with_name_killed, job_usr_with_name]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr_with_name, job_usr_with_name_killed]

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_self_owner_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[str]],
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

        # Run jobs in mixed order
        job_usr1_with_name_killed = await run_job(usr1, job_req_with_name, do_kill=True)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)
        job_usr1_no_name_killed = await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        job_usr1_no_name = await run_job(usr1, job_req_no_name, do_kill=False)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        job_usr1_with_name = await run_job(usr1, job_req_with_name, do_kill=False)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: self owner
        filters = [("owner", usr1.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name_killed,
            job_usr1_no_name_killed,
            job_usr1_no_name,
            job_usr1_with_name,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name,
            job_usr1_no_name,
            job_usr1_no_name_killed,
            job_usr1_with_name_killed,
        ]

        # filter: self owner + job name
        filters = [("name", job_name), ("owner", usr1.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_with_name_killed, job_usr1_with_name]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_with_name, job_usr1_with_name_killed]

        # filter: self owner + status
        filters = [("owner", usr1.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_no_name, job_usr1_with_name]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_with_name, job_usr1_no_name]

        # filter: self owner + name + status
        filters = [("owner", usr1.name), ("name", job_name), ("status", "cancelled")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_with_name_killed]

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_another_owner_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[str]],
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

        # Run jobs in mixed order
        await run_job(usr1, job_req_with_name, do_kill=True)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)
        await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        await run_job(usr1, job_req_no_name, do_kill=False)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        await run_job(usr1, job_req_with_name, do_kill=False)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: another owner
        filters = [("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr2_no_name,
            job_usr2_with_name_killed,
            job_usr2_with_name,
            job_usr2_no_name_killed,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr2_no_name_killed,
            job_usr2_with_name,
            job_usr2_with_name_killed,
            job_usr2_no_name,
        ]

        # filter: another owner + job name
        filters = [("name", job_name), ("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_with_name_killed, job_usr2_with_name]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_with_name, job_usr2_with_name_killed]

        # filter: another owner + status
        filters = [("owner", usr2.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_no_name, job_usr2_with_name]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_with_name, job_usr2_no_name]

        # filter: another owner + name + status
        filters = [("owner", usr2.name), ("name", job_name), ("status", "cancelled")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_with_name_killed]

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_multiple_owners_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        job_request_factory: Callable[[], Dict[str, Any]],
        run_job: Callable[..., Awaitable[str]],
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

        # Run jobs in mixed order
        job_usr1_with_name_killed = await run_job(usr1, job_req_with_name, do_kill=True)
        job_usr2_no_name = await run_job(usr2, job_req_no_name, do_kill=False)
        job_usr1_no_name_killed = await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr2_with_name_killed = await run_job(usr2, job_req_with_name, do_kill=True)
        job_usr1_no_name = await run_job(usr1, job_req_no_name, do_kill=False)
        job_usr2_with_name = await run_job(usr2, job_req_with_name, do_kill=False)
        job_usr1_with_name = await run_job(usr1, job_req_with_name, do_kill=False)
        job_usr2_no_name_killed = await run_job(usr2, job_req_no_name, do_kill=True)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2_with_name_killed)
        await share_job(usr2, usr1, job_usr2_no_name_killed)
        await share_job(usr2, usr1, job_usr2_with_name)
        await share_job(usr2, usr1, job_usr2_no_name)

        # filter: multiple owners
        filters = [("owner", usr1.name), ("owner", usr2.name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name_killed,
            job_usr2_no_name,
            job_usr1_no_name_killed,
            job_usr2_with_name_killed,
            job_usr1_no_name,
            job_usr2_with_name,
            job_usr1_with_name,
            job_usr2_no_name_killed,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr2_no_name_killed,
            job_usr1_with_name,
            job_usr2_with_name,
            job_usr1_no_name,
            job_usr2_with_name_killed,
            job_usr1_no_name_killed,
            job_usr2_no_name,
            job_usr1_with_name_killed,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("limit", "4")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name_killed,
            job_usr2_no_name,
            job_usr1_no_name_killed,
            job_usr2_with_name_killed,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(
            filters + [("limit", "4"), ("reverse", "1")]
        )
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr2_no_name_killed,
            job_usr1_with_name,
            job_usr2_with_name,
            job_usr1_no_name,
        ]

        # filter: multiple owners + job name
        filters = [("owner", usr1.name), ("owner", usr2.name), ("name", job_name)]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name_killed,
            job_usr2_with_name_killed,
            job_usr2_with_name,
            job_usr1_with_name,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name,
            job_usr2_with_name,
            job_usr2_with_name_killed,
            job_usr1_with_name_killed,
        ]

        # filter: multiple owners + status
        filters = [("owner", usr1.name), ("owner", usr2.name), ("status", "running")]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr2_no_name,
            job_usr1_no_name,
            job_usr2_with_name,
            job_usr1_with_name,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name,
            job_usr2_with_name,
            job_usr1_no_name,
            job_usr2_no_name,
        ]

        # filter: multiple owners + name + status
        filters = [
            ("owner", usr1.name),
            ("owner", usr2.name),
            ("name", job_name),
            ("status", "cancelled"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr1_with_name_killed, job_usr2_with_name_killed]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [job_usr2_with_name_killed, job_usr1_with_name_killed]

        # filter: multiple owners + name + multiple statuses
        filters = [
            ("owner", usr1.name),
            ("owner", usr2.name),
            ("name", job_name),
            ("status", "running"),
            ("status", "cancelled"),
            ("status", "succeeded"),
        ]
        jobs = await jobs_client_usr1.get_all_jobs(filters)
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name_killed,
            job_usr2_with_name_killed,
            job_usr2_with_name,
            job_usr1_with_name,
        ]

        jobs = await jobs_client_usr1.get_all_jobs(filters + [("reverse", "1")])
        job_ids = [job["id"] for job in jobs]
        assert job_ids == [
            job_usr1_with_name,
            job_usr2_with_name,
            job_usr2_with_name_killed,
            job_usr1_with_name_killed,
        ]

    @pytest.mark.asyncio
    async def test_get_all_jobs_filter_by_job_name_owner_and_status_invalid_name(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.jobs_base_url
        headers = regular_user.headers

        # filter by name only
        filters = {"name": "InValid_Name.txt"}
        async with client.get(url, headers=headers, params=filters) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()

        # filter by name and status
        filters2 = [("status", "running"), ("name", "InValid_Name.txt")]
        async with client.get(url, headers=headers, params=filters2) as resp:
            assert resp.status == HTTPBadRequest.status_code, await resp.text()

    @pytest.mark.asyncio
    async def test_get_all_jobs_shared(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        regular_user_factory: Callable[[], Any],
        auth_client: AuthClient,
        cluster_name: str,
    ) -> None:
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(
            url, headers=owner.headers, json=job_request
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]

        url = api.jobs_base_url
        headers = owner.headers.copy()
        headers["Accept"] = "application/x-ndjson"
        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert response.headers["Content-Type"] == "application/x-ndjson"
            job_ids = {json.loads(line)["id"] async for line in response.content}
            assert job_ids == {job_id}

        headers = follower.headers.copy()
        headers["Accept"] = "application/x-ndjson"
        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert response.headers["Content-Type"] == "application/x-ndjson"
            job_ids = {json.loads(line)["id"] async for line in response.content}
            assert not job_ids

        permission = Permission(
            uri=f"job://{cluster_name}/{owner.name}/{job_id}", action="read"
        )
        await auth_client.grant_user_permissions(
            follower.name, [permission], token=owner.token
        )

        async with client.get(url, headers=headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert response.headers["Content-Type"] == "application/x-ndjson"
            job_ids = {json.loads(line)["id"] async for line in response.content}
            assert job_ids == {job_id}

    @pytest.mark.asyncio
    async def test_get_shared_job(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        regular_user_factory: Callable[[], Any],
        auth_client: AuthClient,
        cluster_name: str,
    ) -> None:
        owner = await regular_user_factory()
        follower = await regular_user_factory()

        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(
            url, headers=owner.headers, json=job_request
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]

        url = f"{api.jobs_base_url}/{job_id}"
        async with client.get(url, headers=owner.headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            data = await response.json()
            assert data == {
                "missing": [
                    {
                        "action": "read",
                        "uri": f"job://{cluster_name}/{owner.name}/{job_id}",
                    }
                ]
            }

        permission = Permission(
            uri=f"job://{cluster_name}/{owner.name}/{job_id}", action="read"
        )
        await auth_client.grant_user_permissions(
            follower.name, [permission], token=owner.token
        )

        async with client.get(url, headers=follower.headers) as response:
            assert response.status == HTTPOk.status_code, await response.text()

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
                assert (
                    response.status == HTTPAccepted.status_code
                ), await response.text()
                result = await response.json()
                assert result["status"] in ["pending"]
                job_id = result["id"]
                await jobs_client.long_polling_by_job_id(
                    job_id=job_id, status="succeeded"
                )
                jobs_ids.append(job_id)

        jobs = await jobs_client.get_all_jobs()
        assert set(jobs_ids) <= {x["id"] for x in jobs}
        # clean:
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
                    ("status", "cancelled"),
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
                    await jobs_client.long_polling_by_job_id(job_id, status="cancelled")

        jobs_ls = await jobs_client.get_all_jobs(params=filters)
        jobs_ls = [job["id"] for job in jobs_ls]
        assert set(jobs_ids) == set(jobs_ls), "content differs"
        assert jobs_ids == jobs_ls, "order differs"

        # cleanup all:
        for job_id in jobs_ids:
            await jobs_client.delete_job(job_id=job_id)

    @pytest.mark.asyncio
    async def test_get_job_by_cluster_name_and_statuses(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[str]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
    ) -> None:
        job_req_no_name = create_job_request_no_name()
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        jobs_client = jobs_client_factory(usr1)

        job_usr1_killed = await run_job(usr1, job_req_no_name, do_kill=True)
        job_usr1 = await run_job(usr1, job_req_no_name, do_kill=False)
        job_usr2 = await run_job(usr2, job_req_no_name, do_kill=False)

        # usr2 shares their jobs with usr1
        await share_job(usr2, usr1, job_usr2)
        all_job_ids = {job_usr1_killed, job_usr1, job_usr2}

        # filter: test cluster
        filters = [("cluster_name", "test-cluster")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == all_job_ids

        # filter: other cluster
        filters = [("cluster_name", "other-cluster")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == set()

        # filter: test cluster + status
        filters = [("cluster_name", "test-cluster"), ("status", "running")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1, job_usr2}

        # filter: other cluster + status
        filters = [("cluster_name", "other-cluster"), ("status", "running")]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == set()

        # filter: test cluster + self owner
        filters = [("cluster_name", "test-cluster"), ("owner", usr1.name)]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr1_killed, job_usr1}

        # filter: test cluster + other owner
        filters = [("cluster_name", "test-cluster"), ("owner", usr2.name)]
        jobs = await jobs_client.get_all_jobs(filters)
        job_ids = {job["id"] for job in jobs}
        assert job_ids == {job_usr2}

    @pytest.mark.asyncio
    async def test_get_job_by_hostname_self_owner(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[str]],
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
        run_job: Callable[..., Awaitable[str]],
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
        run_job: Callable[..., Awaitable[str]],
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
        run_job: Callable[..., Awaitable[str]],
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
    async def test_set_job_status_no_reason(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        run_job: Callable[..., Awaitable[str]],
        regular_user: _User,
        compute_user: _User,
    ) -> None:
        job_id = await run_job(regular_user, job_submit, do_wait=False)

        url = api.generate_job_url(job_id) + "/status"
        headers = compute_user.headers
        payload = {"status": "failed"}
        async with client.put(url, headers=headers, json=payload) as response:
            if response.status == HTTPConflict.status_code:
                result = await response.json()
                assert result["error"] == f"Job {{id={job_id}}} has changed"
                ok = False
            else:
                assert (
                    response.status == HTTPNoContent.status_code
                ), await response.text()
                ok = True

        if ok:
            result = await jobs_client.get_job_by_id(job_id)
            assert result["status"] == "failed"
            assert result["history"]["status"] == "failed"
            assert result["history"].get("reason") is None
            assert result["history"].get("description") is None
            assert result["history"].get("exit_code") is None

    @pytest.mark.asyncio
    async def test_set_job_status_with_details(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        run_job: Callable[..., Awaitable[str]],
        regular_user: _User,
        compute_user: _User,
    ) -> None:
        job_id = await run_job(regular_user, job_submit, do_wait=False)

        url = api.generate_job_url(job_id) + "/status"
        headers = compute_user.headers
        payload = {
            "status": "failed",
            "reason": "Test failure",
            "description": "test_set_job_status",
            "exit_code": 42,
        }
        async with client.put(url, headers=headers, json=payload) as response:
            if response.status == HTTPConflict.status_code:
                result = await response.json()
                assert result["error"] == f"Job {{id={job_id}}} has changed"
                ok = False
            else:
                assert (
                    response.status == HTTPNoContent.status_code
                ), await response.text()
                ok = True

        if ok:
            result = await jobs_client.get_job_by_id(job_id)
            assert result["status"] == "failed"
            assert result["history"]["status"] == "failed"
            assert result["history"]["reason"] == "Test failure"
            assert result["history"]["description"] == "test_set_job_status"
            assert result["history"]["exit_code"] == 42

    @pytest.mark.asyncio
    async def test_set_job_status_wrong_status(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        run_job: Callable[..., Awaitable[str]],
        regular_user: _User,
        compute_user: _User,
    ) -> None:
        job_id = await run_job(regular_user, job_submit, do_wait=False)

        url = api.generate_job_url(job_id) + "/status"
        headers = compute_user.headers
        payload = {"status": "abrakadabra"}
        async with client.put(url, headers=headers, json=payload) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()

    @pytest.mark.asyncio
    async def test_set_job_status_unprivileged(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        run_job: Callable[..., Awaitable[str]],
        regular_user: _User,
        cluster_name: str,
    ) -> None:
        job_id = await run_job(regular_user, job_submit, do_wait=False)

        url = api.generate_job_url(job_id) + "/status"
        headers = regular_user.headers
        payload = {"status": "running"}
        async with client.put(url, headers=headers, json=payload) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            result = await response.json()
            assert result == {
                "missing": [{"uri": f"job://{cluster_name}", "action": "manage"}]
            }

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
            assert response.status == HTTPAccepted.status_code, await response.text()
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
    async def test_delete_job_forbidden(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        regular_user_factory: Callable[..., Awaitable[_User]],
        regular_user: _User,
        cluster_name: str,
    ) -> None:
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]

        url = api.generate_job_url(job_id)
        another_user = await regular_user_factory()
        async with client.delete(url, headers=another_user.headers) as response:
            assert response.status == HTTPForbidden.status_code, await response.text()
            result = await response.json()
            assert result == {
                "missing": [
                    {
                        "action": "write",
                        "uri": f"job://{cluster_name}/{regular_user.name}/{job_id}",
                    }
                ]
            }

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
            assert response.status == HTTPAccepted.status_code, await response.text()
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
            assert response.status == HTTPBadRequest.status_code, await response.text()
            result = await response.json()
            assert result["error"] == f"no such job {job_id}"

    @pytest.mark.asyncio
    async def test_create_validation_failure(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        request_payload: Dict[str, Any] = {}
        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            assert response.status == HTTPBadRequest.status_code, await response.text()
            response_payload = await response.json()
            assert response_payload == {"error": mock.ANY}
            assert "is required" in response_payload["error"]

    @pytest.mark.asyncio
    async def test_resolve_job_by_name(
        self, job_submit: Dict[str, Any], jobs_client: JobsClient
    ) -> None:
        job_name = f"test-job-name-{random_str()}"
        job_submit["name"] = job_name
        result = await jobs_client.create_job(job_submit)
        job_id = result["id"]
        assert result["name"] == job_name

        result = await jobs_client.get_job_by_id(job_name)
        assert result["id"] == job_id
        assert result["name"] == job_name

        await jobs_client.delete_job(job_name)

    @pytest.mark.asyncio
    async def test_get_job_shared_by_name(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[str]],
        share_job: Callable[[_User, _User, Any], Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_name2 = "test-job-name2"
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        usr3 = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr1)

        job1_id = await run_job(usr2, create_job_request_with_name(job_name))
        job2_id = await run_job(usr2, create_job_request_with_name(job_name2))
        job3_id = await run_job(usr2, create_job_request_no_name())
        job4_id = await run_job(usr3, create_job_request_with_name(job_name))

        # usr2 shares a job with usr1 by name
        await share_job(usr2, usr1, job_name)

        job = await jobs_client_usr1.get_job_by_id(job1_id)
        assert job["id"] == job1_id
        assert job["name"] == job_name
        assert job["owner"] == usr2.name

        for job_id in (job2_id, job3_id, job4_id):
            url = api.generate_job_url(job_id)
            async with client.get(url, headers=usr1.headers) as resp:
                assert resp.status == HTTPForbidden.status_code, await resp.text()

    @pytest.mark.asyncio
    async def test_delete_job_shared_by_name(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        run_job: Callable[..., Awaitable[str]],
        share_job: Callable[..., Awaitable[None]],
        create_job_request_with_name: Callable[[str], Dict[str, Any]],
        create_job_request_no_name: Callable[[], Dict[str, Any]],
    ) -> None:
        job_name = "test-job-name"
        job_name2 = "test-job-name2"
        usr1 = await regular_user_factory()
        usr2 = await regular_user_factory()
        usr3 = await regular_user_factory()
        jobs_client_usr1 = jobs_client_factory(usr1)

        job1_id = await run_job(usr2, create_job_request_with_name(job_name))
        job2_id = await run_job(usr2, create_job_request_with_name(job_name2))
        job3_id = await run_job(usr2, create_job_request_no_name())
        job4_id = await run_job(usr3, create_job_request_with_name(job_name))

        # usr2 shares a job with usr1 by name
        await share_job(usr2, usr1, job_name, action="write")

        await jobs_client_usr1.delete_job(job1_id)

        for job_id in (job2_id, job3_id, job4_id):
            url = api.generate_job_url(job_id)
            async with client.delete(url, headers=usr1.headers) as resp:
                assert resp.status == HTTPForbidden.status_code, await resp.text()

    @pytest.mark.asyncio
    async def test_create_with_custom_volumes(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        cluster_name: str,
    ) -> None:
        request_payload = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "volumes": [
                    {
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}",
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
                "cluster_name": "test-cluster",
                "internal_hostname": f"{job_id}.platformapi-tests",
                "status": "pending",
                "history": {
                    "status": "pending",
                    "reason": "Creating",
                    "description": None,
                    "created_at": mock.ANY,
                    "run_time_seconds": 0,
                    "restarts": 0,
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
                            "src_storage_uri": f"storage://{cluster_name}/"
                            f"{regular_user.name}",
                        }
                    ],
                },
                "is_preemptible": True,
                "is_preemptible_node_required": False,
                "pass_config": False,
                "uri": f"job://test-cluster/{regular_user.name}/{job_id}",
                "restart_policy": "never",
            }

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="succeeded"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "cluster_name": "test-cluster",
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
                "run_time_seconds": mock.ANY,
                "restarts": 0,
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
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}",
                    }
                ],
            },
            "is_preemptible": True,
            "is_preemptible_node_required": False,
            "pass_config": False,
            "uri": f"job://test-cluster/{regular_user.name}/{job_id}",
            "restart_policy": "never",
        }

    @pytest.mark.asyncio
    async def test_job_failed(
        self,
        jobs_client: JobsClient,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        cluster_name: str,
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
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}",
                    },
                    {
                        "dst_path": f"/var/storage/{regular_user.name}/result",
                        "read_only": False,
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}/result",
                    },
                ],
            }
        }
        url = api.jobs_base_url
        async with client.post(
            url, headers=regular_user.headers, json=payload
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            assert result["status"] == "pending"
            job_id = result["id"]

        response_payload = await jobs_client.long_polling_by_job_id(
            job_id=job_id, status="failed"
        )

        assert response_payload == {
            "id": job_id,
            "owner": regular_user.name,
            "cluster_name": "test-cluster",
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
                "run_time_seconds": mock.ANY,
                "restarts": 0,
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
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}",
                    },
                    {
                        "dst_path": f"/var/storage/{regular_user.name}/result",
                        "read_only": False,
                        "src_storage_uri": f"storage://{cluster_name}/"
                        f"{regular_user.name}/result",
                    },
                ],
            },
            "is_preemptible": False,
            "is_preemptible_node_required": False,
            "pass_config": False,
            "uri": f"job://test-cluster/{regular_user.name}/{job_id}",
            "restart_policy": "never",
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
                "cluster_name": "test-cluster",
                "internal_hostname": f"{job_id}.platformapi-tests",
                "status": "pending",
                "history": {
                    "status": "pending",
                    "reason": "Creating",
                    "description": None,
                    "created_at": mock.ANY,
                    "run_time_seconds": 0,
                    "restarts": 0,
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
                "is_preemptible_node_required": False,
                "pass_config": False,
                "uri": f"job://test-cluster/{regular_user.name}/{job_id}",
                "restart_policy": "never",
            }

    @pytest.mark.asyncio
    async def test_create_unknown_tpu_model(
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
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "tpu": {"type": "unknown", "software_version": "unknown"},
                },
            }
        }

        async with client.post(
            api.jobs_base_url, headers=regular_user.headers, json=request_payload
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPBadRequest.status_code, response_text
            data = await response.json()
            assert """'type': DataError(value doesn't match""" in data["error"]

    @pytest.mark.asyncio
    async def test_create_tpu_model(
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
                "resources": {
                    "cpu": 0.1,
                    "memory_mb": 16,
                    "tpu": {"type": "v2-8", "software_version": "1.14"},
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
                "cluster_name": "test-cluster",
                "internal_hostname": f"{job_id}.platformapi-tests",
                "status": "pending",
                "history": {
                    "status": "pending",
                    "reason": "Creating",
                    "description": None,
                    "created_at": mock.ANY,
                    "run_time_seconds": 0,
                    "restarts": 0,
                },
                "container": {
                    "command": "true",
                    "env": {},
                    "image": "ubuntu",
                    "resources": {
                        "cpu": 0.1,
                        "memory_mb": 16,
                        "tpu": {"type": "v2-8", "software_version": "1.14"},
                    },
                    "volumes": [],
                },
                "is_preemptible": False,
                "is_preemptible_node_required": False,
                "pass_config": False,
                "uri": f"job://test-cluster/{regular_user.name}/{job_id}",
                "restart_policy": "never",
            }


class TestStats:
    @pytest.mark.asyncio
    async def test_user_stats_unauthorized(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.stats_for_user_url(regular_user.name)
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_user_stats_for_another_user(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.stats_for_user_url("admin")
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPForbidden.status_code

    @pytest.mark.asyncio
    async def test_user_stats_authorized(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.stats_for_user_url(regular_user.name)
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {
                "name": regular_user.name,
                "jobs": {
                    "total_gpu_run_time_minutes": 0,
                    "total_non_gpu_run_time_minutes": 0,
                },
                "quota": {},
                "clusters": [
                    {
                        "name": "test-cluster",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {},
                    }
                ],
            }

    @pytest.mark.asyncio
    async def test_user_stats_authorized_request_for_non_existing_user(
        self, api: ApiConfig, client: aiohttp.ClientSession, admin_token: str
    ) -> None:
        url = api.stats_for_user_url("non-existing")
        admin_user = _User(name="admin", token=admin_token)
        async with client.get(url, headers=admin_user.headers) as resp:
            assert resp.status == HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_user_stats_admin(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        admin_token: str,
    ) -> None:
        url = api.stats_for_user_url(regular_user.name)
        admin_user = _User(name="admin", token=admin_token)
        async with client.get(url, headers=admin_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {
                "name": regular_user.name,
                "jobs": {
                    "total_gpu_run_time_minutes": 0,
                    "total_non_gpu_run_time_minutes": 0,
                },
                "quota": {},
                "clusters": [
                    {
                        "name": "test-cluster",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {},
                    }
                ],
            }

    @pytest.mark.asyncio
    async def test_user_stats_quota(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[..., Awaitable[_User]],
    ) -> None:
        user = await regular_user_factory(
            auth_clusters=[
                AuthCluster(
                    name="test-cluster",
                    quota=Quota(
                        total_gpu_run_time_minutes=123,
                        total_non_gpu_run_time_minutes=321,
                    ),
                ),
                AuthCluster(name="testcluster2"),
            ]
        )
        url = api.stats_for_user_url(user.name)
        async with client.get(url, headers=user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {
                "name": user.name,
                "jobs": {
                    "total_gpu_run_time_minutes": 0,
                    "total_non_gpu_run_time_minutes": 0,
                },
                "quota": {
                    "total_gpu_run_time_minutes": 123,
                    "total_non_gpu_run_time_minutes": 321,
                },
                "clusters": [
                    {
                        "name": "test-cluster",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {
                            "total_gpu_run_time_minutes": 123,
                            "total_non_gpu_run_time_minutes": 321,
                        },
                    },
                    {
                        "name": "testcluster2",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {},
                    },
                ],
            }

    @pytest.mark.asyncio
    async def test_user_stats_unavailable_clusters(
        self,
        api: ApiConfig,
        auth_api: AuthApiConfig,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client: JobsClient,
        admin_token: str,
        regular_user: _User,
    ) -> None:
        admin_user = _User(name="admin", token=admin_token)
        user = regular_user

        async with client.post(
            api.jobs_base_url, headers=user.headers, json=job_submit
        ) as response:
            assert response.status == HTTPAccepted.status_code, await response.text()
            result = await response.json()
            job_id = result["id"]
            await jobs_client.long_polling_by_job_id(job_id=job_id, status="succeeded")

        url = auth_api.auth_for_user_url(user.name)
        payload = {"name": user.name, "cluster_name": "testcluster2"}
        async with client.put(url, headers=admin_user.headers, json=payload) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

        url = api.stats_for_user_url(user.name)
        async with client.get(url, headers=user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {
                "name": user.name,
                "jobs": {
                    "total_gpu_run_time_minutes": 0,
                    "total_non_gpu_run_time_minutes": 0,
                },
                "quota": {},
                "clusters": [
                    {
                        "name": "testcluster2",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {},
                    },
                    {
                        "name": "test-cluster",
                        "jobs": {
                            "total_gpu_run_time_minutes": mock.ANY,
                            "total_non_gpu_run_time_minutes": mock.ANY,
                        },
                        "quota": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                    },
                ],
            }


class TestTags:
    @pytest.mark.asyncio
    async def test_user_tags_unauthorized(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.tags_base_url
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    @pytest.mark.asyncio
    async def test_user_tags_authorized_empty(
        self, api: ApiConfig, client: aiohttp.ClientSession, regular_user: _User
    ) -> None:
        url = api.tags_base_url
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {"tags": []}

    @pytest.mark.asyncio
    async def test_user_tags_authorized(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user: _User,
        job_submit: Dict[str, Any],
    ) -> None:
        headers = regular_user.headers

        job_submit["tags"] = ["t1"]
        url = api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()

        job_submit["tags"] = ["t4", "t3", "t2"]
        url = api.jobs_base_url
        async with client.post(url, headers=headers, json=job_submit) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()

        url = api.tags_base_url
        async with client.get(url, headers=regular_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {"tags": ["t2", "t3", "t4", "t1"]}


class TestJobPolicyEnforcer:
    @pytest.mark.parametrize("has_gpu", [False, True])
    @pytest.mark.asyncio
    async def test_enforce_quota(
        self,
        api: ApiConfig,
        auth_api: AuthApiConfig,
        config: Config,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_with_custom_quota: _User,
        admin_token: str,
        has_gpu: bool,
    ) -> None:
        quota_type = (
            "total_gpu_run_time_minutes"
            if has_gpu
            else "total_non_gpu_run_time_minutes"
        )

        admin_user = _User(name="admin", token=admin_token)
        user = regular_user_with_custom_quota
        user_jobs_client = jobs_client_factory(user)

        url = api.stats_for_user_url(user.name)
        async with client.get(url, headers=admin_user.headers) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            result = await resp.json()
            assert result == {
                "name": user.name,
                "jobs": {
                    "total_gpu_run_time_minutes": 0,
                    "total_non_gpu_run_time_minutes": 0,
                },
                "quota": {
                    "total_gpu_run_time_minutes": 123,
                    "total_non_gpu_run_time_minutes": 321,
                },
                "clusters": [
                    {
                        "name": "test-cluster",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {
                            "total_gpu_run_time_minutes": 123,
                            "total_non_gpu_run_time_minutes": 321,
                        },
                    },
                    {
                        "name": "testcluster2",
                        "jobs": {
                            "total_gpu_run_time_minutes": 0,
                            "total_non_gpu_run_time_minutes": 0,
                        },
                        "quota": {},
                    },
                ],
            }

        job_submit["container"]["command"] = "sleep 1h"
        if has_gpu:
            job_submit["container"]["resources"]["gpu"] = 1
            job_submit["container"]["resources"]["gpu_model"] = "gpumodel"

        job_default = await user_jobs_client.create_job(job_submit)
        poll_status = "pending" if has_gpu else "running"
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_default["id"], status=poll_status
        )

        job_submit["cluster_name"] = "testcluster2"
        job_cluster2 = await user_jobs_client.create_job(job_submit)
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_cluster2["id"], status=poll_status
        )

        payload = {
            "name": user.name,
            "clusters": [
                {"name": "test-cluster", "quota": {quota_type: 0}},
                {"name": "testcluster2", "quota": {}},
            ],
        }
        url = auth_api.auth_for_user_url(user.name)
        async with client.put(url, headers=admin_user.headers, json=payload) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

        # Due to conflict between quota enforcer and jobs poller (see issue #986),
        # we cannot guarrantee that the quota will be enforced up to one
        # enforce-poller's interval, so we check up to 7 intervals:
        max_enforcing_time = config.job_policy_enforcer.interval_sec * 7
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_default["id"],
            interval_s=0.1,
            status="cancelled",
            max_time=max_enforcing_time,
        )

        await user_jobs_client.long_polling_by_job_id(
            job_id=job_cluster2["id"], status=poll_status
        )

        await user_jobs_client.delete_job(job_cluster2["id"], assert_success=False)


class TestRuntimeLimitEnforcer:
    @pytest.mark.asyncio
    async def test_enforce_runtime(
        self,
        api: ApiConfig,
        auth_api: AuthApiConfig,
        config: Config,
        client: aiohttp.ClientSession,
        job_submit: Dict[str, Any],
        jobs_client_factory: Callable[[_User], JobsClient],
        regular_user_with_custom_quota: _User,
    ) -> None:
        user = regular_user_with_custom_quota
        user_jobs_client = jobs_client_factory(user)

        job_submit["container"]["command"] = "sleep 1h"

        job_submit["max_run_time_minutes"] = 1
        job_default = await user_jobs_client.create_job(job_submit)
        assert job_default["max_run_time_minutes"] == 1
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_default["id"],
            status="running",
        )
        # Due to conflict between quota enforcer and jobs poller (see issue #986),
        # we cannot guarrantee that the quota will be enforced up to one
        # enforce-poller's interval, so we check up to 7 intervals:
        max_enforcing_time = 60 + config.job_policy_enforcer.interval_sec * 7
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_default["id"],
            interval_s=0.1,
            status="cancelled",
            max_time=max_enforcing_time,
        )

        # Explicit very big timeout
        job_submit["max_run_time_minutes"] = 5 * 60
        job_2 = await user_jobs_client.create_job(job_submit)
        assert job_2["max_run_time_minutes"] == 5 * 60
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_2["id"], status="running"
        )

        # Implicitly disabled timeout
        job_submit.pop("max_run_time_minutes", None)
        job_3 = await user_jobs_client.create_job(job_submit)
        assert "max_run_time_minutes" not in job_3
        await user_jobs_client.long_polling_by_job_id(
            job_id=job_3["id"], status="running"
        )

        job_2_status = await user_jobs_client.get_job_by_id(job_2["id"])
        assert job_2_status["status"] == "running"

        job_3_status = await user_jobs_client.get_job_by_id(job_3["id"])
        assert job_3_status["status"] == "running"

        await user_jobs_client.delete_job(job_2["id"], assert_success=False)
        await user_jobs_client.delete_job(job_3["id"], assert_success=False)
