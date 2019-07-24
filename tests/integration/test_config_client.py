from typing import Any, AsyncIterator, Dict, List

import aiohttp
import pytest
from async_generator import asynccontextmanager
from yarl import URL

from platform_api.config_client import ConfigClient

from .conftest import ApiRunner


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
                    "url": "https://1.2.3.4:8443",
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
                    {},
                    {"gpu": 0},
                    {"gpu": 1, "gpu_model": "nvidia-tesla-v100"},
                ],
                "is_http_ingress_secure": True,
            },
            "ssh": {"server": "ssh-auth-dev.neu.ro"},
            "monitoring": {
                "url": "https://dev.neu.ro/api/v1/jobs",
                "elasticsearch": {
                    "hosts": ["http://logging-elasticsearch:9200"],
                    "username": "es_user_name",
                    "password": "es_assword",
                },
            },
        }
    ]


async def create_config_app(payload: List[Dict[str, Any]]) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    async def handle(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(payload)

    app.add_routes((aiohttp.web.get("/api/v1/clusters", handle),))

    return app


@asynccontextmanager
async def create_config_api(
    cluster_configs_payload: List[Dict[str, Any]]
) -> AsyncIterator[URL]:
    app = await create_config_app(cluster_configs_payload)
    runner = ApiRunner(app, port=8082)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()


class TestConfigClient:
    @pytest.mark.asyncio
    async def test_valid_cluster_configs(
        self, cluster_configs_payload: List[Dict[str, Any]]
    ) -> None:
        users_url = URL("https://neu.ro/api/v1/users")
        async with create_config_api(cluster_configs_payload) as url:
            async with ConfigClient(base_url=url) as client:
                result = await client.get_clusters(users_url=users_url)

                assert len(result) == 1

    @pytest.mark.asyncio
    async def test_client_skips_invalid_cluster_configs(
        self, cluster_configs_payload: List[Dict[str, Any]]
    ) -> None:
        cluster_configs_payload.append({})
        users_url = URL("https://neu.ro/api/v1/users")
        async with create_config_api(cluster_configs_payload) as url:
            async with ConfigClient(base_url=url) as client:
                result = await client.get_clusters(users_url=users_url)

                assert len(result) == 1
