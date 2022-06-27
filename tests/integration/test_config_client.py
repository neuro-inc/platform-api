from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest
from yarl import URL

from platform_api.config_client import ConfigClient

from .conftest import ApiRunner


@pytest.fixture
def cluster_configs_payload() -> list[dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
            "storage": {
                "url": "https://dev.neu.ro/api/v1/storage",
            },
            "registry": {
                "url": "https://registry-dev.neu.ro",
            },
            "orchestrator": {
                "job_hostname_template": "{job_id}.jobs.neu.ro",
                "job_internal_hostname_template": "{job_id}.platformapi-tests",
                "resource_pool_types": [
                    {"name": "node-pool1"},
                    {"name": "node-pool2", "gpu": 0},
                    {"name": "node-pool3", "gpu": 1, "gpu_model": "nvidia-tesla-v100"},
                ],
                "resource_presets": [
                    {
                        "name": "cpu-micro",
                        "credits_per_hour": "10",
                        "cpu": 0.1,
                        "memory": 100,
                    }
                ],
                "is_http_ingress_secure": True,
            },
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
            "secrets": {"url": "https://dev.neu.ro/api/v1/secrets"},
            "metrics": {"url": "https://metrics.dev.neu.ro"},
            "disks": {"url": "https://dev.neu.ro/api/v1/disk"},
            "buckets": {"url": "https://dev.neu.ro/api/v1/buckets"},
            "blob_storage": {"url": "https://dev.neu.ro/api/v1/blob"},
        }
    ]


async def create_config_app(payload: list[dict[str, Any]]) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    async def handle(request: aiohttp.web.Request) -> aiohttp.web.Response:
        assert request.query["include"] == "config"
        return aiohttp.web.json_response(payload)

    app.add_routes((aiohttp.web.get("/api/v1/clusters", handle),))

    return app


@asynccontextmanager
async def create_config_api(
    cluster_configs_payload: list[dict[str, Any]]
) -> AsyncIterator[URL]:
    app = await create_config_app(cluster_configs_payload)
    runner = ApiRunner(app, port=8082)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()


class TestConfigClient:
    async def test_valid_cluster_configs(
        self, cluster_configs_payload: list[dict[str, Any]]
    ) -> None:
        async with create_config_api(cluster_configs_payload) as url:
            async with ConfigClient(base_url=url) as client:
                result = await client.get_clusters()

                assert len(result) == 1

    async def test_client_skips_invalid_cluster_configs(
        self, cluster_configs_payload: list[dict[str, Any]]
    ) -> None:
        cluster_configs_payload.append({})
        async with create_config_api(cluster_configs_payload) as url:
            async with ConfigClient(base_url=url) as client:
                result = await client.get_clusters()

                assert len(result) == 1
