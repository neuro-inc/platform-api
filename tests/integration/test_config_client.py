from typing import Any, AsyncIterator, Dict, Sequence

import aiohttp
import pytest
from yarl import URL

from platform_api.cluster_config import ClusterConfig
from platform_api.config import Config
from platform_api.config_client import ConfigClient

from .conftest import ApiRunner


async def create_config_app(
    payload: Sequence[Dict[str, Any]]
) -> aiohttp.web.Application:
    app = aiohttp.web.Application()

    async def handle(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(payload)

    app.add_routes((aiohttp.web.get("/api/v1/clusters", handle),))

    return app


@pytest.fixture
async def config_api_url(
    cluster_configs_payload: Sequence[Dict[str, Any]]
) -> AsyncIterator[URL]:
    app = await create_config_app(cluster_configs_payload)
    runner = ApiRunner(app, port=8082)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()


@pytest.fixture
def cluster_configs_payload() -> Sequence[Dict[str, Any]]:
    return [{}]


@pytest.fixture
def users_url() -> URL:
    return URL("https://dev.neu.ro/api/v1/users")


class _ConfigClient(ConfigClient):
    def __init__(
        self,
        base_url: URL,
        *,
        expected_payload: Sequence[Dict[str, Any]],
        expected_users_url: URL,
        expected_cluster_configs: Sequence[ClusterConfig],
    ):
        super().__init__(base_url=base_url)

        self._expected_payload = expected_payload
        self._expected_users_url = expected_users_url
        self._expected_cluster_configs = expected_cluster_configs

    def create_cluster_configs(
        self, payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> Sequence[ClusterConfig]:
        assert payload == self._expected_payload
        assert users_url == self._expected_users_url

        return self._expected_cluster_configs


class TestConfigClient:
    @pytest.mark.asyncio
    async def test_valid_cluster_config(
        self,
        config_api_url: URL,
        cluster_configs_payload: Sequence[Dict[str, Any]],
        users_url: URL,
        config: Config,
    ) -> None:
        cluster_configs = [config.cluster]
        async with _ConfigClient(
            base_url=config_api_url,
            expected_payload=cluster_configs_payload,
            expected_users_url=users_url,
            expected_cluster_configs=cluster_configs,
        ) as client:
            result = await client.get_clusters(users_url)

            assert cluster_configs == result
