from typing import Any, AsyncIterator, Sequence

import aiohttp
from async_generator import asynccontextmanager
from yarl import URL

from .cluster_config_factory import ClusterConfig, ClusterConfigFactory


class ConfigClient:
    def __init__(
        self,
        *,
        base_url: URL,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
    ):
        self._base_url = base_url
        connector = aiohttp.TCPConnector(limit=conn_pool_size)
        timeout = aiohttp.ClientTimeout(connect=conn_timeout_s, total=read_timeout_s)
        self._client = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def __aenter__(self) -> "ConfigClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.close()
        del self._client

    @asynccontextmanager
    async def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        url = self._base_url / path
        async with self._client.request(method, url, **kwargs) as response:
            response.raise_for_status()
            yield response

    async def get_clusters(
        self, *, users_url: URL
    ) -> Sequence[ClusterConfig]:
        async with self._request("GET", "clusters") as response:
            payload = await response.json()
            return ClusterConfigFactory().create_cluster_configs(
                payload, users_url=users_url
            )
