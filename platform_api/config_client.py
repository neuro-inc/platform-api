from typing import Any, AsyncIterator, Optional, Sequence

import aiohttp
from async_generator import asynccontextmanager
from multidict import CIMultiDict
from yarl import URL

from .cluster_config import ClusterConfig
from .cluster_config_factory import ClusterConfigFactory
from .config import GarbageCollectorConfig


class ConfigClient:
    def __init__(
        self,
        *,
        base_url: URL,
        service_token: Optional[str] = None,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
    ):
        self._base_url = base_url
        self._service_token = service_token
        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size

    async def __aenter__(self) -> "ConfigClient":
        self._init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.close()
        del self._client

    def _init(self) -> None:
        connector = aiohttp.TCPConnector(limit=self._conn_pool_size)
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            headers=self._generate_headers(self._service_token),
            connector=connector,
            timeout=timeout,
        )

    def _generate_headers(self, token: Optional[str] = None) -> "CIMultiDict[str]":
        headers: "CIMultiDict[str]" = CIMultiDict()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @asynccontextmanager
    async def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        url = self._base_url / path
        async with self._client.request(method, url, **kwargs) as response:
            response.raise_for_status()
            yield response

    async def get_clusters(
        self,
        *,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
        registry_username: str,
        registry_password: str,
        garbage_collector: GarbageCollectorConfig,
    ) -> Sequence[ClusterConfig]:
        async with self._request("GET", "clusters") as response:
            payload = await response.json()
            return ClusterConfigFactory().create_cluster_configs(
                payload,
                jobs_ingress_class=jobs_ingress_class,
                jobs_ingress_oauth_url=jobs_ingress_oauth_url,
                registry_username=registry_username,
                registry_password=registry_password,
                garbage_collector=garbage_collector,
            )
