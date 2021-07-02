from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, AsyncIterator, Optional, Sequence

import aiohttp
from multidict import CIMultiDict
from yarl import URL


class AdminClient:
    def __init__(
        self,
        *,
        base_url: URL,
        service_token: Optional[str] = None,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
        trace_configs: Sequence[aiohttp.TraceConfig] = (),
    ):
        self._base_url = base_url
        self._service_token = service_token
        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._trace_configs = trace_configs
        self._client: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "AdminClient":
        self._init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._client:
            return
        await self._client.close()
        del self._client

    def _init(self) -> None:
        if self._client:
            return
        connector = aiohttp.TCPConnector(limit=self._conn_pool_size)
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            headers=self._generate_headers(self._service_token),
            connector=connector,
            timeout=timeout,
            trace_configs=list(self._trace_configs),
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
        assert self._client
        url = self._base_url / path
        async with self._client.request(method, url, **kwargs) as response:
            response.raise_for_status()
            yield response

    async def change_user_credits(
        self,
        cluster_name: str,
        username: str,
        credits_delta: Decimal,
        idempotency_key: str,
    ) -> None:
        payload = {"additional_quota": {"credits": str(credits_delta)}}
        async with self._request(
            "PATCH",
            f"clusters/{cluster_name}/users/{username}/quota",
            json=payload,
            params={"idempotency_key": idempotency_key},
        ) as response:
            response.raise_for_status()

    async def add_debt(
        self,
        cluster_name: str,
        username: str,
        credits: Decimal,
        idempotency_key: str,
    ) -> None:
        payload = {"user_name": username, "credits": str(credits)}
        async with self._request(
            "POST",
            f"clusters/{cluster_name}/debts",
            json=payload,
            params={"idempotency_key": idempotency_key},
        ) as response:
            response.raise_for_status()
