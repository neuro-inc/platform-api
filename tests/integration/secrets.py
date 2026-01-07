import base64
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import aiohttp
import pytest
from yarl import URL

from tests.integration.auth import _User


class SecretsClient:
    def __init__(self, url: URL, user_name: str, user_token: str):
        self._base_url = url / "api/v1"
        self._user_name = user_name
        headers: dict[str, str] = {}
        if user_token:
            headers["Authorization"] = f"Bearer {user_token}"
        self._client = aiohttp.ClientSession(headers=headers)

    async def __aenter__(self) -> "SecretsClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.close()

    async def ping(self) -> None:
        url = self._base_url / "ping"
        async with self._client.get(url) as resp:
            txt = await resp.text()
            assert txt == "Pong"

    def _base64_encode(self, value: str) -> str:
        return base64.b64encode(value.encode()).decode()

    async def create_secret(
        self,
        key: str,
        value: str,
        project_name: str,
        org_name: str,
    ) -> None:
        url = self._base_url / "secrets"
        payload = {
            "key": key,
            "value": self._base64_encode(value),
            "project_name": project_name,
            "org_name": org_name,
        }
        async with self._client.post(url, json=payload) as resp:
            assert resp.status == 201, await resp.text()
            data = await resp.json()
            assert data == {
                "key": key,
                "org_name": org_name,
                "owner": project_name,
                "project_name": project_name,
            }


@asynccontextmanager
async def create_secrets_client(
    url: URL, user_name: str = "", user_token: str = ""
) -> AsyncIterator[SecretsClient]:
    async with SecretsClient(url, user_name, user_token) as client:
        yield client


@pytest.fixture
async def secrets_client_factory(
    k8s_secrets_server_url: URL,
) -> Callable[[_User], AbstractAsyncContextManager[SecretsClient]]:
    def _f(user: _User) -> AbstractAsyncContextManager[SecretsClient]:
        return create_secrets_client(k8s_secrets_server_url, user.name, user.token)

    return _f
