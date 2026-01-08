from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

import aiohttp
import pytest
from yarl import URL

from platform_api.orchestrator.job_request import Disk
from tests.integration.auth import _User


class DiskAPIClient:
    def __init__(self, cluster_name: str, url: URL, auth_token: str):
        self._cluster_name = cluster_name
        self._base_url = url / "api/v1"
        headers: dict[str, str] = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        self._client = aiohttp.ClientSession(headers=headers)

    async def __aenter__(self) -> "DiskAPIClient":
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

    async def create_disk(self, storage: int, project_name: str, org_name: str) -> Disk:
        url = self._base_url / "disk"
        payload = {
            "storage": storage,
            "project_name": project_name,
            "org_name": org_name,
        }

        async with self._client.post(url, json=payload) as resp:
            assert resp.status == 201, await resp.text()
            data = await resp.json()
            disk_path = f"{data['org_name']}/{data['project_name']}"
            return Disk(
                disk_id=data["id"],
                path=disk_path,
                cluster_name=self._cluster_name,
            )


@asynccontextmanager
async def create_disk_api_client(
    cluster_name: str, url: URL, auth_token: str = ""
) -> AsyncIterator[DiskAPIClient]:
    async with DiskAPIClient(cluster_name, url, auth_token) as client:
        yield client


@pytest.fixture
async def disk_client_factory(
    k8s_disk_server_url: URL,
) -> Callable[[_User], AbstractAsyncContextManager[DiskAPIClient]]:
    def _f(user: _User) -> AbstractAsyncContextManager[DiskAPIClient]:
        return create_disk_api_client(
            user.cluster_name, k8s_disk_server_url, user.token
        )

    return _f
