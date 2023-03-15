import asyncio
import base64
import subprocess
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Optional

import aiodocker
import aiodocker.containers
import aiohttp
import pytest
from aiohttp import ClientError
from async_timeout import timeout
from yarl import URL

from platform_api.config import AuthConfig
from platform_api.orchestrator.kube_config import KubeConfig

from tests.integration.auth import _User


@pytest.fixture(scope="session")
def secrets_server_image_name() -> str:
    with open("PLATFORMSECRETS_IMAGE") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
async def docker_host(docker: aiodocker.Docker) -> str:
    if sys.platform.startswith("linux"):
        bridge = await docker.networks.get("bridge")
        bridge_config = await bridge.show()
        return bridge_config["IPAM"]["Config"][0]["Gateway"]
    return "host.docker.internal"


@pytest.fixture(scope="session")
async def kube_proxy_url(docker_host: str) -> AsyncIterator[str]:
    cmd = "kubectl proxy -p 8084 --address='0.0.0.0' --accept-hosts='.*'"
    proc = subprocess.Popen(
        cmd,
        shell=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        close_fds=True,
    )
    try:
        prefix = "Starting to serve on "
        assert proc.stdout, proc
        line = proc.stdout.readline().decode().strip()
        err = f"Error while running command `{cmd}`: output `{line}`"
        if "error" in line.lower():
            raise RuntimeError(f"{err}: Error detected")
        if not line.startswith(prefix):
            raise RuntimeError(f"{err}: Unexpected output")
        try:
            value = line[len(prefix) :]
            _, port_str = value.rsplit(":", 1)
            port = int(port_str)
        except ValueError as e:
            raise RuntimeError(f"{err}: Could not parse `{line}`: {e}")

        yield f"http://{docker_host}:{port}"

    finally:
        proc.terminate()
        proc.wait()


@pytest.fixture
async def secrets_server_url(
    docker: aiodocker.Docker,
    reuse_docker: bool,
    auth_config: AuthConfig,
    kube_config: KubeConfig,
    kube_proxy_url: str,
    secrets_server_image_name: str,
    test_cluster_name: str,
) -> AsyncIterator[URL]:
    image_name = secrets_server_image_name
    container_name = "secrets_server"
    auth_server_container_name = "auth_server"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {
            "PublishAllPorts": True,
            "Links": [f"{auth_server_container_name}:auth_server"],
        },
        "Env": [
            f"NP_SECRETS_PLATFORM_AUTH_URL=http://{auth_server_container_name}:8080",
            f"NP_SECRETS_PLATFORM_AUTH_TOKEN={auth_config.service_token}",
            f"NP_SECRETS_K8S_API_URL={kube_proxy_url}",
            f"NP_SECRETS_K8S_NS={kube_config.namespace}",
            "NP_SECRETS_K8S_AUTH_TYPE=none",
            f"NP_CLUSTER_NAME={test_cluster_name}",
        ],
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                url = await create_secrets_url(container)
                await wait_for_secrets_server(url)
                yield url
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    url = await create_secrets_url(container)
    await wait_for_secrets_server(url)
    yield url

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


async def create_secrets_url(container: aiodocker.containers.DockerContainer) -> URL:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    return url


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

    async def __aexit__(self, *args: Any) -> None:
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
        org_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> None:
        url = self._base_url / "secrets"
        payload = {
            "key": key,
            "value": self._base64_encode(value),
            "org_name": org_name,
        }
        if project_name:
            payload["project_name"] = project_name
        expected_project_name = project_name or self._user_name
        async with self._client.post(url, json=payload) as resp:
            assert resp.status == 201, await resp.text()
            data = await resp.json()
            assert data == {
                "key": key,
                "org_name": org_name,
                "owner": self._user_name,
                "project_name": expected_project_name,
            }


@asynccontextmanager
async def create_secrets_client(
    url: URL, user_name: str = "", user_token: str = ""
) -> AsyncIterator[SecretsClient]:
    async with SecretsClient(url, user_name, user_token) as client:
        yield client


@pytest.fixture
async def secrets_client_factory(
    secrets_server_url: URL,
) -> Callable[[_User], AbstractAsyncContextManager[SecretsClient]]:
    def _f(user: _User) -> AbstractAsyncContextManager[SecretsClient]:
        return create_secrets_client(secrets_server_url, user.name, user.token)

    return _f


@pytest.fixture
async def regular_secrets_client(
    secrets_client_factory: Callable[[_User], SecretsClient], regular_user: _User
) -> AsyncIterator[SecretsClient]:
    async with secrets_client_factory(regular_user) as client:
        yield client


async def wait_for_secrets_server(
    url: URL, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with create_secrets_client(url) as client:
                    await client.ping()
                    break
            except asyncio.CancelledError:
                raise
            except (AssertionError, ClientError):
                pass
            await asyncio.sleep(interval_s)
