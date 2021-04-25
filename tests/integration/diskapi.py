import asyncio
import subprocess
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

import aiodocker
import aiodocker.containers
import aiohttp
import pytest
from aiohttp import ClientError
from async_timeout import timeout
from yarl import URL

from platform_api.config import AuthConfig
from platform_api.orchestrator.job_request import Disk
from platform_api.orchestrator.kube_config import KubeConfig
from tests.integration.auth import _User


@pytest.fixture(scope="session")
def disk_api_server_image_name() -> str:
    with open("PLATFORMDISKAPI_IMAGE") as f:
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
    cmd = "kubectl proxy -p 8086 --address='0.0.0.0' --accept-hosts='.*'"
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
async def disk_server_url(
    docker: aiodocker.Docker,
    reuse_docker: bool,
    auth_config: AuthConfig,
    kube_config: KubeConfig,
    kube_proxy_url: str,
    disk_api_server_image_name: str,
    test_cluster_name: str,
) -> AsyncIterator[URL]:
    image_name = disk_api_server_image_name
    container_name = "diskapi_server"
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
            f"NP_DISK_API_PLATFORM_AUTH_URL=http://{auth_server_container_name}:8080",
            f"NP_DISK_API_PLATFORM_AUTH_TOKEN={auth_config.service_token}",
            f"NP_DISK_API_K8S_API_URL={kube_proxy_url}",
            f"NP_DISK_API_K8S_NS={kube_config.namespace}",
            "NP_DISK_API_K8S_AUTH_TYPE=none",
            # From `tests/k8s/storageclass.yml`:
            "NP_DISK_API_K8S_STORAGE_CLASS=test-storage-class",
            "NP_DISK_API_STORAGE_LIMIT_PER_USER=104857600",  # 100mb
            f"NP_CLUSTER_NAME={test_cluster_name}",
        ],
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                url = await create_disk_api_url(container)
                await wait_for_disk_api_server(url)
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

    url = await create_disk_api_url(container)
    await wait_for_disk_api_server(url)
    yield url

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


async def create_disk_api_url(container: aiodocker.containers.DockerContainer) -> URL:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    return url


class DiskAPIClient:
    def __init__(self, cluster_name: str, url: URL, auth_token: str):
        self._cluster_name = cluster_name
        self._base_url = url / "api/v1"
        headers = {"Authorization": f"Bearer {auth_token}"}
        self._client = aiohttp.ClientSession(headers=headers)

    async def __aenter__(self) -> "DiskAPIClient":
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

    async def create_disk(self, storage: int) -> Disk:
        url = self._base_url / "disk"
        payload = {"storage": storage}
        async with self._client.post(url, json=payload) as resp:
            assert resp.status == 201, await resp.text()
            data = await resp.json()
            return Disk(
                disk_id=data["id"],
                user_name=data["owner"],
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
    disk_server_url: URL,
) -> Callable[[_User], AsyncIterator[DiskAPIClient]]:
    def _f(user: _User) -> AsyncIterator[DiskAPIClient]:
        return create_disk_api_client(user.cluster_name, disk_server_url, user.token)

    return _f


@pytest.fixture
async def regular_disk_api_client(
    disk_client_factory: Callable[[_User], DiskAPIClient], regular_user: _User
) -> AsyncIterator[DiskAPIClient]:
    async with disk_client_factory(regular_user) as client:
        yield client


async def wait_for_disk_api_server(
    url: URL, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with create_disk_api_client(cluster_name="", url=url) as client:
                    await client.ping()
                    break
            except asyncio.CancelledError:
                raise
            except (AssertionError, ClientError):
                pass
            await asyncio.sleep(interval_s)
