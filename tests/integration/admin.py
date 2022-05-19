import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import aiodocker
import aiohttp.web
import pytest
from aiohttp import ClientError, ClientResponseError
from aiohttp.web_exceptions import HTTPCreated, HTTPNoContent, HTTPNotFound
from async_timeout import timeout
from neuro_admin_client import AdminClient
from yarl import URL

from platform_api.config import AuthConfig

from tests.integration.conftest import ApiRunner


@pytest.fixture(scope="session")
async def fake_config_app() -> AsyncIterator[URL]:
    app = aiohttp.web.Application()
    clusters: list[Any] = []

    async def add_cluster(request: aiohttp.web.Request) -> aiohttp.web.Response:
        payload = await request.json()
        payload["status"] = "blank"
        payload["created_at"] = datetime.now().isoformat()
        clusters.append(payload)
        return aiohttp.web.json_response(payload)

    async def list_clusters(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(clusters)

    async def get_cluster(request: aiohttp.web.Request) -> aiohttp.web.Response:
        cluster_name = request.match_info["cluster_name"]
        for cluster in clusters:
            if cluster["name"] == cluster_name:
                return aiohttp.web.json_response(
                    cluster, status=HTTPCreated.status_code
                )
        return aiohttp.web.Response(status=HTTPNotFound.status_code)

    async def add_storage(request: aiohttp.web.Request) -> aiohttp.web.Response:
        cluster_name = request.match_info["cluster_name"]
        for cluster in clusters:
            if cluster["name"] == cluster_name:
                return aiohttp.web.json_response(
                    cluster, status=HTTPCreated.status_code
                )
        return aiohttp.web.Response(status=HTTPNotFound.status_code)

    async def delete_storage(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(status=HTTPNoContent.status_code)

    app.add_routes((aiohttp.web.post("/api/v1/clusters", add_cluster),))
    app.add_routes((aiohttp.web.get("/api/v1/clusters", list_clusters),))
    app.add_routes((aiohttp.web.get("/api/v1/clusters/{cluster_name}", get_cluster),))
    app.add_routes(
        (
            aiohttp.web.post(
                "/api/v1/clusters/{cluster_name}/cloud_provider/storages",
                add_storage,
            ),
        )
    )
    app.add_routes(
        (
            aiohttp.web.delete(
                "/api/v1/clusters/{cluster_name}/cloud_provider"
                "/storages/{storage_name}",
                delete_storage,
            ),
        )
    )

    runner = ApiRunner(app, port=8089)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()


@pytest.fixture(scope="session")
def admin_server_image_name() -> str:
    with open("PLATFORMADMIN_IMAGE") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
async def _admin_server_setup_db(
    docker: aiodocker.Docker,
    reuse_docker: bool,
    admin_server_image_name: str,
    auth_server: AuthConfig,
    admin_token: str,
    admin_postgres_dsn: str,
    fake_config_app: URL,
) -> None:
    image_name = admin_server_image_name
    container_name = "admin_migrations"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {
            "PublishAllPorts": True,
            "Links": ["postgres-admin:postgres"],
            "ExtraHosts": [
                "host.docker.internal:host-gateway",
            ],
        },
        "Cmd": ["alembic", "upgrade", "head"],
        "Env": [
            f"NP_ADMIN_AUTH_TOKEN={admin_token}",
            "NP_ADMIN_AUTH_URL=http://auth:8080",
            f"NP_ADMIN_CONFIG_TOKEN={admin_token}",
            "NP_ADMIN_CONFIG_URL=http://host.docker.internal:8089",
            f"NP_ADMIN_POSTGRES_DSN={admin_postgres_dsn}",
        ],
    }

    try:
        await docker.images.inspect(admin_server_image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(admin_server_image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()
    res = await container.wait()
    if res["StatusCode"] != 0:
        raise Exception(
            "Postgres admin DB migrations failed: " + res["Error"]["Message"]
        )


@pytest.fixture(scope="session")
async def admin_server(
    docker: aiodocker.Docker,
    reuse_docker: bool,
    admin_server_image_name: str,
    auth_server: AuthConfig,
    admin_token: str,
    admin_postgres_dsn: str,
    _admin_server_setup_db: None,
) -> AsyncIterator[URL]:
    image_name = admin_server_image_name
    container_name = "admin_server"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {
            "PublishAllPorts": True,
            "Links": ["postgres-admin:postgres", "auth_server:auth"],
            "ExtraHosts": [
                "host.docker.internal:host-gateway",
            ],
        },
        "Env": [
            f"NP_ADMIN_AUTH_TOKEN={admin_token}",
            "NP_ADMIN_AUTH_URL=http://auth:8080",
            f"NP_ADMIN_CONFIG_TOKEN={admin_token}",
            "NP_ADMIN_CONFIG_URL=http://host.docker.internal:8089",
            f"NP_ADMIN_POSTGRES_DSN={admin_postgres_dsn}",
        ],
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                url = await create_admin_url(container)
                await wait_for_admin_server(url, auth_server)
                yield url
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(admin_server_image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(admin_server_image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    url = await create_admin_url(container)
    await wait_for_admin_server(url, auth_server)
    yield url

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


async def create_admin_url(
    container: aiodocker.containers.DockerContainer,
) -> URL:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    return URL(f"http://{host}:{port}/apis/admin/v1")


@pytest.fixture
async def admin_url(admin_server: URL) -> AsyncIterator[URL]:
    yield admin_server


@asynccontextmanager
async def create_admin_client(
    url: URL, config: AuthConfig
) -> AsyncGenerator[AdminClient, None]:
    async with AdminClient(base_url=url, service_token=config.service_token) as client:
        # Make user for compute token so it can be owner of clusters
        try:
            await client.create_user(name="compute", email="compute@admin.com")
        except ClientResponseError:  # Already exists
            pass
        yield client


@pytest.fixture
async def admin_client(
    admin_url: URL, auth_config: AuthConfig
) -> AsyncGenerator[AdminClient, None]:
    async with create_admin_client(admin_url, auth_config) as client:

        yield client


async def wait_for_admin_server(
    url: URL, auth_config: AuthConfig, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with create_admin_client(url, auth_config) as admin_client:
                    await admin_client.list_users()
                    break
            except (AssertionError, ClientError):
                pass
            await asyncio.sleep(interval_s)
