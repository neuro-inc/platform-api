import asyncio
import uuid
from dataclasses import asdict, dataclass
from typing import AsyncGenerator, Dict, Optional, Sequence

import aiodocker
import pytest
from aiohttp import ClientError
from aiohttp.hdrs import AUTHORIZATION
from async_generator import asynccontextmanager
from async_timeout import timeout
from jose import jwt
from neuro_auth_client import AuthClient, Permission, User as AuthClientUser
from yarl import URL

from platform_api.config import AuthConfig
from platform_api.user import User


@pytest.fixture(scope="session")
def auth_server_image_name() -> str:
    with open("AUTH_SERVER_IMAGE_NAME", "r") as f:
        return f.read()


@pytest.fixture(scope="session")
async def auth_server(
    docker, reuse_docker, auth_server_image_name
) -> AsyncGenerator[AuthConfig, None]:
    image_name = "gcr.io/light-reality-205619/platformauthapi:latest"
    container_name = "auth_server"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": ["NP_JWT_SECRET=secret"],
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                auth_config = await create_auth_config(container)
                await wait_for_auth_server(auth_config)
                yield auth_config
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.get(image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    auth_config = await create_auth_config(container)
    await wait_for_auth_server(auth_config)
    yield auth_config

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


def create_token(name: str) -> str:
    payload = {"identity": name}
    return jwt.encode(payload, "secret", algorithm="HS256")


@pytest.fixture
def token_factory():
    return create_token


@pytest.fixture
def admin_token(token_factory):
    return token_factory("admin")


async def create_auth_config(container) -> AuthConfig:
    host = "localhost"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    token = create_token("compute")
    return AuthConfig(server_endpoint_url=url, service_token=token)  # type: ignore


@pytest.fixture
async def auth_config(auth_server) -> AuthConfig:
    yield auth_server


class _AuthClient(AuthClient):
    async def grant_user_permissions(
        self, name: str, permissions: Sequence[Permission], token: Optional[str] = None
    ) -> None:
        assert permissions, "No permissions passed"
        path = "/api/v1/users/{name}/permissions".format(name=name)
        headers = self._generate_headers(token)
        payload = [asdict(p) for p in permissions]
        await self._request("POST", path, headers=headers, json=payload)


@asynccontextmanager
async def create_auth_client(config: AuthConfig) -> AsyncGenerator[_AuthClient, None]:
    async with _AuthClient(
        url=config.server_endpoint_url, token=config.service_token
    ) as client:
        yield client


@pytest.fixture
async def auth_client(auth_server: AuthConfig) -> AsyncGenerator[_AuthClient, None]:
    async with create_auth_client(auth_server) as client:
        yield client


async def wait_for_auth_server(
    config: AuthConfig, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with create_auth_client(config) as auth_client:
                    await auth_client.ping()
                    break
            except (AssertionError, ClientError):
                pass
            await asyncio.sleep(interval_s)


@dataclass(frozen=True)
class _User(User):
    @property
    def headers(self) -> Dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
async def regular_user_factory(auth_client, token_factory, admin_token):
    async def _factory(name: Optional[str] = None) -> _User:
        if not name:
            name = str(uuid.uuid4())
        user = AuthClientUser(name=name)
        await auth_client.add_user(user, token=admin_token)
        return _User(name=user.name, token=token_factory(user.name))  # type: ignore

    return _factory


@pytest.fixture
async def regular_user(regular_user_factory) -> _User:
    return await regular_user_factory()
