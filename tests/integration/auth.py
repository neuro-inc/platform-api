import asyncio
from dataclasses import dataclass
from typing import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
)

import aiodocker
import pytest
from aiohttp import ClientError
from aiohttp.hdrs import AUTHORIZATION
from async_generator import asynccontextmanager
from async_timeout import timeout
from jose import jwt
from neuro_auth_client import (
    AuthClient,
    Cluster as AuthCluster,
    Quota,
    User as AuthClientUser,
)
from yarl import URL

from platform_api.config import AuthConfig, OAuthConfig
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.user import User
from tests.conftest import random_str


@pytest.fixture(scope="session")
def auth_server_image_name() -> str:
    with open("PLATFORMAUTHAPI_IMAGE", "r") as f:
        return f.read()


@pytest.fixture(scope="session")
async def auth_server(
    docker: aiodocker.Docker, reuse_docker: bool, auth_server_image_name: str
) -> AsyncIterator[AuthConfig]:
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
        await docker.images.inspect(image_name)
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


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    return create_token


@pytest.fixture
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


async def create_auth_config(
    container: aiodocker.containers.DockerContainer,
) -> AuthConfig:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    token = create_token("compute")
    public_endpoint_url = URL(f"https://neu.ro/api/v1/users")
    return AuthConfig(
        server_endpoint_url=url,
        service_token=token,
        public_endpoint_url=public_endpoint_url,
    )


@pytest.fixture
async def auth_config(auth_server: AuthConfig) -> AsyncIterator[AuthConfig]:
    yield auth_server


@asynccontextmanager
async def create_auth_client(config: AuthConfig) -> AsyncGenerator[AuthClient, None]:
    async with AuthClient(
        url=config.server_endpoint_url, token=config.service_token
    ) as client:
        yield client


@pytest.fixture
async def auth_client(auth_server: AuthConfig) -> AsyncGenerator[AuthClient, None]:
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
async def regular_user_factory(
    auth_client: AuthClient, token_factory: Callable[[str], str], admin_token: str
) -> Callable[[Optional[str], Optional[Quota], str], Awaitable[_User]]:
    async def _factory(
        name: Optional[str] = None,
        quota: Optional[Quota] = None,
        cluster_name: str = "test-cluster",
        auth_clusters: Optional[Sequence[AuthCluster]] = None,
    ) -> _User:
        if not name:
            name = random_str()
        quota = quota or Quota()
        if auth_clusters is None:
            auth_clusters = [AuthCluster(name=cluster_name, quota=quota)]
        user = AuthClientUser(name=name, clusters=auth_clusters)
        await auth_client.add_user(user, token=admin_token)
        # Grant cluster-specific permissions
        headers = auth_client._generate_headers(admin_token)
        payload = []
        for cluster in auth_clusters:
            payload.extend(
                [
                    {"uri": f"storage://{cluster.name}/{name}", "action": "manage"},
                    {"uri": f"image://{cluster.name}/{name}", "action": "manage"},
                    {"uri": f"job://{cluster.name}/{name}", "action": "manage"},
                    {"uri": f"secret://{cluster.name}/{name}", "action": "write"},
                ]
            )
        async with auth_client._request(
            "POST", f"/api/v1/users/{name}/permissions", headers=headers, json=payload,
        ) as p:
            assert p.status == 201
        user_token = token_factory(user.name)
        return _User.create_from_auth_user(user, token=user_token)  # type: ignore

    return _factory


@pytest.fixture
async def regular_user(regular_user_factory: Callable[[], Awaitable[_User]]) -> _User:
    return await regular_user_factory()


@pytest.fixture
async def regular_user_with_missing_cluster_name(
    regular_user_factory: Callable[
        [Optional[str], Optional[Quota], Optional[str]], Awaitable[_User]
    ],
) -> _User:
    return await regular_user_factory(None, None, "missing")


@pytest.fixture
async def regular_user_with_custom_quota(
    regular_user_factory: Callable[..., Awaitable[_User]],
) -> _User:
    return await regular_user_factory(
        auth_clusters=[
            AuthCluster(
                name="test-cluster",
                quota=Quota(
                    total_gpu_run_time_minutes=123, total_non_gpu_run_time_minutes=321
                ),
            ),
            AuthCluster(name="testcluster2"),
        ]
    )


@pytest.fixture
def cluster_user(token_factory: Callable[[str], str]) -> _User:
    name = "cluster"
    return _User(  # noqa
        name=name,
        token=token_factory(name),
        quota=AggregatedRunTime.from_quota(Quota()),
        cluster_name="",
    )


@pytest.fixture
def compute_user(token_factory: Callable[[str], str]) -> _User:
    name = "compute"
    return _User(  # noqa
        name=name,
        token=token_factory(name),
        quota=AggregatedRunTime.from_quota(Quota()),
        cluster_name="",
    )


@pytest.fixture
def oauth_config_dev() -> OAuthConfig:
    return OAuthConfig(
        base_url=URL("https://platform-auth0-url"),
        client_id="client_id",
        audience="https://platform-dev-url",
        headless_callback_url=URL("https://dev.neu.ro/oauth/show-code"),
        success_redirect_url=URL("https://platform-default-url"),
    )
