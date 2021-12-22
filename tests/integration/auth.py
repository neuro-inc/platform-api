import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional, Protocol, Union, cast

import aiodocker
import pytest
from aiohttp import ClientError, ClientResponseError
from aiohttp.hdrs import AUTHORIZATION
from async_timeout import timeout
from jose import jwt
from neuro_admin_client import (
    AdminClient,
    Balance,
    ClusterUserRoleType,
    OrgUserRoleType,
    Quota,
)
from neuro_auth_client import AuthClient, Permission, User as AuthUser
from yarl import URL

from platform_api.config import AuthConfig, OAuthConfig
from tests.conftest import random_str


@pytest.fixture(scope="session")
def auth_server_image_name() -> str:
    with open("PLATFORMAUTHAPI_IMAGE") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
async def auth_server(
    docker: aiodocker.Docker, reuse_docker: bool, auth_server_image_name: str
) -> AsyncIterator[AuthConfig]:
    image_name = auth_server_image_name
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
        await docker.images.inspect(auth_server_image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(auth_server_image_name)

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


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


async def create_auth_config(
    container: aiodocker.containers.DockerContainer,
) -> AuthConfig:
    host = "0.0.0.0"
    port = int((await container.port(8080))[0]["HostPort"])
    url = URL(f"http://{host}:{port}")
    token = create_token("compute")
    public_endpoint_url = URL("https://neu.ro/api/v1/users")
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
class _User:
    name: str
    token: str
    clusters: list[str] = field(default_factory=list)

    @property
    def cluster_name(self) -> str:
        assert self.clusters, "Test user has no access to any cluster"
        return self.clusters[0]

    @property
    def headers(self) -> dict[str, str]:
        return {AUTHORIZATION: f"Bearer {self.token}"}


@pytest.fixture
def test_cluster_name() -> str:
    return "test-cluster"


class UserFactory(Protocol):
    async def __call__(
        self,
        name: Optional[str] = None,
        clusters: Optional[
            list[Union[tuple[str, Balance, Quota], tuple[str, str, Balance, Quota]]]
        ] = None,
    ) -> _User:
        ...


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    admin_client: AdminClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
) -> UserFactory:
    async def _factory(
        name: Optional[str] = None,
        clusters: Optional[
            list[Union[tuple[str, Balance, Quota], tuple[str, str, Balance, Quota]]]
        ] = None,
    ) -> _User:
        if not name:
            name = random_str()
        if clusters is None:
            clusters = [(test_cluster_name, Balance(), Quota())]
        await admin_client.create_user(name=name, email=f"{name}@email.com")
        for entry in clusters:
            org_name: Optional[str] = None
            if len(entry) == 3:
                cluster, balance, quota = cast(tuple[str, Balance, Quota], entry)
            else:
                cluster, org_name, balance, quota = cast(
                    tuple[str, str, Balance, Quota], entry
                )
            try:
                await admin_client.create_cluster(cluster)
            except ClientResponseError:
                pass
            if org_name is not None:
                try:
                    await admin_client.create_org(org_name)
                except ClientResponseError:
                    pass
                await admin_client.create_org_user(
                    org_name=org_name,
                    user_name=name,
                    role=OrgUserRoleType.USER,
                )
                try:
                    await admin_client.create_org_cluster(
                        cluster_name=cluster,
                        org_name=org_name,
                    )
                except ClientResponseError:
                    pass
            await admin_client.create_cluster_user(
                cluster_name=cluster,
                org_name=org_name,
                role=ClusterUserRoleType.USER,
                user_name=name,
                balance=balance,
                quota=quota,
            )
        user_token = token_factory(name)
        return _User(
            name=name,
            token=user_token,
            clusters=[cluster_info[0] for cluster_info in clusters],
        )

    return _factory


class ServiceAccountFactory(Protocol):
    async def __call__(
        self,
        owner: _User,
        name: Optional[str] = None,
    ) -> _User:
        ...


@pytest.fixture
async def service_account_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
) -> ServiceAccountFactory:
    async def _factory(
        owner: _User,
        name: Optional[str] = None,
    ) -> _User:
        if not name:
            name = random_str()
        user = AuthUser(name=f"{owner.name}/service-accounts/{name}", clusters=[])
        await auth_client.add_user(user, token=admin_token)
        permissions = []
        # Fake grant access to SA staff
        for cluster in owner.clusters:
            permissions.extend(
                [
                    Permission(uri=f"storage://{cluster}/{user.name}", action="manage"),
                    Permission(uri=f"image://{cluster}/{user.name}", action="manage"),
                    Permission(uri=f"job://{cluster}/{user.name}", action="manage"),
                    Permission(uri=f"secret://{cluster}/{user.name}", action="manage"),
                    Permission(uri=f"disk://{cluster}/{user.name}", action="write"),
                ]
            )
        await auth_client.grant_user_permissions(
            user.name, permissions, token=admin_token
        )
        return _User(
            name=user.name,
            token=token_factory(user.name),
            clusters=owner.clusters,
        )

    return _factory


@pytest.fixture
async def regular_user(regular_user_factory: UserFactory) -> _User:
    return await regular_user_factory()


@pytest.fixture
async def regular_user_with_missing_cluster_name(
    regular_user_factory: UserFactory,
) -> _User:
    return await regular_user_factory(None, [("missing", Balance(), Quota())])


@pytest.fixture
def cluster_user(token_factory: Callable[[str], str]) -> _User:
    name = "cluster"
    return _User(
        name=name,
        token=token_factory(name),
    )


@pytest.fixture
def compute_user(token_factory: Callable[[str], str]) -> _User:
    name = "compute"
    return _User(
        name=name,
        token=token_factory(name),
    )


@pytest.fixture
def oauth_config_dev() -> OAuthConfig:
    return OAuthConfig(
        auth_url=URL("https://platform-auth0-url/auth"),
        token_url=URL("https://platform-auth0-url/token"),
        logout_url=URL("https://platform-auth0-url/logout"),
        client_id="client_id",
        audience="https://platform-dev-url",
        headless_callback_url=URL("https://dev.neu.ro/oauth/show-code"),
        success_redirect_url=URL("https://platform-default-url"),
    )
