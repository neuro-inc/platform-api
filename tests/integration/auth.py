from __future__ import annotations

import asyncio
from asyncio import timeout
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Protocol

import aiodocker
import pytest
from aiodocker.types import JSONObject
from aiohttp import ClientError, ClientResponseError
from aiohttp.hdrs import AUTHORIZATION
from jose import jwt
from neuro_admin_client import (
    AdminClient,
    Balance,
    ClusterUserRoleType,
    OrgUserRoleType,
    Quota,
)
from neuro_auth_client import AuthClient, Permission, User as AuthUser
from neuro_config_client import ConfigClient
from yarl import URL

from platform_api.config import AuthConfig, OAuthConfig
from tests.conftest import random_str


@pytest.fixture(scope="session")
def auth_server_image_name() -> str:
    with Path("PLATFORMAUTHAPI_IMAGE").open() as f:
        return f.read().strip()


@pytest.fixture
async def auth_server(
    docker: aiodocker.Docker, reuse_docker: bool, auth_server_image_name: str
) -> AsyncIterator[AuthConfig]:
    image_name = auth_server_image_name
    container_name = "auth_server"
    container_config: JSONObject = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": ["NP_JWT_SECRET=secret", "NP_LOG_LEVEL=DEBUG"],
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
    val = await container.port(8080)
    assert val is not None
    port = int(val[0]["HostPort"])
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
async def create_auth_client(config: AuthConfig) -> AsyncGenerator[AuthClient]:
    async with AuthClient(
        url=config.server_endpoint_url, token=config.service_token
    ) as client:
        yield client


@pytest.fixture
async def auth_client(auth_server: AuthConfig) -> AsyncGenerator[AuthClient]:
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


@pytest.fixture
def test_org_name() -> str:
    return "test-org"


class UserFactory(Protocol):
    async def __call__(
        self,
        name: str | None = None,
        # fmt: off
        clusters: (list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]])
        | None = None,
        # fmt: on
        cluster_user_role: ClusterUserRoleType = ClusterUserRoleType.USER,
        org_user_role: OrgUserRoleType = OrgUserRoleType.USER,
        do_create_project: bool = True,
    ) -> _User: ...


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    config_client: ConfigClient,
    admin_client: AdminClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
    test_org_name: str,
    admin_client_factory: Callable[[str], Awaitable[AdminClient]],
) -> UserFactory:
    async def _factory(
        name: str | None = None,
        # fmt: off
        clusters: (list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]])
        | None = None,
        # fmt: on
        cluster_user_role: ClusterUserRoleType = ClusterUserRoleType.USER,
        org_user_role: OrgUserRoleType = OrgUserRoleType.USER,
        do_create_project: bool = True,
    ) -> _User:
        if not name:
            name = random_str()
        if clusters is None:
            clusters = [(test_cluster_name, Balance(), Quota())]
        await admin_client.create_user(name=name, email=f"{name}@email.com")
        user_token = token_factory(name)
        user_admin_client = await admin_client_factory(user_token)
        admin_admin_client = await admin_client_factory(admin_token)
        for entry in clusters:
            org_name: str | None = None
            if len(entry) == 3:
                cluster, balance, quota = entry
            else:
                cluster, org_name, balance, quota = entry
            try:
                await admin_client.create_cluster(cluster)
            except ClientResponseError:
                pass
            try:
                # in case docker containers are reused, we want to recreate clusters
                # that were previously stored in memory
                await config_client.create_blank_cluster(
                    name=cluster, service_token="cluster-token"
                )
            except ClientResponseError:
                pass

            if org_name is not None:
                try:
                    await admin_client.create_org(org_name)
                except ClientResponseError:
                    pass
                try:
                    await admin_client.create_org_user(
                        org_name=org_name,
                        user_name=name,
                        role=org_user_role,
                        balance=balance,
                    )
                except ClientResponseError:
                    pass
                try:
                    await admin_client.create_org_cluster(
                        cluster_name=cluster,
                        org_name=org_name,
                    )
                except ClientResponseError:
                    pass
                try:
                    await admin_admin_client.update_org_balance(
                        org_name=org_name,
                        credits=Decimal("100"),
                    )
                except ClientResponseError:
                    pass
            try:
                await admin_client.create_cluster_user(
                    cluster_name=cluster,
                    org_name=org_name,
                    role=cluster_user_role,
                    user_name=name,
                    quota=quota,
                )
            except ClientResponseError:
                pass
            if do_create_project:
                # creating a default project in the tenant for the user
                try:
                    await user_admin_client.create_project(name, cluster, org_name)
                except ClientResponseError:
                    pass
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
        name: str | None = None,
    ) -> _User: ...


@pytest.fixture
async def service_account_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
) -> ServiceAccountFactory:
    async def _factory(
        owner: _User,
        name: str | None = None,
    ) -> _User:
        if not name:
            name = random_str()
        user = AuthUser(name=f"{owner.name}/service-accounts/{name}")
        await auth_client.add_user(user, token=admin_token)
        permissions = []
        # Fake grant access to SA staff
        for cluster in owner.clusters:
            permissions.extend(
                [
                    Permission(
                        uri=f"storage://{cluster}/{owner.name}", action="manage"
                    ),
                    Permission(uri=f"image://{cluster}/{owner.name}", action="manage"),
                    Permission(uri=f"job://{cluster}/{owner.name}", action="manage"),
                    Permission(uri=f"secret://{cluster}/{owner.name}", action="manage"),
                    Permission(uri=f"disk://{cluster}/{owner.name}", action="write"),
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
