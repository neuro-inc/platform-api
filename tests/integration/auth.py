from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol

import pytest
from aiohttp import ClientResponseError
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

from platform_api.config import OAuthConfig
from tests.conftest import random_str


def create_token(name: str) -> str:
    payload = {"identity": name}
    return jwt.encode(payload, "secret", algorithm="HS256")


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    return create_token


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


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
        name: str | None = None,
        # fmt: off
        clusters: (list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]])
        | None = None,
        # fmt: on
        cluster_user_role: ClusterUserRoleType = ClusterUserRoleType.USER,
        org_user_role: OrgUserRoleType = OrgUserRoleType.USER,
        project_name: str = "",
        do_create_project: bool = True,
    ) -> _User: ...


@pytest.fixture
async def regular_user_factory(
    k8s_auth_client: AuthClient,
    k8s_config_client: ConfigClient,
    k8s_admin_client: AdminClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
    org_name: str,
    project_name: str,
) -> UserFactory:
    async def _factory(
        name: str | None = None,
        # fmt: off
        clusters: (list[tuple[str, Balance, Quota] | tuple[str, str, Balance, Quota]])
        | None = None,
        # fmt: on
        cluster_user_role: ClusterUserRoleType = ClusterUserRoleType.USER,
        org_user_role: OrgUserRoleType = OrgUserRoleType.USER,
        project_name: str = project_name,
        do_create_project: bool = True,
    ) -> _User:
        if not name:
            name = random_str()
        if clusters is None:
            clusters = [(test_cluster_name, org_name, Balance(), Quota())]
        await k8s_admin_client.create_user(name=name, email=f"{name}@email.com")
        user_token = token_factory(name)
        for entry in clusters:
            entry_org_name: str | None = None
            if len(entry) == 3:
                cluster, balance, quota = entry
            else:
                cluster, entry_org_name, balance, quota = entry
            try:
                await k8s_admin_client.create_cluster(cluster)
            except ClientResponseError:
                pass
            try:
                # in case docker containers are reused, we want to recreate clusters
                # that were previously stored in memory
                await k8s_config_client.create_blank_cluster(
                    name=cluster, service_token="cluster-token"
                )
            except ClientResponseError:
                pass

            if entry_org_name is not None:
                try:
                    await k8s_admin_client.create_org(entry_org_name)
                except ClientResponseError:
                    pass
                try:
                    await k8s_admin_client.create_org_user(
                        org_name=entry_org_name,
                        user_name=name,
                        role=org_user_role,
                        balance=balance,
                    )
                except ClientResponseError:
                    pass
                try:
                    await k8s_admin_client.create_org_cluster(
                        cluster_name=cluster,
                        org_name=entry_org_name,
                    )
                except ClientResponseError:
                    pass
                try:
                    await k8s_admin_client.update_org_balance(
                        org_name=entry_org_name,
                        credits=Decimal("100"),
                    )
                except ClientResponseError:
                    pass
            try:
                await k8s_admin_client.create_cluster_user(
                    cluster_name=cluster,
                    org_name=entry_org_name,
                    role=cluster_user_role,
                    user_name=name,
                    quota=quota,
                )
            except ClientResponseError:
                pass
            if do_create_project:
                try:
                    await k8s_admin_client.create_project(
                        project_name,
                        cluster,
                        entry_org_name,
                    )
                except ClientResponseError:
                    pass
                try:
                    await k8s_admin_client.create_project_user(
                        project_name=project_name,
                        cluster_name=cluster,
                        org_name=entry_org_name,
                        user_name=name,
                    )
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
    k8s_auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    test_cluster_name: str,
    org_name: str,
    project_name: str,
) -> ServiceAccountFactory:
    async def _factory(
        owner: _User,
        name: str | None = None,
    ) -> _User:
        if not name:
            name = random_str()
        user = AuthUser(name=f"{owner.name}/service-accounts/{name}")
        await k8s_auth_client.add_user(user, token=admin_token)
        permissions = []
        # Grant org-level permissions (covers all projects under the org)
        for cluster in owner.clusters:
            permissions.extend(
                [
                    Permission(
                        uri=f"disk://{cluster}/{org_name}",
                        action="write",
                    ),
                    Permission(
                        uri=f"job://{cluster}/{org_name}",
                        action="write",
                    ),
                    Permission(
                        uri=f"storage://{cluster}/{org_name}",
                        action="write",
                    ),
                    Permission(
                        uri=f"image://{cluster}/{org_name}",
                        action="write",
                    ),
                    Permission(
                        uri=f"secret://{cluster}/{org_name}",
                        action="read",
                    ),
                ]
            )
        # Grant owner-specific manage permissions (for service account operations)
        for cluster in owner.clusters:
            permissions.extend(
                [
                    Permission(
                        uri=f"storage://{cluster}/{org_name}/{owner.name}",
                        action="manage",
                    ),
                    Permission(
                        uri=f"image://{cluster}/{org_name}/{owner.name}",
                        action="manage",
                    ),
                    Permission(
                        uri=f"job://{cluster}/{org_name}/{owner.name}",
                        action="manage",
                    ),
                    Permission(
                        uri=f"secret://{cluster}/{org_name}/{owner.name}",
                        action="manage",
                    ),
                    Permission(
                        uri=f"secret://{cluster}/{org_name}/{project_name}",
                        action="manage",
                    ),
                    Permission(
                        uri=f"disk://{cluster}/{org_name}/{owner.name}",
                        action="write",
                    ),
                ]
            )
        await k8s_auth_client.grant_user_permissions(
            user.name, permissions, token=admin_token
        )
        return _User(
            name=user.name,
            token=token_factory(user.name),
            clusters=owner.clusters,
        )

    return _factory


@pytest.fixture
async def regular_user(
    regular_user_factory: UserFactory, test_cluster_name: str, org_name: str
) -> _User:
    return await regular_user_factory(
        clusters=[(test_cluster_name, org_name, Balance(), Quota())]
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
