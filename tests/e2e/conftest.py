import os
import time
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable, Optional

import aiohttp
import pytest
from jose import jwt
from neuro_auth_client import AuthClient, User
from yarl import URL

from tests.conftest import random_str


class PlatformConfig:
    endpoint_url: str

    def __init__(self, endpoint_url: str) -> None:
        self.endpoint_url = endpoint_url

    @property
    def ping_url(self) -> str:
        return self.endpoint_url + "/ping"

    @property
    def jobs_url(self) -> str:
        return self.endpoint_url + "/jobs"


@dataclass
class SSHAuthConfig:
    ip: str
    port: int
    jobs_namespace: str = "default"


@pytest.fixture(scope="session")
def api_endpoint_url() -> str:
    if "PLATFORM_API_URL" not in os.environ:
        pytest.fail("Environment variable PLATFORM_API_URL is not set")
    return os.environ["PLATFORM_API_URL"]


@pytest.fixture(scope="session")
def api_config(api_endpoint_url: str) -> PlatformConfig:
    return PlatformConfig(api_endpoint_url)


@pytest.fixture(scope="session")
def ssh_auth_config() -> SSHAuthConfig:
    if "SSH_AUTH_URL" not in os.environ:
        pytest.fail("Environment variable SSH_AUTH_URL is not set")
    url = URL(os.environ["SSH_AUTH_URL"])
    assert url.host
    assert url.port
    namespace = os.environ.get("NP_K8S_NS", SSHAuthConfig.jobs_namespace)
    return SSHAuthConfig(url.host, url.port, jobs_namespace=namespace)


@pytest.fixture(scope="session")
def platform_auth_url() -> URL:
    if "AUTH_API_URL" not in os.environ:
        pytest.fail("Environment variable AUTH_API_URL is not set")
    return URL(os.environ["AUTH_API_URL"])


@pytest.fixture(scope="session")
def token_factory() -> Callable[[str], str]:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture(scope="session")
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def api(api_config: PlatformConfig, client: aiohttp.ClientSession) -> None:
    url = api_config.ping_url
    interval_s = 1
    attempts = 30
    while True:
        attempts -= 1
        try:
            response = await client.get(url)
            if response.status == 200:
                break
            if attempts <= 0:
                assert response.status == 200, (
                    f"Unable to connect to Platform API: {url}\n"
                    f"response: {await response.text()!r}"
                )
                break
        except OSError:
            if attempts <= 0:
                raise
            pass
        time.sleep(interval_s)


@pytest.fixture
async def auth_client(
    platform_auth_url: URL, admin_token: str
) -> AsyncIterator[AuthClient]:
    async with AuthClient(url=platform_auth_url, token=admin_token) as client:
        yield client


@pytest.fixture
def cluster_name() -> str:
    # TODO (serhiy 3-Feb-2020): use non-default name
    return "default"


@dataclass
class _User:
    name: str
    token: str


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: Callable[[str], str],
    admin_token: str,
    cluster_name: str,
) -> Callable[[Optional[str]], Awaitable[_User]]:
    async def _factory(name: Optional[str] = None) -> _User:
        if not name:
            name = random_str()
        user = User(name=name, cluster_name=cluster_name)
        await auth_client.add_user(user)
        # Grant permissions to the user home directory
        headers = auth_client._generate_headers(admin_token)
        payload = [
            {"uri": f"storage://{cluster_name}/{name}", "action": "manage"},
        ]
        async with auth_client._request(
            "POST", f"/api/v1/users/{name}/permissions", headers=headers, json=payload
        ) as p:
            assert p.status == 201
        return _User(name=user.name, token=token_factory(user.name))

    return _factory


@pytest.fixture
async def alice(regular_user_factory: Callable[[], Awaitable[_User]]) -> _User:
    return await regular_user_factory()


@pytest.fixture
async def bob(regular_user_factory: Callable[[], Awaitable[_User]]) -> _User:
    return await regular_user_factory()
