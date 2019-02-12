import os
import time
import uuid
from dataclasses import dataclass

import aiohttp
import pytest
from jose import jwt
from neuro_auth_client import AuthClient, User
from yarl import URL


class PlatformConfig:
    endpoint_url: str

    def __init__(self, endpoint_url):
        self.endpoint_url = endpoint_url

    @property
    def ping_url(self):
        return self.endpoint_url + "/ping"

    @property
    def jobs_url(self):
        return self.endpoint_url + "/jobs"

    @property
    def models_url(self):
        return self.endpoint_url + "/models"


@dataclass
class SSHAuthConfig:
    ip: str
    port: int


@pytest.fixture(scope="session")
def api_endpoint_url():
    if "PLATFORM_API_URL" not in os.environ:
        pytest.fail("Environment variable PLATFORM_API_URL is not set")
    return os.environ["PLATFORM_API_URL"]


@pytest.fixture(scope="session")
def api_config(api_endpoint_url):
    return PlatformConfig(api_endpoint_url)


@pytest.fixture(scope="session")
def ssh_auth_config():
    if "SSH_AUTH_URL" not in os.environ:
        pytest.fail("Environment variable SSH_AUTH_URL is not set")
    url = URL(os.environ["SSH_AUTH_URL"])
    return SSHAuthConfig(url.host, url.port)


@pytest.fixture(scope="session")
def platform_auth_url():
    if "AUTH_API_URL" not in os.environ:
        pytest.fail("Environment variable AUTH_API_URL is not set")
    return URL(os.environ["AUTH_API_URL"])


@pytest.fixture(scope="session")
def token_factory():
    def _factory(name: str):
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture(scope="session")
def admin_token(token_factory):
    return token_factory("admin")


@pytest.fixture
async def client():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def api(api_config, client):
    url = api_config.ping_url
    interval_s = 1
    max_attempts = 30
    for _ in range(max_attempts):
        try:
            response = await client.get(url)
            if response.status == 200:
                break
        except OSError:
            pass
        time.sleep(interval_s)
    else:
        pytest.fail(f"Unable to connect to Platform API: {url}")


@pytest.fixture
async def auth_client(platform_auth_url, admin_token):
    async with AuthClient(url=platform_auth_url, token=admin_token) as client:
        yield client


@dataclass
class _User:
    name: str
    token: str


@pytest.fixture
async def regular_user_factory(auth_client, token_factory):
    async def _factory(name=None):
        if not name:
            name = str(uuid.uuid4())
        user = User(name=name)
        await auth_client.add_user(user)
        return _User(name=user.name, token=token_factory(user.name))

    return _factory


@pytest.fixture
async def alice(regular_user_factory):
    return await regular_user_factory()


@pytest.fixture
async def bob(regular_user_factory):
    return await regular_user_factory()
