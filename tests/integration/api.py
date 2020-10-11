import asyncio
import json
import time
from typing import Any, AsyncIterator, Callable, Dict, List, NamedTuple, Optional

import aiohttp
import aiohttp.web
import pytest
from aiohttp.client import ClientSession
from aiohttp.web import HTTPAccepted, HTTPNoContent, HTTPOk
from yarl import URL

from platform_api.api import create_app
from platform_api.cluster_config import ClusterConfig
from platform_api.config import AuthConfig, Config
from platform_api.orchestrator.job import JobStatus

from .auth import _User
from .conftest import ApiRunner


class ApiConfig(NamedTuple):
    host: str
    port: int
    runner: ApiRunner

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}/api/v1"

    @property
    def jobs_base_url(self) -> str:
        return self.endpoint + "/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"

    @property
    def ping_url(self) -> str:
        return self.endpoint + "/ping"

    @property
    def config_url(self) -> str:
        return self.endpoint + "/config"

    @property
    def clusters_sync_url(self) -> str:
        return self.endpoint + "/config/clusters/sync"

    @property
    def stats_base_url(self) -> str:
        return f"{self.endpoint}/stats"

    def stats_for_user_url(self, username: str) -> str:
        return f"{self.stats_base_url}/users/{username}"

    @property
    def tags_base_url(self) -> str:
        return f"{self.endpoint}/tags"


class AuthApiConfig(NamedTuple):
    server_endpoint_url: URL

    @property
    def endpoint(self) -> str:
        return f"{self.server_endpoint_url}/api/v1/users"

    def auth_for_user_url(self, username: str) -> str:
        return f"{self.endpoint}/{username}"


@pytest.fixture
async def api(
    config: Config, cluster_config_factory: Callable[..., ClusterConfig]
) -> AsyncIterator[ApiConfig]:
    app = await create_app(
        config,
        [
            cluster_config_factory("test-cluster"),
            cluster_config_factory("testcluster2"),
        ],
    )
    runner = ApiRunner(app, port=8080)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


@pytest.fixture
async def api_with_oauth(
    config_with_oauth: Config, cluster_config: ClusterConfig
) -> AsyncIterator[ApiConfig]:
    app = await create_app(config_with_oauth, [cluster_config])
    runner = ApiRunner(app, port=8081)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


@pytest.fixture
async def auth_api(auth_config: AuthConfig) -> AuthApiConfig:
    return AuthApiConfig(server_endpoint_url=auth_config.server_endpoint_url)


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


class JobsClient:
    def __init__(
        self, api_config: ApiConfig, client: ClientSession, headers: Dict[str, str]
    ) -> None:
        self._api_config = api_config
        self._client = client
        self._headers = headers

    async def create_job(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._api_config.jobs_base_url
        async with self._client.post(url, headers=self._headers, json=payload) as resp:
            assert resp.status == HTTPAccepted.status_code, await resp.text()
            result = await resp.json()
            assert result["status"] == "pending"
            return result

    async def get_all_jobs(self, params: Any = None) -> List[Dict[str, Any]]:
        url = self._api_config.jobs_base_url
        headers = self._headers.copy()
        headers["Accept"] = "application/x-ndjson"
        async with self._client.get(url, headers=headers, params=params) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert response.headers["Content-Type"] == "application/x-ndjson"
            jobs = [json.loads(line) async for line in response.content]

        for job in jobs:
            assert isinstance(job, dict)
            for key in job:
                assert isinstance(key, str)
        return jobs

    async def get_job_by_id(
        self,
        job_id: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = self._api_config.generate_job_url(job_id)
        async with self._client.get(url, headers=headers or self._headers) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        return result

    async def long_polling_by_job_id(
        self,
        job_id: str,
        status: str,
        interval_s: float = 0.5,
        max_time: float = 300,
        unreachable_optimization: bool = True,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:

        # A little optimization with unreachable statuses
        unreachable_statuses_map: Dict[str, List[str]] = {
            JobStatus.PENDING.value: [
                JobStatus.RUNNING.value,
                JobStatus.SUCCEEDED.value,
                JobStatus.FAILED.value,
            ],
            JobStatus.RUNNING.value: [
                JobStatus.SUCCEEDED.value,
                JobStatus.FAILED.value,
            ],
            JobStatus.SUCCEEDED.value: [
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ],
            JobStatus.FAILED.value: [
                JobStatus.SUCCEEDED.value,
                JobStatus.CANCELLED.value,
            ],
            JobStatus.CANCELLED.value: [
                JobStatus.SUCCEEDED.value,
                JobStatus.FAILED.value,
            ],
        }
        stop_statuses: List[str] = []
        if unreachable_optimization and status in unreachable_statuses_map:
            stop_statuses = unreachable_statuses_map[status]

        t0 = time.monotonic()
        while True:
            response = await self.get_job_by_id(job_id, headers=headers)
            if response["status"] == status:
                return response
            if response["status"] in stop_statuses:
                pytest.fail(f"Status {status} cannot be reached, resp: {response}")
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec; resp: {response}")
            interval_s *= 1.5

    async def wait_job_creation(
        self, job_id: str, interval_s: float = 0.5, max_time: float = 300
    ) -> Dict[str, Any]:
        t0 = time.monotonic()
        while True:
            response = await self.get_job_by_id(job_id)
            if (
                response["status"] != "pending"
                or response["history"]["reason"] != "Creating"
            ):
                return response
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec; resp: {response}")
            interval_s *= 1.5

    async def delete_job(
        self,
        job_id: str,
        assert_success: bool = True,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        url = self._api_config.generate_job_url(job_id)
        async with self._client.delete(
            url, headers=headers or self._headers
        ) as response:
            if assert_success:
                assert (
                    response.status == HTTPNoContent.status_code
                ), await response.text()


@pytest.fixture
async def jobs_client_factory(
    api: ApiConfig, client: ClientSession
) -> AsyncIterator[Callable[[_User], JobsClient]]:
    jobs_clients: List[JobsClient] = []

    def impl(user: _User) -> JobsClient:
        jobs_client = JobsClient(api, client, headers=user.headers)
        jobs_clients.append(jobs_client)
        return jobs_client

    yield impl

    params = [("status", "pending"), ("status", "running")]
    for jobs_client in jobs_clients:
        try:
            jobs = await jobs_client.get_all_jobs(params)
        except aiohttp.ClientConnectorError:
            # server might be down
            continue
        for job in jobs:
            await jobs_client.delete_job(job["id"], assert_success=False)


@pytest.fixture
def jobs_client(
    jobs_client_factory: Callable[[_User], JobsClient], regular_user: _User
) -> JobsClient:
    return jobs_client_factory(regular_user)


@pytest.fixture
async def infinite_job(
    api: ApiConfig,
    client: aiohttp.ClientSession,
    regular_user: _User,
    jobs_client: JobsClient,
) -> AsyncIterator[str]:
    request_payload = {
        "container": {
            "image": "ubuntu",
            "command": "tail -f /dev/null",
            "resources": {"cpu": 0.1, "memory_mb": 16},
        }
    }
    async with client.post(
        api.jobs_base_url, headers=regular_user.headers, json=request_payload
    ) as response:
        assert response.status == HTTPAccepted.status_code, await response.text()
        result = await response.json()
        job_id = result["id"]
        assert isinstance(job_id, str)

    yield job_id

    await jobs_client.delete_job(job_id)


@pytest.fixture
def job_request_factory() -> Callable[[], Dict[str, Any]]:
    def _factory(cluster_name: Optional[str] = None) -> Dict[str, Any]:
        # Note: Optional fields (as "name") should not have a value here
        request = {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "http": {"port": 1234},
            },
            "description": "test job submitted by neuro job submit",
        }
        if cluster_name:
            request["cluster_name"] = cluster_name
        return request

    return _factory


@pytest.fixture
async def job_submit(
    job_request_factory: Callable[[], Dict[str, Any]]
) -> Dict[str, Any]:
    return job_request_factory()
