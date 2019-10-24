import asyncio
import time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Sequence,
)

import aiohttp
import aiohttp.web
import pytest
from aiohttp.client import ClientSession
from aiohttp.web import HTTPAccepted, HTTPNoContent, HTTPOk

from platform_api.api import create_app
from platform_api.cluster_config import ClusterConfig
from platform_api.config import Config
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


async def get_cluster_configs(
    cluster_configs: Sequence[ClusterConfig]
) -> Sequence[ClusterConfig]:
    return cluster_configs


@pytest.fixture
async def api(
    config: Config, cluster_config: ClusterConfig
) -> AsyncIterator[ApiConfig]:
    app = await create_app(config, get_cluster_configs([cluster_config]))
    runner = ApiRunner(app, port=8080)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


@pytest.fixture
async def api_with_oauth(
    config_with_oauth: Config, cluster_config: ClusterConfig
) -> AsyncIterator[ApiConfig]:
    app = await create_app(config_with_oauth, get_cluster_configs([cluster_config]))
    runner = ApiRunner(app, port=8081)
    api_address = await runner.run()
    api_config = ApiConfig(host=api_address.host, port=api_address.port, runner=runner)
    yield api_config
    await runner.close()


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

    async def get_all_jobs(self, params: Any = None) -> List[Dict[str, Any]]:
        url = self._api_config.jobs_base_url
        async with self._client.get(
            url, headers=self._headers, params=params
        ) as response:
            response_text = await response.text()
            assert response.status == HTTPOk.status_code, response_text
            result = await response.json()
        jobs = result["jobs"]
        assert isinstance(jobs, list)
        for job in jobs:
            assert isinstance(job, dict)
            for key in job:
                assert isinstance(key, str)
        return jobs

    async def get_job_by_id(self, job_id: str) -> Dict[str, Any]:
        url = self._api_config.generate_job_url(job_id)
        async with self._client.get(url, headers=self._headers) as response:
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
            JobStatus.SUCCEEDED.value: [JobStatus.FAILED.value],
            JobStatus.FAILED.value: [JobStatus.SUCCEEDED],
        }
        stop_statuses: List[str] = []
        if unreachable_optimization and status in unreachable_statuses_map:
            stop_statuses = unreachable_statuses_map[status]

        t0 = time.monotonic()
        while True:
            response = await self.get_job_by_id(job_id)
            if response["status"] == status:
                return response
            if response["status"] in stop_statuses:
                pytest.fail(f"Status {status} cannot be reached, resp: {response}")
            await asyncio.sleep(max(interval_s, time.monotonic() - t0))
            current_time = time.monotonic() - t0
            if current_time > max_time:
                pytest.fail(f"too long: {current_time:.3f} sec; resp: {response}")
            interval_s *= 1.5

    async def delete_job(self, job_id: str, assert_success: bool = True) -> None:
        url = self._api_config.generate_job_url(job_id)
        async with self._client.delete(url, headers=self._headers) as response:
            if assert_success:
                assert (
                    response.status == HTTPNoContent.status_code
                ), await response.text()


@pytest.fixture
def jobs_client_factory(
    api: ApiConfig, client: ClientSession
) -> Iterator[Callable[[_User], JobsClient]]:
    def impl(user: _User) -> JobsClient:
        return JobsClient(api, client, headers=user.headers)

    yield impl


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
    def _factory() -> Dict[str, Any]:
        # Note: Optional fields (as "name") should not have a value here
        return {
            "container": {
                "image": "ubuntu",
                "command": "true",
                "resources": {"cpu": 0.1, "memory_mb": 16},
                "http": {"port": 1234},
            },
            "description": "test job submitted by neuro job submit",
        }

    return _factory


@pytest.fixture
async def job_submit(
    job_request_factory: Callable[[], Dict[str, Any]]
) -> Dict[str, Any]:
    return job_request_factory()
