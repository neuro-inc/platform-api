from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import aiohttp.web
import pytest
from aiohttp.web_response import json_response
from yarl import URL

from platform_api.orchestrator.job import JobStatusItem, JobStatusReason
from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.jobs_poller import HttpJobsPollerApi


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int

    @property
    def url(self) -> URL:
        return URL(f"http://{self.host}:{self.port}")


@asynccontextmanager
async def create_local_app_server(
    app: aiohttp.web.Application, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    runner = aiohttp.web.AppRunner(app)
    try:
        await runner.setup()
        api_address = ApiAddress("0.0.0.0", port)
        site = aiohttp.web.TCPSite(runner, api_address.host, api_address.port)
        await site.start()
        yield api_address
    finally:
        await runner.shutdown()
        await runner.cleanup()


class JobsHttpApi:
    def __init__(self) -> None:
        self.address: Optional[ApiAddress] = None
        self.last_url_tail: Optional[str] = None
        self.last_query: Optional[Any] = None
        self.last_payload: Optional[Mapping[str, Any]] = None
        self.to_return: Any = []


@pytest.fixture
async def jobs_api_mock() -> AsyncIterator[JobsHttpApi]:
    control_obj = JobsHttpApi()

    app = aiohttp.web.Application()

    async def handle_any(request: aiohttp.web.Request) -> aiohttp.web.Response:
        control_obj.last_url_tail = request.match_info["tail"]
        control_obj.last_query = request.query
        if request.method not in ("GET", "HEAD"):
            control_obj.last_payload = await request.json()
        return json_response(control_obj.to_return)

    app.add_routes(
        [
            aiohttp.web.get("/api/v1/{tail:.*}", handle_any),
            aiohttp.web.post("/api/v1/{tail:.*}", handle_any),
            aiohttp.web.put("/api/v1/{tail:.*}", handle_any),
        ]
    )

    async with create_local_app_server(app) as address:
        control_obj.address = address
        yield control_obj


@pytest.fixture
async def http_jobs_poller_api(
    jobs_api_mock: JobsHttpApi,
) -> AsyncIterator[HttpJobsPollerApi]:
    assert jobs_api_mock.address
    async with HttpJobsPollerApi(
        token="test_token",
        url=jobs_api_mock.address.url / "api/v1",
        cluster_name="default",
    ) as api:
        yield api


class TestHttpJobsStorage:
    @pytest.mark.asyncio
    async def test_get_unfinished(
        self,
        http_jobs_poller_api: HttpJobsPollerApi,
        jobs_api_mock: JobsHttpApi,
    ) -> None:
        jobs_api_mock.to_return = {"jobs": []}
        jobs = await http_jobs_poller_api.get_unfinished_jobs()
        assert jobs == []
        assert jobs_api_mock.last_url_tail == "jobs"
        assert jobs_api_mock.last_query
        assert set(JobStatus.active_values()) == set(
            jobs_api_mock.last_query.getall("status")
        )
        assert jobs_api_mock.last_query["cluster_name"] == "default"

    @pytest.mark.asyncio
    async def test_get_for_deletion(
        self,
        http_jobs_poller_api: HttpJobsPollerApi,
        jobs_api_mock: JobsHttpApi,
    ) -> None:
        jobs_api_mock.to_return = {"jobs": []}
        jobs = await http_jobs_poller_api.get_jobs_for_deletion(delay=timedelta())
        assert jobs == []
        assert jobs_api_mock.last_url_tail == "jobs"
        assert jobs_api_mock.last_query
        assert jobs_api_mock.last_query["materialized"] == "true"
        assert set(JobStatus.finished_values()) == set(
            jobs_api_mock.last_query.getall("status")
        )
        assert jobs_api_mock.last_query["cluster_name"] == "default"

    @pytest.mark.asyncio
    async def test_push_status(
        self,
        http_jobs_poller_api: HttpJobsPollerApi,
        jobs_api_mock: JobsHttpApi,
    ) -> None:
        status_item = JobStatusItem(
            status=JobStatus.FAILED,
            transition_time=datetime.now(),
            reason=JobStatusReason.CLUSTER_SCALE_UP_FAILED,
            description="test description",
            exit_code=222,
        )
        await http_jobs_poller_api.push_status("job-id", status_item)
        assert jobs_api_mock.last_url_tail == "jobs/job-id/status"
        assert jobs_api_mock.last_payload
        assert jobs_api_mock.last_payload["status"] == status_item.status
        assert jobs_api_mock.last_payload["reason"] == status_item.reason
        assert jobs_api_mock.last_payload["description"] == status_item.description
        assert jobs_api_mock.last_payload["exit_code"] == status_item.exit_code

    @pytest.mark.asyncio
    async def test_set_materialized(
        self,
        http_jobs_poller_api: HttpJobsPollerApi,
        jobs_api_mock: JobsHttpApi,
    ) -> None:
        await http_jobs_poller_api.set_materialized("job-id", True)
        assert jobs_api_mock.last_url_tail == "jobs/job-id/materialized"
        assert jobs_api_mock.last_payload == {"materialized": True}
