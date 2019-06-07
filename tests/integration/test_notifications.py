import asyncio
from typing import Any, Callable, Dict, Tuple

import aiohttp.web
import pytest

from platform_api.orchestrator.job import Quota

from .api import ApiConfig
from .conftest import ApiAddress


async def _wait_for_notifications() -> None:
    """
    TODO find better way than sleep
    :return:
    """
    await asyncio.sleep(0.2)


class TestCannotStartJobQuotaReached:
    @pytest.mark.asyncio
    async def test_not_sent_if_quota_not_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: Tuple[ApiAddress, aiohttp.web.Application],
    ) -> None:
        notifications_app = mock_notifications_server[1]
        quota = Quota(total_non_gpu_run_time_minutes=100)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        await _wait_for_notifications()
        assert notifications_app["requests"] == []

    @pytest.mark.asyncio
    async def test_sent_if_non_gpu_quota_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: Tuple[ApiAddress, aiohttp.web.Application],
    ) -> None:
        notifications_app = mock_notifications_server[1]
        quota = Quota(total_non_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        await _wait_for_notifications()
        assert (
            "job-cannot-start-quota-reached",
            {"user_id": user.name},
        ) in notifications_app["requests"]

    @pytest.mark.asyncio
    async def test_sent_if_gpu_quota_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: Tuple[ApiAddress, aiohttp.web.Application],
    ) -> None:
        notifications_app = mock_notifications_server[1]
        quota = Quota(total_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        job_request["container"]["resources"]["gpu"] = 1
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        await _wait_for_notifications()
        assert (
            "job-cannot-start-quota-reached",
            {"user_id": user.name},
        ) in notifications_app["requests"]
