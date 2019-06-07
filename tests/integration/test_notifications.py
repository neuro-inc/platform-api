from typing import Any, Callable, Dict

import aiohttp.web
import pytest

from platform_api.orchestrator.job import Quota

from .api import ApiConfig
from .notifications import NotificationsServer


class TestCannotStartJobQuotaReached:
    @pytest.mark.asyncio
    async def test_not_sent_if_quota_not_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
    ) -> None:
        quota = Quota(total_non_gpu_run_time_minutes=100)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()
        assert len(mock_notifications_server.requests) == 0

    @pytest.mark.asyncio
    async def test_sent_if_non_gpu_quota_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
    ) -> None:
        quota = Quota(total_non_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()
        assert (
            "job-cannot-start-quota-reached",
            {"user_id": user.name},
        ) in mock_notifications_server.requests

    @pytest.mark.asyncio
    async def test_sent_if_gpu_quota_reached(
        self,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        jobs_client: Callable[[], Any],
        regular_user_factory: Callable[..., Any],
        mock_notifications_server: NotificationsServer,
    ) -> None:
        quota = Quota(total_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api.jobs_base_url
        job_request = job_request_factory()
        job_request["container"]["resources"]["gpu"] = 1
        async with client.post(url, headers=user.headers, json=job_request) as response:
            await response.read()
        # Notification will be sent in graceful app shutdown
        await api.runner.close()
        assert (
            "job-cannot-start-quota-reached",
            {"user_id": user.name},
        ) in mock_notifications_server.requests
