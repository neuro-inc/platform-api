import asyncio
from typing import Any, Callable, Dict, List

import aiohttp
import pytest
from aiohttp.web import HTTPBadRequest
from neuro_auth_client.client import Quota

from .api import ApiConfig
from .notifications import Notification


class TestUserNotifications:
    @pytest.mark.asyncio
    async def test_cannot_start_job_quota_reached(
        self,
        api_with_notifications: ApiConfig,
        client: aiohttp.ClientSession,
        job_request_factory: Callable[[], Dict[str, Any]],
        regular_user_factory: Callable[..., Any],
        received_notifications: List[Any],
    ) -> None:
        quota = Quota(total_gpu_run_time_minutes=0)
        user = await regular_user_factory(quota=quota)
        url = api_with_notifications.jobs_base_url
        job_request = job_request_factory()
        job_request["container"]["resources"]["gpu"] = 1

        async with client.post(url, headers=user.headers, json=job_request) as response:
            assert response.status == HTTPBadRequest.status_code

        await asyncio.sleep(0.2)  # notification will be delivered with some timeout
        assert received_notifications == [
            Notification(
                type="job-cannot-start-quota-reached", payload={"user_id": user.name}
            )
        ]
