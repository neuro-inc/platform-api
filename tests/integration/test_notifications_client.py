from typing import Any, Callable, Dict

import aiohttp
import pytest
from aiohttp.web import HTTPAccepted
from neuro_auth_client.client import Quota

from .test_api import ApiConfig


@pytest.mark.asyncio
async def test_notifications_one(
    api_with_notifications: ApiConfig,
    client: aiohttp.ClientSession,
    job_request_factory: Callable[[], Dict[str, Any]],
    jobs_client: Callable[[], Any],
    regular_user_factory: Callable[..., Any],
) -> None:
    quota = Quota(total_gpu_run_time_minutes=100)
    user = await regular_user_factory(quota=quota)
    url = api_with_notifications.jobs_base_url
    job_request = job_request_factory()
    job_request["container"]["resources"]["gpu"] = 1
    async with client.post(url, headers=user.headers, json=job_request) as response:
        assert response.status == HTTPAccepted.status_code
