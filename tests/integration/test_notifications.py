from typing import Any, Callable, Dict

import aiohttp
import pytest
from aiohttp.web import HTTPBadRequest
from neuro_auth_client.client import Quota

from .test_api import ApiConfig, client, job_request_factory


@pytest.mark.asyncio
async def test_one(
    notifications_server,
    api_with_oauth: ApiConfig,
    client: aiohttp.ClientSession,
    job_request_factory: Callable[[], Dict[str, Any]],
    regular_user_factory: Callable[..., Any],
):
    # import asyncio
    # while True:
    #     await asyncio.sleep(10)
    quota = Quota(total_non_gpu_run_time_minutes=0)
    user = await regular_user_factory(quota=quota)
    url = api_with_oauth.jobs_base_url
    job_request = job_request_factory()
    async with client.post(url, headers=user.headers, json=job_request) as response:
        assert response.status == HTTPBadRequest.status_code
        data = await response.json()
        assert data == {"error": f"non-GPU quota exceeded for user '{user.name}'"}
