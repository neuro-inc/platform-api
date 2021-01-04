import time

import aiohttp
import pytest

from .conftest import PlatformConfig, _User


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_basic_command(
    api_config: PlatformConfig, alice: _User, client: aiohttp.ClientSession
) -> None:
    job_request_payload = {
        "container": {
            "image": "ubuntu",
            "command": "true",
            "resources": {"cpu": 0.1, "memory_mb": 16},
        },
    }
    headers = {"Authorization": f"Bearer {alice.token}"}
    response = await client.post(
        api_config.jobs_url, headers=headers, json=job_request_payload
    )
    assert response.status == 202, await response.text()
    job_payload = await response.json()
    job_id = job_payload["id"]
    job_url = f"{api_config.jobs_url}/{job_id}"

    for i in range(30):
        response = await client.get(job_url, headers=headers)
        assert response.status == 200, await response.text()
        jobs_payload = await response.json()
        assert jobs_payload
        status_name = jobs_payload["status"]
        if status_name == "succeeded":
            break
        if status_name == "failed":
            pytest.fail(f"Job failed: {jobs_payload}")
        # COMMENT(adavydow): should we fail if we did not succeeded in 30 seconds?
        time.sleep(1)
