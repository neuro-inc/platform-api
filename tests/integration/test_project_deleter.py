import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import aiohttp
import pytest
from apolo_events_client import Ack, EventType, RecvEvent, RecvEvents, StreamType, Tag
from apolo_events_client.pytest import EventsQueues

from tests.integration.auth import _User


@pytest.mark.asyncio
async def test_project_deleter(
    events_queues: EventsQueues,
    client: aiohttp.ClientSession,
    api_url: str,
    regular_user: _User,
    cluster_name: str,
) -> None:
    # Create test jobs in different projects
    test_org = "test-org"
    project_to_delete = "project-to-delete"
    project_to_keep = "project-to-keep"

    jobs_url = f"{api_url}/jobs"
    headers = regular_user.headers

    # Create a job in the project that will be deleted
    job_to_delete_payload = {
        "image": "ubuntu:20.04",
        "command": ["sleep", "3600"],
        "resources": {
            "memory": "1G",
            "shm": False,
        },
        "org_name": test_org,
        "project_name": project_to_delete,
    }

    async with client.post(
        jobs_url, headers=headers, json=job_to_delete_payload
    ) as resp:
        assert resp.status == 202, await resp.text()
        job_to_delete = await resp.json()

    # Create a job in the project that should be kept
    job_to_keep_payload = {
        "image": "ubuntu:20.04",
        "command": ["sleep", "3600"],
        "resources": {
            "memory": "1G",
            "shm": False,
        },
        "org_name": test_org,
        "project_name": project_to_keep,
    }

    async with client.post(jobs_url, headers=headers, json=job_to_keep_payload) as resp:
        assert resp.status == 202, await resp.text()
        job_to_keep = await resp.json()

    # Verify jobs were created
    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_delete, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_delete = [eval(line) for line in content.strip().split("\n") if line]

    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_keep, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_keep = [eval(line) for line in content.strip().split("\n") if line]

    assert len(jobs_to_delete) == 1
    assert jobs_to_delete[0]["id"] == job_to_delete["id"]
    assert len(jobs_to_keep) == 1
    assert jobs_to_keep[0]["id"] == job_to_keep["id"]

    # Send project-remove event
    await asyncio.wait_for(
        events_queues.outcome.put(
            RecvEvents(
                subscr_id=uuid4(),
                events=[
                    RecvEvent(
                        tag=Tag("delete-project-123"),
                        timestamp=datetime.now(tz=UTC),
                        sender="platform-admin",
                        stream=StreamType("platform-admin"),
                        event_type=EventType("project-remove"),
                        org=test_org,
                        cluster=cluster_name,
                        project=project_to_delete,
                        user="admin",
                    ),
                ],
            )
        ),
        timeout=5.0,
    )

    # Wait for event acknowledgment
    ev = await asyncio.wait_for(events_queues.income.get(), timeout=5.0)

    assert isinstance(ev, Ack)
    assert ev.events[StreamType("platform-admin")] == ["delete-project-123"]

    # Verify that jobs from deleted project are cancelled
    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_delete, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_delete_after = [
            eval(line) for line in content.strip().split("\n") if line
        ]

    assert len(jobs_to_delete_after) == 1
    assert jobs_to_delete_after[0]["status"] in ["cancelled", "failed", "succeeded"]

    # Verify that jobs from other projects remain untouched
    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_keep, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_keep_after = [
            eval(line) for line in content.strip().split("\n") if line
        ]

    assert len(jobs_to_keep_after) == 1
    assert jobs_to_keep_after[0]["id"] == job_to_keep["id"]
