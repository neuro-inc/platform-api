import asyncio
import logging
from datetime import UTC, datetime
from uuid import uuid4

import aiohttp
import pytest
from apolo_events_client import Ack, EventType, RecvEvent, RecvEvents, StreamType, Tag
from apolo_events_client.pytest import EventsQueues

from tests.integration.auth import _User

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_project_deleter(
    events_queues: EventsQueues,
    client: aiohttp.ClientSession,
    api_url: str,
    regular_user: _User,
    cluster_name: str,
) -> None:
    logger.info("Starting test_project_deleter")

    # Create test jobs in different projects
    test_org = "test-org"
    project_to_delete = "project-to-delete"
    project_to_keep = "project-to-keep"

    jobs_url = f"{api_url}/jobs"
    headers = regular_user.headers

    logger.info(
        "Test setup - org: %s, project_to_delete: %s, project_to_keep: %s",
        test_org,
        project_to_delete,
        project_to_keep,
    )

    # Create a job in the project that will be deleted
    logger.info("Creating job in project to delete")
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
        logger.info("Created job to delete: %s", job_to_delete["id"])

    # Create a job in the project that should be kept
    logger.info("Creating job in project to keep")
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
        logger.info("Created job to keep: %s", job_to_keep["id"])

    # Verify jobs were created
    logger.info("Verifying jobs were created - checking jobs to delete")
    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_delete, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_delete = [eval(line) for line in content.strip().split("\n") if line]
        logger.info("Found %d jobs to delete", len(jobs_to_delete))

    logger.info("Verifying jobs were created - checking jobs to keep")
    async with client.get(
        jobs_url,
        headers={**headers, "Accept": "application/x-ndjson"},
        params={"project_name": project_to_keep, "org_name": test_org},
    ) as resp:
        assert resp.status == 200
        content = await resp.text()
        jobs_to_keep = [eval(line) for line in content.strip().split("\n") if line]
        logger.info("Found %d jobs to keep", len(jobs_to_keep))

    assert len(jobs_to_delete) == 1
    assert jobs_to_delete[0]["id"] == job_to_delete["id"]
    assert len(jobs_to_keep) == 1
    assert jobs_to_keep[0]["id"] == job_to_keep["id"]
    logger.info("Job creation verification completed successfully")

    # Send project-remove event
    logger.info("Sending project-remove event for project: %s", project_to_delete)
    try:
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
        logger.info("Project-remove event sent successfully")
    except TimeoutError:
        logger.error("Timeout sending project-remove event")
        raise

    # Wait for event acknowledgment
    logger.info("Waiting for event acknowledgment")
    try:
        ev = await asyncio.wait_for(events_queues.income.get(), timeout=5.0)
        logger.info("Received event acknowledgment: %s", ev)
    except TimeoutError:
        logger.error("Timeout waiting for event acknowledgment")
        raise

    assert isinstance(ev, Ack)
    assert ev.events[StreamType("platform-admin")] == ["delete-project-123"]
    logger.info("Event acknowledgment validation passed")

    # Verify that jobs from deleted project are cancelled
    logger.info("Verifying jobs from deleted project are cancelled")
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
        logger.info("Found %d jobs after deletion event", len(jobs_to_delete_after))

    assert len(jobs_to_delete_after) == 1
    job_status = jobs_to_delete_after[0]["status"]
    logger.info("Job status after deletion event: %s", job_status)
    assert job_status in ["cancelled", "failed", "succeeded"]

    # Verify that jobs from other projects remain untouched
    logger.info("Verifying jobs from other projects remain untouched")
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
        logger.info(
            "Found %d jobs in project to keep after deletion event",
            len(jobs_to_keep_after),
        )

    assert len(jobs_to_keep_after) == 1
    assert jobs_to_keep_after[0]["id"] == job_to_keep["id"]
    logger.info("test_project_deleter completed successfully")
