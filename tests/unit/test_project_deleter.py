from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from apolo_events_client import (
    AbstractEventsClient,
    EventType,
    RecvEvent,
    StreamType,
    Tag,
)

from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import JobStatus
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.project_deleter import ProjectDeleter


@pytest.fixture
def mock_events_client() -> AbstractEventsClient:
    mock = Mock(spec=AbstractEventsClient)
    mock.subscribe_group = AsyncMock()
    return mock


@pytest.fixture
def mock_jobs_service() -> Mock:
    mock = Mock(spec=JobsService)
    mock.get_all_jobs = AsyncMock()
    mock.cancel_job = AsyncMock()
    return mock


@pytest.fixture
async def project_deleter(
    mock_events_client: AbstractEventsClient,
    mock_jobs_service: Mock,
) -> ProjectDeleter:
    return ProjectDeleter(
        events_client=mock_events_client,
        jobs_service=mock_jobs_service,
    )


def create_test_job(job_id: str = "job-123") -> Mock:
    job = Mock(spec=Job)
    job.id = job_id
    job.status = JobStatus.RUNNING
    return job


@pytest.mark.asyncio
async def test_on_admin_event_ignores_non_project_remove_events(
    project_deleter: ProjectDeleter,
    mock_jobs_service: Mock,
) -> None:
    event = RecvEvent(
        tag=Tag("test-tag-1"),
        event_type=EventType("job-start"),
        timestamp=datetime.now(UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        cluster="test-cluster",
        org="test-org",
        project="test-project",
        user="admin",
    )

    await project_deleter._on_admin_event(event)

    mock_jobs_service.get_all_jobs.assert_not_called()
    mock_jobs_service.cancel_job.assert_not_called()


@pytest.mark.asyncio
async def test_process_project_deletion_exception_is_handled(
    project_deleter: ProjectDeleter,
    mock_jobs_service: Mock,
) -> None:
    mock_jobs_service.get_all_jobs.side_effect = Exception("Database error")

    event = RecvEvent(
        tag=Tag("test-tag"),
        event_type=EventType("project-remove"),
        timestamp=datetime.now(UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        cluster="test-cluster",
        org="test-org",
        project="test-project",
        user="admin",
    )

    # Should not raise an exception
    await project_deleter._on_admin_event(event)

    mock_jobs_service.get_all_jobs.assert_called_once()
    mock_jobs_service.cancel_job.assert_not_called()


@pytest.mark.asyncio
async def test_individual_job_cancellation_exception_continues_processing(
    project_deleter: ProjectDeleter,
    mock_jobs_service: Mock,
) -> None:
    job1 = create_test_job("job-1")
    job2 = create_test_job("job-2")
    job3 = create_test_job("job-3")

    mock_jobs_service.get_all_jobs.return_value = [job1, job2, job3]
    mock_jobs_service.cancel_job.side_effect = [
        None,  # job1 succeeds
        Exception("Cancellation failed"),  # job2 fails
        None,  # job3 succeeds
    ]

    event = RecvEvent(
        tag=Tag("test-tag"),
        event_type=EventType("project-remove"),
        timestamp=datetime.now(UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        cluster="test-cluster",
        org="test-org",
        project="test-project",
        user="admin",
    )

    # Should not raise an exception and should continue processing all jobs
    await project_deleter._on_admin_event(event)

    assert mock_jobs_service.cancel_job.call_count == 3
    mock_jobs_service.cancel_job.assert_any_call("job-1")
    mock_jobs_service.cancel_job.assert_any_call("job-2")
    mock_jobs_service.cancel_job.assert_any_call("job-3")


@pytest.mark.asyncio
async def test_successful_project_deletion_processes_all_jobs(
    project_deleter: ProjectDeleter,
    mock_jobs_service: Mock,
) -> None:
    job1 = create_test_job("job-1")
    job2 = create_test_job("job-2")

    mock_jobs_service.get_all_jobs.return_value = [job1, job2]
    mock_jobs_service.cancel_job.return_value = None

    event = RecvEvent(
        tag=Tag("test-tag"),
        event_type=EventType("project-remove"),
        timestamp=datetime.now(UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        cluster="test-cluster",
        org="test-org",
        project="test-project",
        user="admin",
    )

    await project_deleter._on_admin_event(event)

    mock_jobs_service.get_all_jobs.assert_called_once()
    assert mock_jobs_service.cancel_job.call_count == 2
    mock_jobs_service.cancel_job.assert_any_call("job-1")
    mock_jobs_service.cancel_job.assert_any_call("job-2")
