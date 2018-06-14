import asyncio
import pytest


from platform_api.orchestrator import JobsStatusPooling, JobRequest, JobStatus, InMemoryJobsService


@pytest.mark.asyncio
async def test_test(mock_orchestrator, event_loop):
    jobs_service = InMemoryJobsService(orchestrator=mock_orchestrator)
    jobs_status_pooling = JobsStatusPooling(jobs_service=jobs_service, loop=event_loop, interval_s=1)
    await jobs_status_pooling.start()

    await jobs_service.create_job(JobRequest.create(container=None))
    await jobs_service.create_job(JobRequest.create(container=None))

    all_jobs = await jobs_service.get_all_jobs()
    assert all(x['status'].value == JobStatus.PENDING for x in all_jobs)

    mock_orchestrator.update_status_to_return(JobStatus.SUCCEEDED)
    await asyncio.sleep(2)

    assert all(x['status'].value == JobStatus.SUCCEEDED for x in all_jobs)
    await jobs_status_pooling.stop()