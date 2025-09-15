import logging
from typing import Self

from apolo_events_client import (
    AbstractEventsClient,
    EventType,
    RecvEvent,
    StreamType,
)

from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage.base import JobFilter

logger = logging.getLogger(__name__)


class ProjectDeleter:
    ADMIN_STREAM = StreamType("platform-admin")
    PROJECT_REMOVE = EventType("project-remove")

    def __init__(
        self,
        events_client: AbstractEventsClient,
        jobs_service: JobsService,
    ) -> None:
        self._events_client = events_client
        self._jobs_service = jobs_service

    async def __aenter__(self) -> Self:  # pragma: no cover
        await self._events_client.subscribe_group(
            self.ADMIN_STREAM, self._on_admin_event, auto_ack=True
        )
        return self

    async def __aexit__(
        self, exc_typ: object, exc_val: object, exc_tb: object
    ) -> None:  # pragma: no cover
        pass

    async def _on_admin_event(self, ev: RecvEvent) -> None:
        if ev.event_type != self.PROJECT_REMOVE:
            return

        try:
            await self._process_project_deletion(ev)
        except Exception:
            logger.exception("Error processing project deletion")

    async def _process_project_deletion(self, ev: RecvEvent) -> None:
        cluster = ev.cluster
        assert cluster is not None
        org = ev.org
        assert org is not None
        project = ev.project
        assert project is not None

        logger.info(
            "Processing project deletion: cluster=%s, org=%s, project=%s",
            cluster,
            org,
            project,
        )

        # Create filter to find all jobs for this project
        job_filter = JobFilter(
            clusters={cluster: {org: {project: set()}}},
            orgs={org},
            projects={project},
        )

        # Get all jobs for this project
        jobs = await self._jobs_service.get_all_jobs(job_filter=job_filter)

        logger.info(
            "Found %d jobs to delete for project %s/%s",
            len(jobs),
            org,
            project,
        )

        # Cancel/delete each job
        for job in jobs:
            try:
                logger.info(
                    "Cancelling job %s (status: %s) for project deletion",
                    job.id,
                    job.status,
                )
                await self._jobs_service.cancel_job(job.id)
            except Exception:
                logger.exception(
                    "Failed to cancel job %s for project %s/%s",
                    job.id,
                    org,
                    project,
                )
