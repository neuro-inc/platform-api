import logging
from datetime import timedelta

import aiohttp.web
from aiohttp.web_response import json_response
from neuro_auth_client import Permission, check_permissions

from platform_api.config import Config
from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobStorageTransactionError


logger = logging.getLogger(__name__)


class PollerHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.get("/data", self.handle_get),
                aiohttp.web.put("/update", self.handle_update),
            )
        )

    async def handle_get(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        try:
            cluster_name = request.query["cluster_name"]
        except KeyError:
            raise aiohttp.web.HTTPBadRequest(
                text="GET parameter 'cluster_name' is required"
            )
        permission = Permission(uri=f"job://{cluster_name}", action="read")
        await check_permissions(request, [permission])
        delay = None
        if "deletion_delay_s" in request.query:
            delay = timedelta(seconds=float(request.query["deletion_delay_s"]))

        data = await self._jobs_service.get_poller_data(cluster_name, delay)
        return json_response(
            {
                "unfinished_jobs": [
                    record.to_primitive(include_version=True)
                    for record in data.unfinished_jobs
                ],
                "for_deletion_jobs": [
                    record.to_primitive(include_version=True)
                    for record in data.for_deletion_jobs
                ],
                "resource_pool_types": [
                    rpt.to_primitive() for rpt in data.resource_pool_types
                ],
            }
        )

    async def handle_update(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        payload = await request.json()
        # TODO: probably we have to add some validation here?
        record = JobRecord.from_primitive(payload)
        old_version = payload["record_version"]
        permission = Permission(uri=str(record.to_uri()), action="write")
        await check_permissions(request, [permission])
        try:
            await self._jobs_service.update_job(record, old_version)
        except JobStorageTransactionError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPConflict.status_code
            )
        else:
            raise aiohttp.web.HTTPNoContent()
