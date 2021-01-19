import dataclasses
import hashlib
import json
from datetime import timedelta
from typing import Any, Dict

import aiohttp.web
from neuro_auth_client import Permission, check_permissions

from platform_api.orchestrator.job import JobRecord
from platform_api.orchestrator.job_request import JobError
from platform_api.orchestrator.jobs_storage import (
    JobFilter,
    JobsStorage,
    JobsStorageException,
    JobStorageJobFoundError,
    JobStorageTransactionError,
)


class JobsStorageHttpApiHandler:
    def __init__(self, *, app: aiohttp.web.Application) -> None:
        self._app = app

    @property
    def _jobs_storage(self) -> JobsStorage:
        return self._app["jobs_storage"]

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.post("/try_create_job", self.try_create_job),
                aiohttp.web.post("/set_job", self.set_job),
                aiohttp.web.get("/get_job", self.get_job),
                aiohttp.web.get("/try_update_job", self.try_update_job_start),
                aiohttp.web.post("/try_update_job", self.try_update_job_finish),
                aiohttp.web.get("/iter_all_jobs", self.iter_all_jobs),
                aiohttp.web.get("/get_jobs_by_ids", self.get_jobs_by_ids),
                aiohttp.web.get("/get_jobs_for_deletion", self.get_jobs_for_deletion),
                aiohttp.web.get("/get_tags", self.get_tags),
            )
        )

    def _make_error_response(self, error: JobsStorageException) -> aiohttp.web.Response:
        if isinstance(error, JobStorageTransactionError):
            payload = {
                "type": "JobStorageTransactionError",
                "message": error.message,
            }
        elif isinstance(error, JobStorageJobFoundError):
            payload = {
                "type": "JobStorageJobFoundError",
                "job_name": error.job_name,
                "job_owner": error.job_owner,
                "found_job_id": error.found_job_id,
            }
        else:
            payload = {
                "type": "JobsStorageException",
            }
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code
        )

    def _make_record_version(self, job: JobRecord) -> str:
        hasher = hashlib.new("sha256")
        val = job.to_primitive()
        data = json.dumps(val, sort_keys=True)
        hasher.update(data.encode("utf-8"))
        return hasher.hexdigest()

    async def _check_permissions(self, request: aiohttp.web.Request) -> None:
        permission = Permission(uri="job://", action="write")
        await check_permissions(request, [permission])

    async def try_create_job(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        await self._check_permissions(request)
        payload = await request.json()
        job_record = JobRecord.from_primitive(payload)
        try:
            async with self._jobs_storage.try_create_job(job=job_record):
                pass
        except JobsStorageException as error:
            return self._make_error_response(error)
        raise aiohttp.web.HTTPNoContent()

    async def set_job(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        await self._check_permissions(request)
        payload = await request.json()
        job_record = JobRecord.from_primitive(payload)
        try:
            await self._jobs_storage.set_job(job=job_record)
        except JobsStorageException as error:
            return self._make_error_response(error)
        raise aiohttp.web.HTTPNoContent()

    async def get_job(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        await self._check_permissions(request)
        job_id = request.query["job_id"]
        try:
            job_record = await self._jobs_storage.get_job(job_id=job_id)
        except JobsStorageException as error:
            return self._make_error_response(error)
        except JobError as error:
            return aiohttp.web.json_response(
                {
                    "type": "JobError",
                    "message": error.args[0],
                },
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        return aiohttp.web.json_response(data=job_record.to_primitive())

    async def try_update_job_start(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        await self._check_permissions(request)
        job_id = request.query["job_id"]
        try:
            job_record = await self._jobs_storage.get_job(job_id=job_id)
        except JobsStorageException as error:
            return self._make_error_response(error)
        except JobError as error:
            return aiohttp.web.json_response(
                {
                    "type": "JobError",
                    "message": error.args[0],
                },
                status=aiohttp.web.HTTPBadRequest.status_code,
            )
        return aiohttp.web.json_response(
            data=job_record.to_primitive(),
            headers={"X-JOB-VERSION": self._make_record_version(job_record)},
        )

    async def try_update_job_finish(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        await self._check_permissions(request)
        old_version = request.headers["X-JOB-VERSION"]
        payload = await request.json()
        new_data = JobRecord.from_primitive(payload)
        try:
            async with self._jobs_storage.try_update_job(
                job_id=new_data.id
            ) as job_record:
                if old_version != self._make_record_version(job_record):
                    raise JobStorageTransactionError(
                        f"Job {{id={job_record.id}}} has changed"
                    )
                # Copy data from new_data to job_record
                for field in dataclasses.fields(job_record):
                    setattr(job_record, field.name, getattr(new_data, field.name))
        except JobsStorageException as error:
            return self._make_error_response(error)
        raise aiohttp.web.HTTPNoContent()

    async def iter_all_jobs(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        await self._check_permissions(request)
        job_filter = None
        if "job_filter" in request.query:
            job_filter = JobFilter.from_primitive(
                json.loads(request.query["job_filter"])
            )
        kwargs: Dict[str, Any] = {}
        if "reverse" in request.query:
            kwargs["reverse"] = True
        if "limit" in request.query:
            kwargs["limit"] = int(request.query["limit"])
        response = aiohttp.web.StreamResponse()
        response.headers["Content-Type"] = "application/x-ndjson"
        await response.prepare(request)
        async for job in self._jobs_storage.iter_all_jobs(job_filter, **kwargs):
            await response.write(json.dumps(job.to_primitive()).encode() + b"\n")
        return response

    async def get_jobs_by_ids(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        await self._check_permissions(request)
        job_ids = json.loads(request.query["job_ids"])
        kwargs: Dict[str, Any] = {}
        if "job_filter" in request.query:
            kwargs["job_filter"] = JobFilter.from_primitive(
                json.loads(request.query["job_filter"])
            )
        return aiohttp.web.json_response(
            data=[
                job.to_primitive()
                for job in await self._jobs_storage.get_jobs_by_ids(job_ids, **kwargs)
            ]
        )

    async def get_jobs_for_deletion(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        await self._check_permissions(request)
        delay = timedelta(seconds=float(request.query["delay_s"]))
        kwargs: Dict[str, Any] = {}
        if "cluster_name" in request.query:
            kwargs["cluster_name"] = request.query["cluster_name"]
        return aiohttp.web.json_response(
            data=[
                job.to_primitive()
                for job in await self._jobs_storage.get_jobs_for_deletion(
                    delay=delay, **kwargs
                )
            ]
        )

    async def get_tags(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        await self._check_permissions(request)
        owner = request.query["owner"]
        return aiohttp.web.json_response(data=await self._jobs_storage.get_tags(owner))
