from pathlib import PurePath
from typing import Any, Dict

import aiohttp.web
import trafaret as t
from aiohttp_security import check_authorized, check_permission
from neuro_auth_client import Permission
from yarl import URL

from platform_api.config import Config, StorageConfig
from platform_api.orchestrator import JobsService
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import ContainerVolume, JobRequest
from platform_api.user import untrusted_user

from .job_request_builder import ContainerBuilder
from .validators import (
    create_container_request_validator,
    create_job_history_validator,
    create_job_status_validator,
)


def create_job_request_validator() -> t.Trafaret:
    return t.Dict({"container": create_container_request_validator(allow_volumes=True)})


def create_job_response_validator() -> t.Trafaret:
    return t.Dict(
        {
            "id": t.String,
            # TODO (A Danshyn 10/08/18): `owner` is allowed to be a blank
            # string because initially jobs did not have such information
            # on the dev and staging envs. we may want to change this once the
            # prod env is there.
            "owner": t.String(allow_blank=True),
            # `status` is left for backward compat. the python client/cli still
            # relies on it.
            "status": create_job_status_validator(),
            t.Key("http_url", optional=True): t.String,
            "history": create_job_history_validator(),
            "container": create_container_request_validator(allow_volumes=True),
        }
    )


def convert_job_container_to_json(
    container, storage_config: StorageConfig
) -> Dict[str, Any]:
    ret = {"image": container.image, "env": container.env, "volumes": []}
    if container.command is not None:
        ret["command"] = container.command

    resources = {
        "cpu": container.resources.cpu,
        "memory_mb": container.resources.memory_mb,
    }
    if container.resources.gpu is not None:
        resources["gpu"] = container.resources.gpu
    if container.resources.shm is not None:
        resources["shm"] = container.resources.shm
    ret["resources"] = resources

    if container.http_server is not None:
        ret["http"] = {
            "port": container.http_server.port,
            "health_check_path": container.http_server.health_check_path,
        }
    for volume in container.volumes:
        ret["volumes"].append(convert_container_volume_to_json(volume, storage_config))
    return ret


def convert_container_volume_to_json(
    volume: ContainerVolume, storage_config: StorageConfig
) -> Dict[str, Any]:
    uri = str(volume.uri)
    if not uri:
        try:
            rel_dst_path = volume.dst_path.relative_to(
                storage_config.container_mount_path
            )
        except ValueError:
            rel_dst_path = PurePath()
        dst_path = PurePath("/") / rel_dst_path
        uri = str(URL(f"{storage_config.uri_scheme}:/{dst_path}"))
    return {
        "src_storage_uri": uri,
        "dst_path": str(volume.dst_path),
        "read_only": volume.read_only,
    }


def convert_job_to_job_response(
    job: Job, storage_config: StorageConfig
) -> Dict[str, Any]:
    history = job.status_history
    current_status = history.current
    response_payload = {
        "id": job.id,
        "owner": job.owner,
        "status": current_status.status,
        "history": {
            "status": current_status.status,
            "reason": current_status.reason,
            "description": current_status.description,
            "created_at": history.created_at_str,
        },
        "container": convert_job_container_to_json(
            job.request.container, storage_config
        ),
    }
    if job.has_http_server_exposed:
        response_payload["http_url"] = job.http_url
    if history.started_at:
        response_payload["history"]["started_at"] = history.started_at_str
    if history.is_finished:
        response_payload["history"]["finished_at"] = history.finished_at_str
    return response_payload


class JobsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._storage_config = config.storage

        self._job_request_validator = create_job_request_validator()
        self._job_response_validator = create_job_response_validator()
        self._bulk_jobs_response_validator = t.Dict(
            {"jobs": t.List(self._job_response_validator)}
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    def register(self, app):
        app.add_routes(
            (
                aiohttp.web.get("", self.handle_get_all),
                aiohttp.web.post("", self.create_job),
                aiohttp.web.delete("/{job_id}", self.handle_delete),
                aiohttp.web.get("/{job_id}", self.handle_get),
                aiohttp.web.get("/{job_id}/log", self.stream_log),
            )
        )

    async def create_job(self, request):
        user = await untrusted_user(request)
        permission = Permission(uri=str(user.to_job_uri()), action="write")
        await check_permission(request, permission.action, [permission])

        orig_payload = await request.json()
        request_payload = self._job_request_validator.check(orig_payload)
        container = ContainerBuilder.from_container_payload(
            request_payload["container"], storage_config=self._storage_config
        ).build()
        job_request = JobRequest.create(container)
        job, _ = await self._jobs_service.create_job(job_request, user=user)
        response_payload = convert_job_to_job_response(job, self._storage_config)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def handle_get(self, request):
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)
        response_payload = convert_job_to_job_response(job, self._storage_config)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    async def handle_get_all(self, request):
        # TODO (A Danshyn 10/04/18): we do not store user names in jobs yet,
        # therefore for now we only check whether the user is authorized
        await check_authorized(request)

        # TODO use pagination. may eventually explode with OOM.
        jobs = await self._jobs_service.get_all_jobs()
        response_payload = {
            "jobs": [
                convert_job_to_job_response(job, self._storage_config) for job in jobs
            ]
        }
        self._bulk_jobs_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    async def handle_delete(self, request):
        # TODO (A Danshyn 10/04/18): we do not store user names in jobs yet,
        # therefore for now we only check whether the user is authorized
        await check_authorized(request)

        job_id = request.match_info["job_id"]
        await self._jobs_service.delete_job(job_id)
        raise aiohttp.web.HTTPNoContent()

    async def stream_log(self, request):
        # TODO (A Danshyn 10/04/18): we do not store user names in jobs yet,
        # therefore for now we only check whether the user is authorized
        await check_authorized(request)

        job_id = request.match_info["job_id"]
        log_reader = await self._jobs_service.get_job_log_reader(job_id)
        # TODO: expose. make configurable
        chunk_size = 1024

        response = aiohttp.web.StreamResponse(status=200)
        response.content_type = "text/plain"
        response.charset = "utf-8"
        await response.prepare(request)

        async with log_reader:
            while True:
                chunk = await log_reader.read(size=chunk_size)
                if not chunk:
                    break
                await response.write(chunk)

        await response.write_eof()
        return response
