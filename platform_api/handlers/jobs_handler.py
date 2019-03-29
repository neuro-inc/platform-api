import asyncio
import logging
from pathlib import PurePath
from typing import Any, Dict, List, Sequence, Set

import aiohttp.web
import trafaret as t
from aiohttp_security import check_authorized, check_permission
from multidict import MultiDictProxy
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.client import ClientSubTreeViewRoot
from yarl import URL

from platform_api.config import Config, RegistryConfig, StorageConfig
from platform_api.log import log_debug_time
from platform_api.orchestrator import JobsService, Orchestrator
from platform_api.orchestrator.job import Job, JobStats
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolume,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_storage import JobFilter
from platform_api.resource import GPUModel
from platform_api.user import User, authorized_user, untrusted_user

from .job_request_builder import ContainerBuilder
from .validators import (
    create_container_request_validator,
    create_container_response_validator,
    create_job_history_validator,
    create_job_name_validator,
    create_job_status_validator,
)


logger = logging.getLogger(__name__)


def create_job_request_validator(
    *, allowed_gpu_models: Sequence[GPUModel]
) -> t.Trafaret:
    return t.Dict(
        {
            "container": create_container_request_validator(
                allow_volumes=True, allowed_gpu_models=allowed_gpu_models
            ),
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("is_preemptible", optional=True, default=False): t.Bool,
        }
    )


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
            t.Key("ssh_server", optional=True): t.String,
            "ssh_auth_server": t.String,
            "history": create_job_history_validator(),
            "container": create_container_response_validator(),
            "is_preemptible": t.Bool,
            t.Key("internal_hostname", optional=True): t.String,
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
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
    if container.resources.gpu_model_id:
        resources["gpu_model"] = container.resources.gpu_model_id
    if container.resources.shm is not None:
        resources["shm"] = container.resources.shm
    ret["resources"] = resources

    if container.http_server is not None:
        ret["http"] = {
            "port": container.http_server.port,
            "health_check_path": container.http_server.health_check_path,
            "requires_auth": container.http_server.requires_auth,
        }
    if container.ssh_server is not None:
        ret["ssh"] = {"port": container.ssh_server.port}
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
        "ssh_auth_server": job.ssh_auth_server,
        "is_preemptible": job.is_preemptible,
    }
    if job.name:
        response_payload["name"] = job.name
    if job.description:
        response_payload["description"] = job.description
    if job.has_http_server_exposed:
        response_payload["http_url"] = job.http_url
    if job.has_ssh_server_exposed:
        response_payload["ssh_server"] = job.ssh_server
    if job.internal_hostname:
        response_payload["internal_hostname"] = job.internal_hostname
    if history.started_at:
        response_payload["history"]["started_at"] = history.started_at_str
    if history.is_finished:
        response_payload["history"]["finished_at"] = history.finished_at_str
    return response_payload


def infer_permissions_from_container(
    user: User, container: Container, registry_config: RegistryConfig
) -> List[Permission]:
    permissions = [Permission(uri=str(user.to_job_uri()), action="write")]
    if container.belongs_to_registry(registry_config):
        permissions.append(
            Permission(uri=str(container.to_image_uri(registry_config)), action="read")
        )
    for volume in container.volumes:
        action = "read" if volume.read_only else "write"
        permission = Permission(uri=str(volume.uri), action=action)
        permissions.append(permission)
    return permissions


class JobsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._storage_config = config.storage

        self._job_name_validator = create_job_name_validator()
        self._job_response_validator = create_job_response_validator()
        self._bulk_jobs_response_validator = t.Dict(
            {"jobs": t.List(self._job_response_validator)}
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _orchestrator(self) -> Orchestrator:
        return self._app["orchestrator"]

    @property
    def _auth_client(self) -> AuthClient:
        return self._app["auth_client"]

    def register(self, app):
        app.add_routes(
            (
                aiohttp.web.get("", self.handle_get_all),
                aiohttp.web.post("", self.create_job),
                aiohttp.web.delete("/{job_id}", self.handle_delete),
                aiohttp.web.get("/{job_id}", self.handle_get),
                aiohttp.web.get("/{job_id}/log", self.stream_log),
                aiohttp.web.get("/{job_id}/top", self.stream_top),
            )
        )

    async def _create_job_request_validator(self) -> t.Trafaret:
        gpu_models = await self._orchestrator.get_available_gpu_models()
        return create_job_request_validator(allowed_gpu_models=gpu_models)

    async def create_job(self, request):
        user = await authorized_user(request)

        orig_payload = await request.json()

        job_request_validator = await self._create_job_request_validator()
        request_payload = job_request_validator.check(orig_payload)

        container = ContainerBuilder.from_container_payload(
            request_payload["container"], storage_config=self._storage_config
        ).build()

        permissions = infer_permissions_from_container(
            user, container, self._config.registry
        )
        logger.info("Checking whether %r has %r", user, permissions)
        await check_permission(request, permissions[0].action, permissions)

        name = request_payload.get("name")
        description = request_payload.get("description")
        is_preemptible = request_payload["is_preemptible"]
        job_request = JobRequest.create(container, description)
        job, _ = await self._jobs_service.create_job(
            job_request, user=user, job_name=name, is_preemptible=is_preemptible
        )
        response_payload = convert_job_to_job_response(job, self._storage_config)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def handle_get(self, request):
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        response_payload = convert_job_to_job_response(job, self._storage_config)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    def _build_job_filter(
        self, query: MultiDictProxy, tree: ClientSubTreeViewRoot, owner: str
    ) -> JobFilter:
        # validating query parameters first
        job_name = None
        if "name" in query:
            job_name = self._job_name_validator.check(query["name"])
        statuses = (
            {JobStatus(s) for s in query.getall("status")}
            if "status" in query
            else set()
        )
        owners = infer_job_owners_filter_from_access_tree(tree)
        # TODO: intersect owners with the ones that come in query

        if job_name and not owners:
            # TODO: this is an edge case when a user who has access to all
            # jobs (job:) tries to filter them by name. not yet supported
            # by JobsStorage.
            owners.add(owner)

        return JobFilter(statuses=statuses, owners=owners, name=job_name)

    async def handle_get_all(self, request):
        # TODO (A Danshyn 10/08/18): remove once
        # AuthClient.get_permissions_tree accepts the token param
        await check_authorized(request)
        user = await untrusted_user(request)

        with log_debug_time(f"Retrieved job access tree for user '{user.name}'"):
            tree = await self._auth_client.get_permissions_tree(user.name, "job:")
        try:
            job_filter = self._build_job_filter(request.query, tree, user.name)
            with log_debug_time(f"Read jobs with {job_filter}"):
                jobs = await self._jobs_service.get_all_jobs(job_filter)
            with log_debug_time("Filtered jobs"):
                jobs = filter_jobs_with_access_tree(jobs, tree)
        except JobFilterException:
            jobs = []

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
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="write")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        await self._jobs_service.delete_job(job_id)
        raise aiohttp.web.HTTPNoContent()

    async def stream_log(self, request):
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        log_reader = await self._jobs_service.get_job_log_reader(job_id)
        # TODO: expose. make configurable
        chunk_size = 1024

        response = aiohttp.web.StreamResponse(status=200)
        response.enable_chunked_encoding()
        response.enable_compression(aiohttp.web.ContentCoding.identity)
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

    async def stream_top(self, request):
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        logger.info("Websocket connection starting")
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(request)
        logger.info("Websocket connection ready")

        # TODO (truskovskiyk 09/12/18) remove CancelledError
        # https://github.com/aio-libs/aiohttp/issues/3443

        # TODO expose configuration
        sleep_timeout = 1

        telemetry = await self._jobs_service.get_job_telemetry(job.id)

        async with telemetry:

            try:
                while True:
                    # client close connection
                    if request.transport.is_closing():
                        break

                    job = await self._jobs_service.get_job(job_id)

                    if job.is_running:
                        job_stats = await telemetry.get_latest_stats()
                        if job_stats:
                            message = self._convert_job_stats_to_ws_message(job_stats)
                            await ws.send_json(message)

                    if job.is_finished:
                        await ws.close()
                        break

                    await asyncio.sleep(sleep_timeout)

            except asyncio.CancelledError as ex:
                logger.info(f"got cancelled error {ex}")

        return ws

    def _convert_job_stats_to_ws_message(self, job_stats: JobStats) -> Dict[str, Any]:
        message = {
            "cpu": job_stats.cpu,
            "memory": job_stats.memory,
            "timestamp": job_stats.timestamp,
        }
        if job_stats.gpu_duty_cycle is not None:
            message["gpu_duty_cycle"] = job_stats.gpu_duty_cycle
        if job_stats.gpu_memory is not None:
            message["gpu_memory"] = job_stats.gpu_memory
        return message


class JobFilterException(ValueError):
    pass


def infer_job_owners_filter_from_access_tree(tree: ClientSubTreeViewRoot) -> Set[str]:
    owners: Set[str] = set()

    if tree.sub_tree.action == "deny":
        # no job resources whatsoever
        raise JobFilterException("no jobs")

    if tree.sub_tree.action != "list":
        # read access to all jobs = job:
        return owners

    for owner, sub_tree in tree.sub_tree.children.items():
        if sub_tree.action == "deny":
            continue

        if sub_tree.action == "list":
            # specific ids
            shared_job_ids = [
                job_id
                for job_id, job_sub_tree in sub_tree.children.items()
                if job_sub_tree.action not in ("deny", "list")
            ]
            if shared_job_ids:
                owners.add(owner)
            continue

        # read/write/manage access to all owner's jobs = job://owner
        owners.add(owner)

    return owners


def filter_jobs_with_access_tree(
    jobs: List[Job], tree: ClientSubTreeViewRoot
) -> List[Job]:
    owners_shared_all_jobs: Set = set()
    shared_job_ids: Set = set()

    if tree.sub_tree.action == "deny":
        # no job resources whatsoever
        return []

    if tree.sub_tree.action != "list":
        # read access to all jobs = job:
        return jobs

    for owner, sub_tree in tree.sub_tree.children.items():
        if sub_tree.action == "deny":
            continue

        if sub_tree.action == "list":
            # specific ids
            shared_job_ids.update(
                job_id
                for job_id, job_sub_tree in sub_tree.children.items()
                if job_sub_tree.action not in ("deny", "list")
            )
            continue

        # read/write/manage access to all owner's jobs = job://owner
        owners_shared_all_jobs.add(owner)

    return [
        job
        for job in jobs
        if job.owner in owners_shared_all_jobs or job.id in shared_job_ids
    ]
