import asyncio
import logging
from dataclasses import dataclass, replace
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence, Set

import aiohttp.web
import trafaret as t
from aiohttp_security import check_authorized, check_permission, permits
from multidict import MultiDictProxy
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.client import ClientSubTreeViewRoot
from yarl import URL

from platform_api.config import Config, RegistryConfig, StorageConfig
from platform_api.log import log_debug_time
from platform_api.orchestrator.job import Job, JobStats
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolume,
    JobError,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_service import JobsService
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
    create_user_name_validator,
    sanitize_dns_name,
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
            t.Key("http_url_named", optional=True): t.String,
            "ssh_server": t.String,
            "ssh_auth_server": t.String,  # deprecated
            "history": create_job_history_validator(),
            "container": create_container_response_validator(),
            "is_preemptible": t.Bool,
            t.Key("internal_hostname", optional=True): t.String,
            t.Key("name", optional=True): create_job_name_validator(max_length=None),
            t.Key("description", optional=True): t.String,
        }
    )


def convert_job_container_to_json(
    container: Container, storage_config: StorageConfig
) -> Dict[str, Any]:
    ret: Dict[str, Any] = {
        "image": container.image,
        "env": container.env,
        "volumes": [],
    }
    if container.command is not None:
        ret["command"] = container.command

    resources: Dict[str, Any] = {
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


def convert_job_to_job_response(job: Job) -> Dict[str, Any]:
    history = job.status_history
    current_status = history.current
    response_payload: Dict[str, Any] = {
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
            job.request.container, job.storage_config
        ),
        "ssh_server": job.ssh_server,
        "ssh_auth_server": job.ssh_server,  # deprecated
        "is_preemptible": job.is_preemptible,
    }
    if job.name:
        response_payload["name"] = job.name
    if job.description:
        response_payload["description"] = job.description
    if job.has_http_server_exposed:
        response_payload["http_url"] = job.http_url
        if job.http_url_named:
            # TEMPORARY FIX (ayushkovskiy, May 10): Too long DNS label (longer than 63
            # chars) is invalid, therefore we don't send it back to user (issue #642)
            http_url_named_sanitized = sanitize_dns_name(job.http_url_named)
            if http_url_named_sanitized is not None:
                response_payload["http_url_named"] = http_url_named_sanitized
    if job.internal_hostname:
        response_payload["internal_hostname"] = job.internal_hostname
    if history.started_at:
        response_payload["history"]["started_at"] = history.started_at_str
    if history.is_finished:
        response_payload["history"]["finished_at"] = history.finished_at_str
    if current_status.exit_code is not None:
        response_payload["history"]["exit_code"] = current_status.exit_code
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

        self._job_base_domain = config.orchestrator.jobs_domain_name_template.partition(
            "."
        )[2]
        self._job_filter_factory = JobFilterFactory()
        self._job_response_validator = create_job_response_validator()
        self._bulk_jobs_response_validator = t.Dict(
            {"jobs": t.List(self._job_response_validator)}
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _auth_client(self) -> AuthClient:
        return self._app["auth_client"]

    def register(self, app: aiohttp.web.Application) -> None:
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

    async def _create_job_request_validator(self, user: User) -> t.Trafaret:
        gpu_models = await self._jobs_service.get_available_gpu_models(user)
        return create_job_request_validator(allowed_gpu_models=gpu_models)

    async def create_job(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        user = await authorized_user(request)

        orig_payload = await request.json()

        job_request_validator = await self._create_job_request_validator(user)
        request_payload = job_request_validator.check(orig_payload)

        cluster_config = await self._jobs_service.get_cluster_config(user)

        container = ContainerBuilder.from_container_payload(
            request_payload["container"], storage_config=cluster_config.storage
        ).build()

        permissions = infer_permissions_from_container(
            user, container, cluster_config.registry
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
        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def handle_get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="read")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    async def handle_get_all(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        # TODO (A Danshyn 10/08/18): remove once
        # AuthClient.get_permissions_tree accepts the token param
        await check_authorized(request)
        user = await untrusted_user(request)

        filter: Optional[JobFilter] = None
        jobs: List[Job] = []

        hostname = request.query.get("hostname")
        if hostname is None:
            filter = self._job_filter_factory.create_from_query(request.query)
        else:
            if (
                "name" in request.query
                or "owner" in request.query
                or "status" in request.query
            ):
                raise ValueError("Invalid request")

            label, _, base_domain = hostname.partition(".")
            if base_domain == self._job_base_domain:
                filter = self._job_filter_factory.create_from_label(label)
                if filter is None:
                    try:
                        job = await self._jobs_service.get_job(label)
                    except JobError:
                        pass
                    else:
                        permission = Permission(uri=str(job.to_uri()), action="read")
                        logger.info("Checking whether %r has %r", user, permission)
                        if await permits(request, permission.action, [permission]):
                            jobs.append(job)

        if filter is not None:
            with log_debug_time(f"Retrieved job access tree for user '{user.name}'"):
                tree = await self._auth_client.get_permissions_tree(user.name, "job:")

            try:
                bulk_job_filter = BulkJobFilterBuilder(
                    query_filter=filter, access_tree=tree
                ).build()

                if bulk_job_filter.bulk_filter:
                    with log_debug_time(
                        f"Read bulk jobs with {bulk_job_filter.bulk_filter}"
                    ):
                        jobs.extend(
                            await self._jobs_service.get_all_jobs(
                                bulk_job_filter.bulk_filter
                            )
                        )

                if bulk_job_filter.shared_ids:
                    with log_debug_time(
                        f"Read shared jobs with {bulk_job_filter.shared_ids_filter}"
                    ):
                        jobs.extend(
                            await self._jobs_service.get_jobs_by_ids(
                                bulk_job_filter.shared_ids,
                                job_filter=bulk_job_filter.shared_ids_filter,
                            )
                        )
            except JobFilterException:
                pass

        response_payload = {"jobs": [convert_job_to_job_response(job) for job in jobs]}
        self._bulk_jobs_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    async def handle_delete(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="write")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        await self._jobs_service.delete_job(job_id)
        raise aiohttp.web.HTTPNoContent()

    async def stream_log(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
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

    async def stream_top(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
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
                    assert request.transport is not None
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


class JobFilterFactory:
    def __init__(self) -> None:
        self._job_name_validator = create_job_name_validator()
        self._user_name_validator = create_user_name_validator()

    def create_from_query(self, query: MultiDictProxy) -> JobFilter:  # type: ignore
        job_name = self._job_name_validator.check(query.get("name"))
        statuses = {JobStatus(s) for s in query.getall("status", [])}
        owners = {
            self._user_name_validator.check(owner)
            for owner in query.getall("owner", [])
        }
        return JobFilter(statuses=statuses, owners=owners, name=job_name)

    def create_from_label(self, label: str) -> Optional[JobFilter]:
        job_name, _, owner = label.partition("--")
        if not owner:
            return None
        job_name = self._job_name_validator.check(job_name)
        owner = self._user_name_validator.check(owner)
        if not job_name or not owner:
            return None
        return JobFilter(owners={owner}, name=job_name)


@dataclass(frozen=True)
class BulkJobFilter:
    bulk_filter: Optional[JobFilter]

    shared_ids: Set[str]
    shared_ids_filter: Optional[JobFilter]


class BulkJobFilterBuilder:
    def __init__(
        self, query_filter: JobFilter, access_tree: ClientSubTreeViewRoot
    ) -> None:
        self._query_filter = query_filter
        self._access_tree = access_tree

        self._has_access_to_all: bool = False
        self._owners_shared_all: Set[str] = set()
        self._shared_ids: Set[str] = set()

    def build(self) -> BulkJobFilter:
        self._traverse_access_tree()
        bulk_filter = self._create_bulk_filter()
        shared_ids_filter = self._query_filter if self._shared_ids else None
        return BulkJobFilter(
            bulk_filter=bulk_filter,
            shared_ids=self._shared_ids,
            shared_ids_filter=shared_ids_filter,
        )

    def _traverse_access_tree(self) -> None:
        tree = self._access_tree

        owners_shared_all: Set[str] = set()
        shared_ids: Set[str] = set()

        if tree.sub_tree.action == "deny":
            # no job resources whatsoever
            raise JobFilterException("no jobs")

        if tree.sub_tree.action != "list":
            # read access to all jobs = job:
            self._has_access_to_all = True
            return

        for owner, sub_tree in tree.sub_tree.children.items():
            if sub_tree.action == "deny":
                continue

            if self._query_filter.owners and owner not in self._query_filter.owners:
                # skipping owners
                continue

            if sub_tree.action == "list":
                # specific ids
                shared_ids.update(
                    job_id
                    for job_id, job_sub_tree in sub_tree.children.items()
                    if job_sub_tree.action not in ("deny", "list")
                )
            else:
                # read/write/manage access to all owner's jobs = job://owner
                owners_shared_all.add(owner)

        if not owners_shared_all and not shared_ids:
            # no job resources whatsoever
            raise JobFilterException("no jobs")

        self._owners_shared_all = owners_shared_all
        self._shared_ids = shared_ids

    def _create_bulk_filter(self) -> Optional[JobFilter]:
        if not self._has_access_to_all and not self._owners_shared_all:
            return None
        # `self._owners_shared_all` is already filtered against
        # `self._query_filter.owners`.
        # if `self._owners_shared_all` is empty, we still want to try to limit
        # the scope to the owners passed in the query, otherwise pull all.
        owners = set(self._owners_shared_all or self._query_filter.owners)
        return replace(self._query_filter, owners=owners)
