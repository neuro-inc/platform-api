import json
import logging
from dataclasses import dataclass, replace
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence, Set

import aiohttp.web
import trafaret as t
from aiohttp_security import check_authorized
from multidict import MultiDictProxy
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.client import ClientSubTreeViewRoot
from yarl import URL

from platform_api.config import Config, RegistryConfig, StorageConfig
from platform_api.log import log_debug_time
from platform_api.orchestrator.job import JOB_USER_NAMES_SEPARATOR, Job
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolume,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter
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


def create_job_request_validator(*, allowed_gpu_models: Sequence[str]) -> t.Trafaret:
    return t.Dict(
        {
            "container": create_container_request_validator(
                allow_volumes=True, allowed_gpu_models=allowed_gpu_models
            ),
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("is_preemptible", optional=True, default=False): t.Bool,
            t.Key("schedule_timeout", optional=True): t.Float(gte=1, lt=30 * 24 * 3600),
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
            "cluster_name": t.String(allow_blank=False),
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
            t.Key("schedule_timeout", optional=True): t.Float,
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
    if container.entrypoint is not None:
        ret["entrypoint"] = container.entrypoint
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


def convert_job_to_job_response(job: Job, cluster_name: str) -> Dict[str, Any]:
    assert cluster_name, "empty cluster name must be already replaced with `default`"
    history = job.status_history
    current_status = history.current
    response_payload: Dict[str, Any] = {
        "id": job.id,
        "owner": job.owner,
        "cluster_name": cluster_name,
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
    if job.schedule_timeout is not None:
        response_payload["schedule_timeout"] = job.schedule_timeout
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
        await self._check_permissions(request, user, permissions)

        name = request_payload.get("name")
        description = request_payload.get("description")
        is_preemptible = request_payload["is_preemptible"]
        schedule_timeout = request_payload.get("schedule_timeout")
        job_request = JobRequest.create(container, description)
        job, _ = await self._jobs_service.create_job(
            job_request,
            user=user,
            job_name=name,
            is_preemptible=is_preemptible,
            schedule_timeout=schedule_timeout,
        )
        cluster_name = self._jobs_service.get_cluster_name(job)
        response_payload = convert_job_to_job_response(job, cluster_name)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def handle_get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        user = await untrusted_user(request)
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(uri=str(job.to_uri()), action="read")
        await self._check_permissions(request, user, [permission])

        cluster_name = self._jobs_service.get_cluster_name(job)
        response_payload = convert_job_to_job_response(job, cluster_name)
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

        with log_debug_time(f"Retrieved job access tree for user '{user.name}'"):
            tree = await self._auth_client.get_permissions_tree(user.name, "job:")

        jobs: List[Job] = []

        try:
            bulk_job_filter = BulkJobFilterBuilder(
                query_filter=self._job_filter_factory.create_from_query(request.query),
                access_tree=tree,
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

        response_payload = {
            "jobs": [
                convert_job_to_job_response(
                    job, cluster_name=self._jobs_service.get_cluster_name(job)
                )
                for job in jobs
            ]
        }
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
        await self._check_permissions(request, user, [permission])

        await self._jobs_service.delete_job(job_id)
        raise aiohttp.web.HTTPNoContent()

    async def _check_permissions(
        self, request: aiohttp.web.Request, user: User, permissions: List[Permission]
    ) -> None:
        await check_authorized(request)
        assert permissions, "empty permission set to check"
        logger.info("Checking whether %r has %r", user, permissions)
        missing = await self._auth_client.get_missing_permissions(
            user.name, permissions
        )
        if missing:
            error_details = {
                "resources": [self._permission_to_primitive(p) for p in missing]
            }
            raise aiohttp.web.HTTPForbidden(
                text=json.dumps(error_details), content_type="application/json"
            )

    def _permission_to_primitive(self, perm: Permission) -> Dict[str, str]:
        return {"uri": perm.uri, "action": perm.action}


class JobFilterException(ValueError):
    pass


class JobFilterFactory:
    def __init__(self) -> None:
        self._job_name_validator = create_job_name_validator()
        self._user_name_validator = create_user_name_validator()

    def create_from_query(self, query: MultiDictProxy) -> JobFilter:  # type: ignore
        statuses = {JobStatus(s) for s in query.getall("status", [])}
        hostname = query.get("hostname")
        if hostname is None:
            job_name = self._job_name_validator.check(query.get("name"))
            owners = {
                self._user_name_validator.check(owner)
                for owner in query.getall("owner", [])
            }
            return JobFilter(statuses=statuses, owners=owners, name=job_name)

        if "name" in query or "owner" in query:
            raise ValueError("Invalid request")

        label = hostname.partition(".")[0]
        job_name, sep, owner = label.rpartition(JOB_USER_NAMES_SEPARATOR)
        if not sep:
            return JobFilter(statuses=statuses, ids={label})
        job_name = self._job_name_validator.check(job_name)
        owner = self._user_name_validator.check(owner)
        return JobFilter(statuses=statuses, owners={owner}, name=job_name)


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
