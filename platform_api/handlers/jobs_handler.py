import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, replace
from pathlib import PurePath
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import aiohttp.web
import iso8601
import trafaret as t
from aiohttp_security import check_authorized
from aiohttp_security.api import AUTZ_KEY
from multidict import MultiDictProxy
from neuro_auth_client import AuthClient, Permission, check_permissions
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from yarl import URL

from platform_api.cluster_config import ClusterConfig, RegistryConfig, StorageConfig
from platform_api.config import Config
from platform_api.log import log_debug_time
from platform_api.orchestrator.job import (
    JOB_USER_NAMES_SEPARATOR,
    Job,
    JobRestartPolicy,
    JobStatusItem,
    maybe_job_id,
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolume,
    DiskContainerVolume,
    JobError,
    JobRequest,
    JobStatus,
    SecretContainerVolume,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import (
    ClusterOwnerNameSet,
    JobFilter,
    JobStorageTransactionError,
)
from platform_api.resource import Preset, TPUResource
from platform_api.user import User, authorized_user, untrusted_user

from .job_request_builder import create_container_from_payload
from .validators import (
    create_cluster_name_validator,
    create_container_request_validator,
    create_container_response_validator,
    create_job_history_validator,
    create_job_name_validator,
    create_job_status_validator,
    create_job_tag_validator,
    create_user_name_validator,
    sanitize_dns_name,
)


logger = logging.getLogger(__name__)


def create_job_request_validator(
    *,
    allow_flat_structure: bool = False,
    allowed_gpu_models: Sequence[str],
    allowed_tpu_resources: Sequence[TPUResource],
    cluster_name: str,
    storage_scheme: str = "storage",
) -> t.Trafaret:
    container_validator = create_container_request_validator(
        allow_volumes=True,
        allowed_gpu_models=allowed_gpu_models,
        allowed_tpu_resources=allowed_tpu_resources,
        storage_scheme=storage_scheme,
        cluster_name=cluster_name,
    )
    job_validator = t.Dict(
        {
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("preset_name", optional=True): t.String,
            t.Key("tags", optional=True): t.List(
                create_job_tag_validator(), max_length=16
            ),
            t.Key("is_preemptible", optional=True, default=False): t.Bool,
            t.Key("is_preemptible_node_required", default=False): t.Bool,
            t.Key("pass_config", optional=True, default=False): t.Bool,
            t.Key("wait_for_jobs_quota", optional=True, default=False): t.Bool,
            t.Key("schedule_timeout", optional=True): t.Float(gte=1, lt=30 * 24 * 3600),
            t.Key("max_run_time_minutes", optional=True): t.Int(gte=1),
            t.Key("cluster_name", default=cluster_name): t.Atom(cluster_name),
            t.Key("restart_policy", default=str(JobRestartPolicy.NEVER)): t.Enum(
                *[str(policy) for policy in JobRestartPolicy]
            )
            >> JobRestartPolicy,
        }
    )
    # Either flat structure or payload with container field are allowed
    if not allow_flat_structure:
        # Deprecated. Use flat structure
        return job_validator + t.Dict({"container": container_validator})
    return job_validator + container_validator


def create_job_preset_validator(
    presets: Sequence[Preset],
) -> t.Trafaret:
    def _check_no_resources(payload: Dict[str, Any]) -> Dict[str, Any]:
        if "container" in payload:
            resources = payload["container"].get("resources")
        else:
            resources = payload.get("resources")
        if not resources:
            return payload
        if set(resources.keys()) - {"shm"}:
            raise t.DataError("Both preset and resources are not allowed")
        return payload

    def _set_preset_resources(payload: Dict[str, Any]) -> Dict[str, Any]:
        preset_name = payload["preset_name"]
        preset = {p.name: p for p in presets}[preset_name]
        payload["is_preemptible"] = preset.is_preemptible
        payload["is_preemptible_node_required"] = preset.is_preemptible_node_required
        if "container" in payload:
            shm = payload["container"].get("resources", {}).get("shm", False)
        else:
            shm = payload.get("resources", {}).get("shm", False)
        container_resources = {
            "cpu": preset.cpu,
            "memory_mb": preset.memory_mb,
            "shm": shm,
        }
        if preset.gpu:
            container_resources["gpu"] = preset.gpu
            container_resources["gpu_model"] = preset.gpu_model
        if preset.tpu:
            container_resources["tpu"] = {
                "type": preset.tpu.type,
                "software_version": preset.tpu.software_version,
            }
        if "container" in payload:
            payload["container"]["resources"] = container_resources
        else:
            payload["resources"] = container_resources
        return payload

    validator = (
        t.Dict({"preset_name": t.Enum(*[p.name for p in presets])}).allow_extra("*")
        >> _check_no_resources
        >> _set_preset_resources
    )

    return validator


def create_job_cluster_name_validator(default_cluster_name: str) -> t.Trafaret:
    return t.Dict(
        {t.Key("cluster_name", default=default_cluster_name): t.String}
    ).allow_extra("*")


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
            "uri": t.String(allow_blank=False),
            # `status` is left for backward compat. the python client/cli still
            # relies on it.
            "status": create_job_status_validator(),
            t.Key("http_url", optional=True): t.String,
            t.Key("http_url_named", optional=True): t.String,
            "history": create_job_history_validator(),
            "container": create_container_response_validator(),
            "is_preemptible": t.Bool,
            "is_preemptible_node_required": t.Bool,
            "pass_config": t.Bool,
            t.Key("internal_hostname", optional=True): t.String,
            t.Key("internal_hostname_named", optional=True): t.String,
            t.Key("name", optional=True): create_job_name_validator(max_length=None),
            t.Key("preset_name", optional=True): t.String,
            t.Key("description", optional=True): t.String,
            t.Key("tags", optional=True): t.List(create_job_tag_validator()),
            t.Key("schedule_timeout", optional=True): t.Float,
            t.Key("max_run_time_minutes", optional=True): t.Int,
            "restart_policy": t.String,
        }
    )


def create_job_set_status_validator() -> t.Trafaret:
    return t.Dict(
        {
            "status": t.String,
            t.Key("reason", optional=True): t.String | t.Null,
            t.Key("description", optional=True): t.String | t.Null,
            t.Key("exit_code", optional=True): t.Int | t.Null,
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
    if container.resources.tpu:
        resources["tpu"] = {
            "type": container.resources.tpu.type,
            "software_version": container.resources.tpu.software_version,
        }
    ret["resources"] = resources

    if container.http_server is not None:
        ret["http"] = {
            "port": container.http_server.port,
            "health_check_path": container.http_server.health_check_path,
            "requires_auth": container.http_server.requires_auth,
        }
    for volume in container.volumes:
        ret["volumes"].append(convert_container_volume_to_json(volume, storage_config))
    for sec_volume in container.secret_volumes:
        if "secret_volumes" not in ret:
            ret["secret_volumes"] = []
        ret["secret_volumes"].append(convert_secret_volume_to_json(sec_volume))
    for disk_volume in container.disk_volumes:
        if "disk_volumes" not in ret:
            ret["disk_volumes"] = []
        ret["disk_volumes"].append(convert_disk_volume_to_json(disk_volume))
    for env_name, sec_env in container.secret_env.items():
        if "secret_env" not in ret:
            ret["secret_env"] = {}
        ret["secret_env"][env_name] = str(sec_env.to_uri())

    if container.tty:
        ret["tty"] = True
    if container.working_dir is not None:
        ret["working_dir"] = container.working_dir
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


def convert_secret_volume_to_json(volume: SecretContainerVolume) -> Dict[str, Any]:
    return {
        "src_secret_uri": str(volume.to_uri()),
        "dst_path": str(volume.dst_path),
    }


def convert_disk_volume_to_json(volume: DiskContainerVolume) -> Dict[str, Any]:
    return {
        "src_disk_uri": str(volume.disk.to_uri()),
        "dst_path": str(volume.dst_path),
        "read_only": volume.read_only,
    }


def convert_job_to_job_response(job: Job) -> Dict[str, Any]:
    assert (
        job.cluster_name
    ), "empty cluster name must be already replaced with `default`"
    history = job.status_history
    current_status = history.current
    response_payload: Dict[str, Any] = {
        "id": job.id,
        "owner": job.owner,
        "cluster_name": job.cluster_name,
        "status": current_status.status,
        "history": {
            "status": current_status.status,
            "reason": current_status.reason,
            "description": current_status.description,
            "created_at": history.created_at_str,
            "run_time_seconds": job.get_run_time().total_seconds(),
            "restarts": history.restart_count,
        },
        "container": convert_job_container_to_json(
            job.request.container, job.storage_config
        ),
        "is_preemptible": job.is_preemptible,
        "is_preemptible_node_required": job.is_preemptible_node_required,
        "pass_config": job.pass_config,
        "uri": str(job.to_uri()),
        "restart_policy": str(job.restart_policy),
    }
    if job.name:
        response_payload["name"] = job.name
    if job.preset_name:
        response_payload["preset_name"] = job.preset_name
    if job.description:
        response_payload["description"] = job.description
    if job.tags:
        response_payload["tags"] = job.tags
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
    if job.internal_hostname_named:
        response_payload["internal_hostname_named"] = job.internal_hostname_named
    if job.schedule_timeout is not None:
        response_payload["schedule_timeout"] = job.schedule_timeout
    if job.max_run_time_minutes is not None:
        response_payload["max_run_time_minutes"] = job.max_run_time_minutes
    if history.started_at:
        response_payload["history"]["started_at"] = history.started_at_str
    if history.is_finished:
        response_payload["history"]["finished_at"] = history.finished_at_str
    if current_status.exit_code is not None:
        response_payload["history"]["exit_code"] = current_status.exit_code
    return response_payload


def infer_permissions_from_container(
    user: User,
    container: Container,
    registry_config: RegistryConfig,
    cluster_name: str,
) -> List[Permission]:
    permissions = [Permission(uri=str(user.to_job_uri(cluster_name)), action="write")]
    if container.belongs_to_registry(registry_config):
        permissions.append(
            Permission(
                uri=str(container.to_image_uri(registry_config, cluster_name)),
                action="read",
            )
        )
    for secret_uri in container.get_secret_uris():
        permissions.append(Permission(uri=str(secret_uri), action="read"))
    for volume in container.volumes:
        action = "read" if volume.read_only else "write"
        permission = Permission(uri=str(volume.uri), action=action)
        permissions.append(permission)
    for disk_volume in container.disk_volumes:
        action = "read" if disk_volume.read_only else "write"
        permission = Permission(uri=str(disk_volume.disk.to_uri()), action=action)
        permissions.append(permission)
    return permissions


class JobsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

        self._job_filter_factory = JobFilterFactory()
        self._job_response_validator = create_job_response_validator()
        self._job_set_status_validator = create_job_set_status_validator()
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
                aiohttp.web.put("/{job_id}/status", self.handle_put_status),
            )
        )

    def _check_user_can_submit_jobs(
        self, user_cluster_configs: Sequence[ClusterConfig]
    ) -> None:
        if not user_cluster_configs:
            raise aiohttp.web.HTTPForbidden(
                text=json.dumps({"error": "No clusters"}),
                content_type="application/json",
            )

    async def _create_job_request_validator(
        self,
        cluster_config: ClusterConfig,
        allow_flat_structure: bool = False,
    ) -> t.Trafaret:
        resource_pool_types = cluster_config.orchestrator.resource_pool_types
        gpu_models = list(
            {rpt.gpu_model for rpt in resource_pool_types if rpt.gpu_model}
        )
        return create_job_request_validator(
            allow_flat_structure=allow_flat_structure,
            allowed_gpu_models=gpu_models,
            allowed_tpu_resources=cluster_config.orchestrator.tpu_resources,
            cluster_name=cluster_config.name,
            storage_scheme=cluster_config.storage.uri_scheme,
        )

    def _get_cluster_config(
        self, cluster_configs: Sequence[ClusterConfig], cluster_name: str
    ) -> ClusterConfig:
        for config in cluster_configs:
            if config.name == cluster_name:
                return config
        raise aiohttp.web.HTTPForbidden(
            text=json.dumps(
                {
                    "error": (
                        "User is not allowed to submit jobs " "to the specified cluster"
                    )
                }
            ),
            content_type="application/json",
        )

    async def create_job(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        user = await authorized_user(request)

        orig_payload = await request.json()

        cluster_configs = await self._jobs_service.get_user_cluster_configs(user)
        self._check_user_can_submit_jobs(cluster_configs)
        default_cluster_name = cluster_configs[0].name

        job_cluster_name_validator = create_job_cluster_name_validator(
            default_cluster_name
        )
        request_payload = job_cluster_name_validator.check(orig_payload)
        self._check_user_can_submit_jobs(cluster_configs)
        cluster_name = request_payload["cluster_name"]
        cluster_config = self._get_cluster_config(cluster_configs, cluster_name)

        if "from_preset" in request.query:
            job_preset_validator = create_job_preset_validator(
                cluster_config.orchestrator.presets
            )
            request_payload = job_preset_validator.check(request_payload)
            allow_flat_structure = True
        else:
            allow_flat_structure = False

        job_request_validator = await self._create_job_request_validator(
            cluster_config, allow_flat_structure=allow_flat_structure
        )
        request_payload = job_request_validator.check(request_payload)

        container = create_container_from_payload(
            request_payload, storage_config=cluster_config.storage
        )

        permissions = infer_permissions_from_container(
            user, container, cluster_config.registry, cluster_name
        )
        await check_permissions(request, permissions)

        name = request_payload.get("name")
        preset_name = request_payload.get("preset_name")
        tags = sorted(set(request_payload.get("tags", [])))
        description = request_payload.get("description")
        is_preemptible = request_payload["is_preemptible"]
        is_preemptible_node_required = request_payload.get(
            "is_preemptible_node_required", False
        )
        pass_config = request_payload["pass_config"]
        schedule_timeout = request_payload.get("schedule_timeout")
        max_run_time_minutes = request_payload.get("max_run_time_minutes")
        wait_for_jobs_quota = request_payload.get("wait_for_jobs_quota")
        job_request = JobRequest.create(container, description)
        job, _ = await self._jobs_service.create_job(
            job_request,
            user=user,
            cluster_name=cluster_name,
            job_name=name,
            preset_name=preset_name,
            tags=tags,
            is_preemptible=is_preemptible,
            is_preemptible_node_required=is_preemptible_node_required,
            pass_config=pass_config,
            wait_for_jobs_quota=wait_for_jobs_quota,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
            restart_policy=request_payload["restart_policy"],
        )
        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def _resolve_job(self, request: aiohttp.web.Request, action: str) -> Job:
        id_or_name = request.match_info["job_id"]
        try:
            job = await self._jobs_service.get_job(id_or_name)
        except JobError:
            await check_authorized(request)
            user = await untrusted_user(request)
            job = await self._jobs_service.get_job_by_name(id_or_name, user)

        uri = job.to_uri()
        permissions = [Permission(uri=str(uri), action=action)]
        if job.name:
            permissions.append(
                Permission(uri=str(_job_uri_with_name(uri, job.name)), action=action)
            )
        await check_any_permissions(request, permissions)
        return job

    async def handle_get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        job = await self._resolve_job(request, "read")

        if request.query.get("_tests_check_materialized"):
            # Used in tests during cleanup
            return aiohttp.web.json_response(
                data={"materialized": job.materialized},
                status=aiohttp.web.HTTPOk.status_code,
            )

        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )

    def _accepts_ndjson(self, request: aiohttp.web.Request) -> bool:
        accept = request.headers.get("Accept", "")
        return "application/x-ndjson" in accept

    async def handle_get_all(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        # TODO (A Danshyn 10/08/18): remove once
        # AuthClient.get_permissions_tree accepts the token param
        await check_authorized(request)
        user = await untrusted_user(request)

        with log_debug_time(f"Retrieved job access tree for user '{user.name}'"):
            tree = await self._auth_client.get_permissions_tree(user.name, "job:")

        try:
            bulk_job_filter = BulkJobFilterBuilder(
                query_filter=self._job_filter_factory.create_from_query(request.query),
                access_tree=tree,
            ).build()
        except JobFilterException:
            bulk_job_filter = BulkJobFilter(
                bulk_filter=None, shared_ids=set(), shared_ids_filter=None
            )

        reverse = _parse_bool(request.query.get("reverse", "0"))
        if "limit" in request.query:
            limit = int(request.query["limit"])
            if limit <= 0:
                raise ValueError("limit should be > 0")

            async def limit_filter(it: AsyncIterator[Job]) -> AsyncIterator[Job]:
                count = limit
                async for x in it:
                    yield x
                    count -= 1
                    if not count:
                        break

            jobs = limit_filter(
                self._iter_filtered_jobs(bulk_job_filter, reverse, limit)
            )
        else:
            jobs = self._iter_filtered_jobs(bulk_job_filter, reverse, None)

        if self._accepts_ndjson(request):
            response = aiohttp.web.StreamResponse()
            response.headers["Content-Type"] = "application/x-ndjson"
            await response.prepare(request)
            try:
                async for job in jobs:
                    response_payload = convert_job_to_job_response(job)
                    self._job_response_validator.check(response_payload)
                    await response.write(json.dumps(response_payload).encode() + b"\n")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                msg_str = (
                    f"Unexpected exception {e.__class__.__name__}: {str(e)}. "
                    f"Path with query: {request.path_qs}."
                )
                logging.exception(msg_str)
                payload = {"error": msg_str}
                await response.write(json.dumps(payload).encode())
            await response.write_eof()
            return response
        else:
            response_payload = {
                "jobs": [convert_job_to_job_response(job) async for job in jobs]
            }
            self._bulk_jobs_response_validator.check(response_payload)
            return aiohttp.web.json_response(
                data=response_payload, status=aiohttp.web.HTTPOk.status_code
            )

    async def _iter_filtered_jobs(
        self, bulk_job_filter: "BulkJobFilter", reverse: bool, limit: Optional[int]
    ) -> AsyncIterator[Job]:
        def job_key(job: Job) -> Tuple[float, str, Job]:
            return job.status_history.created_at_timestamp, job.id, job

        if bulk_job_filter.shared_ids:
            with log_debug_time(
                f"Read shared jobs with {bulk_job_filter.shared_ids_filter}"
            ):
                shared_jobs = [
                    job_key(job)
                    for job in await self._jobs_service.get_jobs_by_ids(
                        bulk_job_filter.shared_ids,
                        job_filter=bulk_job_filter.shared_ids_filter,
                    )
                ]
                shared_jobs.sort(reverse=not reverse)
        else:
            shared_jobs = []

        if bulk_job_filter.bulk_filter:
            with log_debug_time(f"Read bulk jobs with {bulk_job_filter.bulk_filter}"):
                async for job in self._jobs_service.iter_all_jobs(
                    bulk_job_filter.bulk_filter, reverse=reverse, limit=limit
                ):
                    key = job_key(job)
                    # Merge shared jobs and bulk jobs in the creation order
                    while shared_jobs and (
                        shared_jobs[-1] > key if reverse else shared_jobs[-1] < key
                    ):
                        yield shared_jobs.pop()[-1]
                    yield job

        for key in reversed(shared_jobs):
            yield key[-1]

    async def handle_delete(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job = await self._resolve_job(request, "write")

        await self._jobs_service.cancel_job(job.id)
        raise aiohttp.web.HTTPNoContent()

    async def handle_put_status(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        assert job.cluster_name
        permission = Permission(uri=f"job://{job.cluster_name}", action="manage")
        await check_permissions(request, [permission])

        orig_payload = await request.json()
        request_payload = self._job_set_status_validator.check(orig_payload)

        status_item = JobStatusItem.create(
            JobStatus(request_payload["status"]),
            reason=request_payload.get("reason"),
            description=request_payload.get("description"),
            exit_code=request_payload.get("exit_code"),
        )

        try:
            await self._jobs_service.set_job_status(job_id, status_item)
        except JobStorageTransactionError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPConflict.status_code
            )
        else:
            raise aiohttp.web.HTTPNoContent()


class JobFilterException(ValueError):
    pass


class JobFilterFactory:
    def __init__(self) -> None:
        self._job_name_validator = create_job_name_validator()
        self._user_name_validator = create_user_name_validator()
        self._cluster_name_validator = create_cluster_name_validator()

    def create_from_query(self, query: MultiDictProxy) -> JobFilter:  # type: ignore
        statuses = {JobStatus(s) for s in query.getall("status", [])}
        tags = set(query.getall("tag", []))
        hostname = query.get("hostname")
        if hostname is None:
            job_name = self._job_name_validator.check(query.get("name"))
            owners = {
                self._user_name_validator.check(owner)
                for owner in query.getall("owner", [])
            }
            clusters: ClusterOwnerNameSet = {
                self._cluster_name_validator.check(cluster_name): {}
                for cluster_name in query.getall("cluster_name", [])
            }
            since = query.get("since")
            until = query.get("until")
            return JobFilter(
                statuses=statuses,
                clusters=clusters,
                owners=owners,
                name=job_name,
                tags=tags,
                since=iso8601.parse_date(since) if since else JobFilter.since,
                until=iso8601.parse_date(until) if until else JobFilter.until,
            )

        for key in ("name", "owner", "cluster_name", "since", "until"):
            if key in query:
                raise ValueError("Invalid request")

        label = hostname.partition(".")[0]
        job_name, sep, owner = label.rpartition(JOB_USER_NAMES_SEPARATOR)
        if not sep:
            return JobFilter(statuses=statuses, ids={label}, tags=tags)
        job_name = self._job_name_validator.check(job_name)
        owner = self._user_name_validator.check(owner)
        return JobFilter(statuses=statuses, owners={owner}, name=job_name, tags=tags)


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
        self._has_clusters_shared_all: bool = False
        self._clusters_shared_any: Dict[str, Dict[str, Set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        self._owners_shared_any: Set[str] = set()
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
        tree = self._access_tree.sub_tree

        if not tree.can_list():
            # no job resources whatsoever
            raise JobFilterException("no jobs")

        if tree.can_read():
            # read access to all jobs = job:
            self._has_access_to_all = True
            return

        self._traverse_clusters(tree)

        if (
            not self._clusters_shared_any
            and not self._owners_shared_any
            and not self._shared_ids
        ):
            # no job resources whatsoever
            raise JobFilterException("no jobs")

    def _traverse_clusters(self, tree: ClientAccessSubTreeView) -> None:
        for cluster_name, sub_tree in tree.children.items():
            if not sub_tree.can_list():
                continue

            if (
                self._query_filter.clusters
                and cluster_name not in self._query_filter.clusters
            ):
                # skipping clusters
                continue

            if sub_tree.can_read():
                # read/write/manage access to all jobs on the cluster =
                # job://cluster
                self._has_clusters_shared_all = True
                self._clusters_shared_any[cluster_name] = {}
            else:
                self._traverse_owners(sub_tree, cluster_name)

    def _traverse_owners(
        self, tree: ClientAccessSubTreeView, cluster_name: str
    ) -> None:
        for owner, sub_tree in tree.children.items():
            if not sub_tree.can_list():
                continue

            if self._query_filter.owners and owner not in self._query_filter.owners:
                # skipping owners
                continue

            if sub_tree.can_read():
                # read/write/manage access to all owner's jobs =
                # job://cluster/owner
                self._owners_shared_any.add(owner)
                self._clusters_shared_any[cluster_name][owner] = set()
            else:
                # specific ids or names
                self._traverse_jobs(sub_tree, cluster_name, owner)

    def _traverse_jobs(
        self, tree: ClientAccessSubTreeView, cluster_name: str, owner: str
    ) -> None:
        for name, sub_tree in tree.children.items():
            if sub_tree.can_read():
                if maybe_job_id(name):
                    self._shared_ids.add(name)
                    continue

                if self._query_filter.name and name != self._query_filter.name:
                    # skipping name
                    continue

                self._owners_shared_any.add(owner)
                self._clusters_shared_any[cluster_name][owner].add(name)

    def _create_bulk_filter(self) -> Optional[JobFilter]:
        if not (
            self._has_access_to_all
            or self._clusters_shared_any
            or self._owners_shared_any
        ):
            return None
        bulk_filter = self._query_filter
        # `self._owners_shared_any` is already filtered against
        # `self._query_filter.owners`.
        # if `self._owners_shared_any` is empty and no clusters share full
        # access, we still want to try to limit the scope to the owners
        # passed in the query, otherwise pull all.
        if not self._has_clusters_shared_all and self._owners_shared_any:
            bulk_filter = replace(bulk_filter, owners=self._owners_shared_any)
        # `self._clusters_shared_any` is already filtered against
        # `self._query_filter.clusters`.
        # if `self._clusters_shared_any` is empty, we still want to try to limit
        # the scope to the clusters passed in the query, otherwise pull all.
        if not self._has_access_to_all:
            self._optimize_clusters_owners(bulk_filter.owners, bulk_filter.name)
            bulk_filter = replace(
                bulk_filter,
                clusters={
                    cluster_name: dict(owners)
                    for cluster_name, owners in self._clusters_shared_any.items()
                },
            )
        return bulk_filter

    def _optimize_clusters_owners(
        self, owners: AbstractSet[str], name: Optional[str]
    ) -> None:
        if owners or name:
            names = {name}
            for cluster_owners in self._clusters_shared_any.values():
                if name:
                    for owner_names in cluster_owners.values():
                        if owner_names == names:
                            owner_names.clear()
                if (
                    owners
                    and cluster_owners.keys() == owners
                    and not any(cluster_owners.values())
                ):
                    cluster_owners.clear()


def _parse_bool(value: str) -> bool:
    value = value.lower()
    if value in ("0", "false"):
        return False
    elif value in ("1", "true"):
        return True
    else:
        raise ValueError('Required "0", "1", "false" or "true"')


async def check_any_permissions(
    request: aiohttp.web.Request, permissions: List[Permission]
) -> None:
    user_name = await check_authorized(request)
    auth_policy = request.config_dict.get(AUTZ_KEY)
    if not auth_policy:
        raise RuntimeError("Auth policy not configured")

    try:
        missing = await auth_policy.get_missing_permissions(user_name, permissions)
    except aiohttp.ClientError as e:
        # re-wrap in order not to expose the client
        raise RuntimeError(e) from e

    if len(missing) >= len(permissions):
        payload = {"missing": [_permission_to_primitive(p) for p in missing]}
        raise aiohttp.web.HTTPForbidden(
            text=json.dumps(payload), content_type="application/json"
        )


def _permission_to_primitive(perm: Permission) -> Dict[str, str]:
    return {"uri": perm.uri, "action": perm.action}


def _job_uri_with_name(uri: URL, name: str) -> URL:
    assert name
    assert uri.host
    assert uri.name
    return uri.with_name(name)
