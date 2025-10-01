import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence, Set as AbstractSet
from dataclasses import dataclass, replace
from typing import Any

import aiohttp.web
import iso8601
import trafaret as t
import trafaret.keys
from aiohttp_security import check_authorized
from multidict import MultiDictProxy
from neuro_auth_client import (
    AuthClient,
    Permission,
    check_permissions,
)
from neuro_auth_client.client import ClientAccessSubTreeView, ClientSubTreeViewRoot
from neuro_config_client import Cluster, ResourcePreset, TPUResource
from yarl import URL

from platform_api.config import STORAGE_URI_SCHEME, Config
from platform_api.log import log_debug_time
from platform_api.orchestrator.job import (
    JOB_NAME_SEPARATOR,
    Job,
    JobPriority,
    JobRestartPolicy,
    JobStatusItem,
    JobStatusReason,
    get_base_owner,
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
from platform_api.orchestrator.jobs_service import JobsService, UserClusterConfig
from platform_api.orchestrator.jobs_storage import (
    ClusterOrgProjectNameSet,
    JobFilter,
    JobStorageTransactionError,
)
from platform_api.user import authorized_user, untrusted_user
from platform_api.utils.asyncio import asyncgeneratorcontextmanager

from .job_request_builder import create_container_from_payload
from .validators import (
    create_base_owner_name_validator,
    create_cluster_name_validator,
    create_container_request_validator,
    create_container_response_validator,
    create_job_history_validator,
    create_job_name_validator,
    create_job_status_validator,
    create_job_tag_validator,
    create_org_name_validator,
    create_org_project_hash_validator,
    create_project_name_validator,
    create_user_name_validator,
    sanitize_dns_name,
)

logger = logging.getLogger(__name__)


def create_job_request_validator(
    *,
    allow_flat_structure: bool = False,
    allowed_tpu_resources: Sequence[TPUResource],
    cluster_name: str,
    org_name: str | None,
    storage_scheme: str = "storage",
    allowed_energy_schedule_names: Sequence[str] = (),
) -> t.Trafaret:
    def _check_no_schedule_timeout_for_scheduled_jobs(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if "schedule_timeout" in payload and payload["scheduler_enabled"]:
            raise t.DataError("schedule_timeout is not allowed for scheduled jobs")
        return payload

    def _check_scheduler_enabled_for_energy_schedule_name(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if payload.get("energy_schedule_name") and not payload["scheduler_enabled"]:
            raise t.DataError("energy_schedule_name requires scheduler_enabled")
        return payload

    container_validator = create_container_request_validator(
        allow_volumes=True,
        allowed_tpu_resources=allowed_tpu_resources,
        storage_scheme=storage_scheme,
        cluster_name=cluster_name,
    )

    def multiname_key(
        name: str, keys: Sequence[str], default: Any, trafaret_func: t.Trafaret
    ) -> t.Key:
        _empty = object()

        def _take_first(data: dict[str, Any]) -> dict[str, Any]:
            for key in keys:
                if data[key] is not _empty:
                    return trafaret_func(data[key])
            return trafaret_func(default)

        return t.keys.subdict(
            name,
            *(t.Key(name=key, optional=True, default=_empty) for key in keys),
            trafaret=_take_first,
        )

    job_validator = t.Dict(
        {
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("preset_name", optional=True): t.String,
            t.Key("tags", optional=True): t.List(
                create_job_tag_validator(), max_length=16
            ),
            t.Key("pass_config", optional=True, default=False): t.Bool,
            t.Key("wait_for_jobs_quota", optional=True, default=False): t.Bool,
            t.Key("privileged", optional=True, default=False): t.Bool,
            # fmt: off
            t.Key(
                "priority", optional=True, default=JobPriority.NORMAL.to_name()
            ): t.Enum(*(p.to_name() for p in JobPriority)) >> JobPriority.from_name,
            # fmt: on
            t.Key("schedule_timeout", optional=True): t.Float(gte=1, lt=30 * 24 * 3600),
            t.Key("max_run_time_minutes", optional=True): t.Int(gte=1),
            t.Key("cluster_name", default=cluster_name): t.Atom(cluster_name),
            t.Key("org_name", default=org_name): t.Atom(org_name),
            t.Key("project_name", optional=True): t.String,
            t.Key("restart_policy", default=str(JobRestartPolicy.NEVER)): t.Enum(
                *(str(policy) for policy in JobRestartPolicy)
            )
            >> JobRestartPolicy,
            t.Key("energy_schedule_name", optional=True): t.Enum(
                *allowed_energy_schedule_names
            ),
        },
        multiname_key(
            "scheduler_enabled",
            ["scheduler_enabled", "is_preemptible"],
            default=False,
            trafaret_func=t.Bool(),
        ),
        multiname_key(
            "preemptible_node",
            ["preemptible_node", "is_preemptible_node_required"],
            default=False,
            trafaret_func=t.Bool(),
        ),
    )
    # Either flat structure or payload with container field are allowed
    if not allow_flat_structure:
        # Deprecated. Use flat structure
        validator = job_validator + t.Dict({"container": container_validator})
    else:
        validator = job_validator + container_validator
    return (
        validator
        >> _check_no_schedule_timeout_for_scheduled_jobs
        >> _check_scheduler_enabled_for_energy_schedule_name
    )


def create_job_preset_validator(presets: Sequence[ResourcePreset]) -> t.Trafaret:
    def _check_no_resources(payload: dict[str, Any]) -> dict[str, Any]:
        if "container" in payload:
            resources = payload["container"].get("resources")
        else:
            resources = payload.get("resources")
        if not resources:
            return payload
        if set(resources.keys()) - {"shm"}:
            raise t.DataError("Both preset and resources are not allowed")
        return payload

    def _check_preset_exists(payload: dict[str, Any]) -> dict[str, Any]:
        if not presets:
            raise t.DataError("At least one preset is required to run a job")
        preset_name = payload.get("preset_name")
        preset_names = [p.name for p in presets]
        if not preset_name:
            payload["preset_name"] = preset_names[0]
            return payload
        if preset_name not in preset_names:
            raise t.DataError(
                "Preset was not found", name="preset_name", value=preset_name
            )
        return payload

    def _set_preset_resources(payload: dict[str, Any]) -> dict[str, Any]:
        preset_name = payload["preset_name"]
        preset = {p.name: p for p in presets}[preset_name]
        payload["scheduler_enabled"] = preset.scheduler_enabled
        payload["preemptible_node"] = preset.preemptible_node
        if "container" in payload:
            shm = payload["container"].get("resources", {}).get("shm", False)
        else:
            shm = payload.get("resources", {}).get("shm", False)
        container_resources = {
            "cpu": preset.cpu,
            "memory": preset.memory,
            "shm": shm,
        }
        if preset.nvidia_gpu:
            container_resources["nvidia_gpu"] = preset.nvidia_gpu.count
            if preset.nvidia_gpu.model:
                container_resources["nvidia_gpu_model"] = preset.nvidia_gpu.model
                container_resources["gpu_model"] = preset.nvidia_gpu.model
        if preset.amd_gpu:
            container_resources["amd_gpu"] = preset.amd_gpu.count
            if preset.amd_gpu.model:
                container_resources["amd_gpu_model"] = preset.amd_gpu.model
        if preset.intel_gpu:
            container_resources["intel_gpu"] = preset.intel_gpu.count
            if preset.intel_gpu.model:
                container_resources["intel_gpu_model"] = preset.intel_gpu.model
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

    return t.Call(_check_no_resources) >> _check_preset_exists >> _set_preset_resources


def create_job_cluster_org_name_validator(
    *,
    default_cluster_name: str,
    default_org_name: str,
    default_project_name: str,
) -> t.Trafaret:
    return t.Dict(
        {
            t.Key(
                "cluster_name", default=default_cluster_name
            ): create_cluster_name_validator(),
            t.Key("org_name", default=default_org_name): create_org_name_validator(),
            t.Key(
                "project_name", default=default_project_name
            ): create_project_name_validator(),
        }
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
            "namespace": t.String,
            "cluster_name": t.String(allow_blank=False),
            t.Key("org_name", optional=True): t.String,
            "project_name": t.String(allow_blank=False),
            "org_project_hash": t.String(allow_blank=False),
            "uri": t.String(allow_blank=False),
            # `status` is left for backward compat. the python client/cli still
            # relies on it.
            "status": create_job_status_validator(),
            "statuses": t.List(
                t.Dict(
                    {
                        "status": create_job_status_validator(),
                        "transition_time": t.String,
                        "reason": t.String(allow_blank=True) | t.Null,
                        "description": t.String(allow_blank=True) | t.Null,
                        t.Key("exit_code", optional=True): t.Int | t.Null,
                    }
                )
            ),
            t.Key("http_url", optional=True): t.String,
            t.Key("http_url_named", optional=True): t.String,
            "history": create_job_history_validator(),
            "container": create_container_response_validator(),
            "scheduler_enabled": t.Bool,
            "preemptible_node": t.Bool,
            "materialized": t.Bool,
            "being_dropped": t.Bool,
            "logs_removed": t.Bool,
            "total_price_credits": t.String,
            "price_credits_per_hour": t.String,
            t.Key("is_preemptible", optional=True): t.Bool,
            t.Key("is_preemptible_node_required", optional=True): t.Bool,
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
            "privileged": t.Bool,
            "priority": t.String,
            t.Key("energy_schedule_name", optional=True): t.String,
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


def create_job_set_materialized_validator() -> t.Trafaret:
    return t.Dict(
        {
            "materialized": t.Bool,
        }
    )


def create_job_update_max_run_time_minutes_validator() -> t.Trafaret:
    def _check_exactly_one(payload: dict[str, Any]) -> dict[str, Any]:
        if not payload or (
            "max_run_time_minutes" in payload
            and "additional_max_run_time_minutes" in payload
        ):
            raise t.DataError(
                "Exactly one of 'max_run_time_minutes' and "
                "'additional_max_run_time_minutes' allowed"
            )
        return payload

    return (
        t.Dict(
            {
                t.Key("max_run_time_minutes", optional=True): t.Int(gte=1),
                t.Key("additional_max_run_time_minutes", optional=True): t.Int(gte=1),
            }
        )
        >> _check_exactly_one
    )


def create_drop_progress_validator() -> t.Trafaret:
    return t.Dict(
        {
            t.Key("logs_removed", optional=True): t.Bool,
        }
    )


def convert_job_container_to_json(container: Container) -> dict[str, Any]:
    ret: dict[str, Any] = {
        "image": container.image,
        "env": container.env,
        "volumes": [],
    }
    if container.entrypoint is not None:
        ret["entrypoint"] = container.entrypoint
    if container.command is not None:
        ret["command"] = container.command

    resources: dict[str, Any] = {
        "cpu": container.resources.cpu,
        "memory": container.resources.memory,
        "memory_mb": container.resources.memory // 2**20,
    }
    if container.resources.nvidia_gpu is not None:
        resources["nvidia_gpu"] = container.resources.nvidia_gpu
        resources["gpu"] = container.resources.nvidia_gpu
    if container.resources.amd_gpu is not None:
        resources["amd_gpu"] = container.resources.amd_gpu
    if container.resources.intel_gpu is not None:
        resources["intel_gpu"] = container.resources.intel_gpu
    if container.resources.nvidia_gpu_model:
        resources["gpu_model"] = container.resources.nvidia_gpu_model
        resources["nvidia_gpu_model"] = container.resources.nvidia_gpu_model
    if container.resources.amd_gpu_model:
        resources["amd_gpu_model"] = container.resources.amd_gpu_model
    if container.resources.intel_gpu_model:
        resources["intel_gpu_model"] = container.resources.intel_gpu_model
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
        ret["volumes"].append(convert_container_volume_to_json(volume))
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


def convert_container_volume_to_json(volume: ContainerVolume) -> dict[str, Any]:
    return {
        "src_storage_uri": str(volume.uri),
        "dst_path": str(volume.dst_path),
        "read_only": volume.read_only,
    }


def convert_secret_volume_to_json(volume: SecretContainerVolume) -> dict[str, Any]:
    return {
        "src_secret_uri": str(volume.to_uri()),
        "dst_path": str(volume.dst_path),
    }


def convert_disk_volume_to_json(volume: DiskContainerVolume) -> dict[str, Any]:
    return {
        "src_disk_uri": str(volume.disk.to_uri()),
        "dst_path": str(volume.dst_path),
        "read_only": volume.read_only,
    }


def convert_job_to_job_response(job: Job) -> dict[str, Any]:
    assert job.cluster_name
    history = job.status_history
    current_status = history.current
    response_payload: dict[str, Any] = {
        "id": job.id,
        "owner": job.owner,
        "namespace": job.namespace,
        "cluster_name": job.cluster_name,
        "project_name": job.project_name,
        "org_project_hash": job.org_project_hash.hex(),
        "status": current_status.status,
        "statuses": [item.to_primitive() for item in history.all],
        "history": {
            "status": current_status.status,
            "reason": current_status.reason,
            "description": current_status.description,
            "created_at": history.created_at_str,
            "run_time_seconds": job.get_run_time().total_seconds(),
            "restarts": history.restart_count,
        },
        "container": convert_job_container_to_json(job.request.container),
        "scheduler_enabled": job.scheduler_enabled,
        "preemptible_node": job.preemptible_node,
        "is_preemptible": job.scheduler_enabled,
        "is_preemptible_node_required": job.preemptible_node,
        "pass_config": job.pass_config,
        "uri": str(job.to_uri()),
        "restart_policy": str(job.restart_policy),
        "privileged": job.privileged,
        "materialized": job.materialized,
        "being_dropped": job.being_dropped,
        "logs_removed": job.logs_removed,
        "total_price_credits": str(job.get_total_price_credits()),
        "price_credits_per_hour": str(job.price_credits_per_hour),
        "priority": job.priority.to_name(),
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
    if job.org_name:
        response_payload["org_name"] = job.org_name
    if job.energy_schedule_name:
        response_payload["energy_schedule_name"] = job.energy_schedule_name
    return response_payload


def infer_permissions_from_container(
    container: Container,
    registry_host: str,
    cluster_name: str,
    org_name: str | None,
    *,
    project_name: str,
) -> list[Permission]:
    permissions = [
        Permission(
            uri=str(make_job_uri(cluster_name, project_name, org_name)),
            action="write",
        )
    ]
    if container.belongs_to_registry(registry_host):
        permissions.append(
            Permission(
                uri=str(container.to_image_uri(registry_host, cluster_name)),
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
        permission = Permission(uri=disk_volume.disk.to_permission_uri(), action=action)
        permissions.append(permission)
    return permissions


def make_job_uri(
    cluster_name: str,
    project_name: str,
    org_name: str | None,
) -> URL:
    return URL.build(scheme="job", host=cluster_name) / (org_name or "") / project_name


class JobsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

        self._job_filter_factory = JobFilterFactory()
        self._job_response_validator = create_job_response_validator()
        self._job_set_status_validator = create_job_set_status_validator()
        self._job_set_materialized_validator = create_job_set_materialized_validator()
        self._job_update_run_time_validator = (
            create_job_update_max_run_time_minutes_validator()
        )
        self._drop_progress_validator = create_drop_progress_validator()
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
                aiohttp.web.put("/{job_id}/materialized", self.handle_put_materialized),
                aiohttp.web.put(
                    "/{job_id}/max_run_time_minutes",
                    self.handle_put_max_run_time_minutes,
                ),
                aiohttp.web.post("/{job_id}/drop", self.handle_drop_job),
                aiohttp.web.post("/{job_id}/drop_progress", self.handle_drop_progress),
            )
        )

    def _check_user_can_submit_jobs(
        self, user_cluster_configs: Sequence[UserClusterConfig]
    ) -> None:
        if not user_cluster_configs:
            raise aiohttp.web.HTTPForbidden(
                text=json.dumps({"error": "No clusters"}),
                content_type="application/json",
            )

    async def _create_job_request_validator(
        self,
        cluster_config: Cluster,
        allow_flat_structure: bool = False,
        org_name: str | None = None,
    ) -> t.Trafaret:
        return create_job_request_validator(
            allow_flat_structure=allow_flat_structure,
            allowed_tpu_resources=cluster_config.orchestrator.tpu_resources,
            cluster_name=cluster_config.name,
            org_name=org_name,
            storage_scheme=STORAGE_URI_SCHEME,
            allowed_energy_schedule_names=cluster_config.energy.schedule_names,
        )

    def _get_cluster_config(
        self,
        user_cluster_configs: Sequence[UserClusterConfig],
        cluster_name: str,
        org_name: str | None,
    ) -> Cluster:
        for user_cluster_config in user_cluster_configs:
            if user_cluster_config.config.name == cluster_name:
                if org_name in user_cluster_config.orgs:
                    return user_cluster_config.config
                raise aiohttp.web.HTTPForbidden(
                    text=json.dumps(
                        {
                            "error": (
                                "User is not allowed to submit jobs to the specified "
                                "cluster as a member of given organization"
                            )
                        }
                    ),
                    content_type="application/json",
                )
        raise aiohttp.web.HTTPForbidden(
            text=json.dumps(
                {
                    "error": (
                        "User is not allowed to submit jobs to the specified cluster"
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
        default_cluster_name = cluster_configs[0].config.name
        cluster_for_default_org = (
            orig_payload.get("cluster_name") or default_cluster_name
        )
        cluster_config_for_default_org = next(
            (
                cluster_config
                for cluster_config in cluster_configs
                if cluster_config.config.name == cluster_for_default_org
            ),
            None,
        )
        # Use first org from user's cluster access as default
        if cluster_config_for_default_org is None:
            raise aiohttp.web.HTTPForbidden(text="User must have cluster access")
        if not cluster_config_for_default_org.orgs:
            raise aiohttp.web.HTTPForbidden(text="User must have at least one org")
        default_org_name = cluster_config_for_default_org.orgs[0]

        job_cluster_org_name_validator = create_job_cluster_org_name_validator(
            default_cluster_name=default_cluster_name,
            default_org_name=default_org_name,
            default_project_name=get_base_owner(user.name),
        )
        request_payload = job_cluster_org_name_validator.check(orig_payload)
        cluster_name = request_payload["cluster_name"]
        org_name = request_payload["org_name"]
        project_name = request_payload["project_name"]
        cluster_config = self._get_cluster_config(
            cluster_configs, cluster_name, org_name
        )

        if "from_preset" in request.query:
            job_preset_validator = create_job_preset_validator(
                cluster_config.orchestrator.resource_presets
            )
            request_payload = job_preset_validator.check(request_payload)
            allow_flat_structure = True
        else:
            allow_flat_structure = False

        job_request_validator = await self._create_job_request_validator(
            cluster_config, org_name=org_name, allow_flat_structure=allow_flat_structure
        )
        request_payload = job_request_validator.check(request_payload)

        container = create_container_from_payload(request_payload)

        permissions = infer_permissions_from_container(
            container,
            cluster_config.registry.host,
            cluster_name,
            org_name,
            project_name=project_name,
        )
        await check_permissions(request, permissions)

        name = request_payload.get("name")
        preset_name = request_payload.get("preset_name")
        tags = sorted(set(request_payload.get("tags", [])))
        description = request_payload.get("description")
        scheduler_enabled = request_payload["scheduler_enabled"]
        preemptible_node = request_payload.get("preemptible_node", False)
        pass_config = request_payload["pass_config"]
        privileged = request_payload["privileged"]
        schedule_timeout = request_payload.get("schedule_timeout")
        max_run_time_minutes = request_payload.get("max_run_time_minutes")
        wait_for_jobs_quota = request_payload.get("wait_for_jobs_quota")
        priority = request_payload["priority"]
        job_request = JobRequest.create(container, description)
        job, _ = await self._jobs_service.create_job(
            job_request,
            user=user,
            cluster_name=cluster_name,
            org_name=org_name,
            project_name=project_name,
            job_name=name,
            preset_name=preset_name,
            tags=tags,
            scheduler_enabled=scheduler_enabled,
            preemptible_node=preemptible_node,
            pass_config=pass_config,
            wait_for_jobs_quota=wait_for_jobs_quota,
            privileged=privileged,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
            restart_policy=request_payload["restart_policy"],
            priority=priority,
            energy_schedule_name=request_payload.get("energy_schedule_name"),
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
        await check_permissions(request, [permissions])
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
                query_filter=self._job_filter_factory.create_from_query(
                    request.query  # type: ignore
                ),
                access_tree=tree,
            ).build()
        except JobFilterException:
            bulk_job_filter = BulkJobFilter(
                bulk_filter=None, shared_ids=set(), shared_ids_filter=None
            )

        reverse = _parse_bool(request.query.get("reverse", "0"))
        limit: int | None = None
        if "limit" in request.query:
            limit = int(request.query["limit"])
            if limit <= 0:
                raise ValueError("limit should be > 0")

        async with self._iter_filtered_jobs(bulk_job_filter, reverse, limit) as jobs:
            if limit is not None:

                async def limit_filter(
                    it: AsyncIterator[Job], count: int
                ) -> AsyncIterator[Job]:
                    async for x in it:
                        yield x
                        count -= 1
                        if not count:
                            break

                jobs = limit_filter(jobs, limit)

            if self._accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                try:
                    async for job in jobs:
                        response_payload = convert_job_to_job_response(job)
                        self._job_response_validator.check(response_payload)
                        await response.write(
                            json.dumps(response_payload).encode() + b"\n"
                        )
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

            response_payload = {
                "jobs": [convert_job_to_job_response(job) async for job in jobs]
            }
            self._bulk_jobs_response_validator.check(response_payload)
            return aiohttp.web.json_response(
                data=response_payload, status=aiohttp.web.HTTPOk.status_code
            )

    @asyncgeneratorcontextmanager
    async def _iter_filtered_jobs(
        self, bulk_job_filter: "BulkJobFilter", reverse: bool, limit: int | None
    ) -> AsyncIterator[Job]:
        def job_key(job: Job) -> tuple[float, str, Job]:
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
                async with self._jobs_service.iter_all_jobs(
                    bulk_job_filter.bulk_filter, reverse=reverse, limit=limit
                ) as it:
                    async for job in it:
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
        await self._jobs_service.cancel_job(job.id, JobStatusReason.USER_REQUESTED)
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

    async def handle_put_materialized(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        assert job.cluster_name
        permission = Permission(uri=f"job://{job.cluster_name}", action="manage")
        await check_permissions(request, [permission])

        orig_payload = await request.json()
        request_payload = self._job_set_materialized_validator.check(orig_payload)

        try:
            await self._jobs_service.set_job_materialized(
                job_id=job_id,
                materialized=request_payload["materialized"],
            )
        except JobStorageTransactionError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPConflict.status_code
            )
        except JobError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPBadRequest.status_code
            )
        else:
            raise aiohttp.web.HTTPNoContent()

    async def handle_put_max_run_time_minutes(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job = await self._resolve_job(request, "write")

        orig_payload = await request.json()
        request_payload = self._job_update_run_time_validator.check(orig_payload)

        try:
            await self._jobs_service.update_max_run_time(
                job_id=job.id,
                max_run_time_minutes=request_payload.get("max_run_time_minutes"),
                additional_max_run_time_minutes=request_payload.get(
                    "additional_max_run_time_minutes"
                ),
            )
        except JobStorageTransactionError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPConflict.status_code
            )
        except JobError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPBadRequest.status_code
            )
        else:
            raise aiohttp.web.HTTPNoContent()

    async def handle_drop_job(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job = await self._resolve_job(request, "write")
        try:
            await self._jobs_service.drop_job(job.id)
        except JobError as e:
            payload = {"error": str(e)}
            return aiohttp.web.json_response(
                payload, status=aiohttp.web.HTTPBadRequest.status_code
            )
        else:
            raise aiohttp.web.HTTPNoContent()

    async def handle_drop_progress(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job = await self._resolve_job(request, "write")

        orig_payload = await request.json()
        request_payload = self._drop_progress_validator.check(orig_payload)

        await self._jobs_service.drop_progress(
            job.id, logs_removed=request_payload.get("logs_removed")
        )
        raise aiohttp.web.HTTPNoContent()


class JobFilterException(ValueError):
    pass


class JobFilterFactory:
    def __init__(self) -> None:
        self._job_name_validator = create_job_name_validator()
        self._user_name_validator = create_user_name_validator()
        self._base_owner_name_validator = create_base_owner_name_validator()
        self._cluster_name_validator = create_cluster_name_validator()
        self._org_name_validator = create_org_name_validator()
        self._project_name_validator = create_project_name_validator()
        self._org_project_hash_validator = create_org_project_hash_validator()

    def create_from_query(self, query: MultiDictProxy) -> JobFilter:  # type: ignore
        statuses = {JobStatus(s) for s in query.getall("status", [])}
        tags = set(query.getall("tag", []))
        hostname = query.get("hostname")
        bool_filters = {}
        for name in ["materialized", "being_dropped", "logs_removed"]:
            if name in query:
                bool_filters[name] = _parse_bool(query[name])
        if hostname is None:
            job_name = self._job_name_validator.check(query.get("name"))
            owners = {
                self._user_name_validator.check(owner)
                for owner in query.getall("owner", [])
            }
            base_owners = {
                self._base_owner_name_validator.check(owner)
                for owner in query.getall("base_owner", [])
            }
            clusters: ClusterOrgProjectNameSet = {
                self._cluster_name_validator.check(cluster_name): {}
                for cluster_name in query.getall("cluster_name", [])
            }
            orgs = {
                self._org_name_validator.check(org_name)
                for org_name in query.getall("org_name", [])
            }
            projects = {
                self._project_name_validator.check(project_name)
                for project_name in query.getall("project_name", [])
            }
            since = query.get("since")
            until = query.get("until")
            return JobFilter(
                statuses=statuses,
                clusters=clusters,
                orgs=orgs,
                owners=owners,
                base_owners=base_owners,
                projects=projects,
                name=job_name,
                tags=tags,
                since=iso8601.parse_date(since) if since else JobFilter.since,
                until=iso8601.parse_date(until) if until else JobFilter.until,
                **bool_filters,  # type: ignore
            )

        for key in ("name", "project_name", "cluster_name", "since", "until"):
            if key in query:
                raise ValueError("Invalid request")

        label = hostname.partition(".")[0]
        job_name, sep, org_project_hash = label.rpartition(JOB_NAME_SEPARATOR)
        if not sep:
            return JobFilter(
                statuses=statuses,
                ids={label},
                tags=tags,
                **bool_filters,  # type: ignore
            )
        job_name = self._job_name_validator.check(job_name)
        org_project_hash = self._org_project_hash_validator.check(org_project_hash)
        return JobFilter(
            statuses=statuses,
            org_project_hash=org_project_hash,
            name=job_name,
            tags=tags,
            **bool_filters,  # type: ignore
        )


@dataclass(frozen=True)
class BulkJobFilter:
    bulk_filter: JobFilter | None

    shared_ids: set[str]
    shared_ids_filter: JobFilter | None


class BulkJobFilterBuilder:
    def __init__(
        self, query_filter: JobFilter, access_tree: ClientSubTreeViewRoot
    ) -> None:
        self._query_filter = query_filter
        self._access_tree = access_tree

        self._has_access_to_all: bool = False
        self._has_clusters_shared_all: bool = False
        self._has_orgs_shared_all: bool = False
        self._clusters_shared_any: dict[str, dict[str, dict[str, set[str]]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        )
        self._projects_shared_any: set[str] = set()
        self._orgs_shared_any: set[str] = set()
        self._shared_ids: set[str] = set()

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
            and not self._orgs_shared_any
            and not self._projects_shared_any
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
                self._traverse_orgs(sub_tree, cluster_name)

    def _traverse_orgs(self, tree: ClientAccessSubTreeView, cluster_name: str) -> None:
        for org_name, sub_tree in tree.children.items():
            if not sub_tree.can_list():
                continue

            if self._query_filter.orgs and org_name not in self._query_filter.orgs:
                # skipping owners
                continue

            if sub_tree.can_read():
                # read/write/manage access to all org's jobs =
                # job://cluster/org
                self._has_orgs_shared_all = True
                self._orgs_shared_any.add(org_name)
                self._clusters_shared_any[cluster_name][org_name] = {}
            else:
                # specific ids or names
                self._traverse_projects(sub_tree, cluster_name, org_name)

    def _traverse_projects(
        self,
        tree: ClientAccessSubTreeView,
        cluster_name: str,
        org_name: str,
    ) -> None:
        for project, sub_tree in tree.children.items():
            if not sub_tree.can_list():
                continue

            if (
                self._query_filter.projects
                and project not in self._query_filter.projects
            ):
                # skipping owners
                continue

            if sub_tree.can_read():
                # read/write/manage access to all jobs within the project =
                # job://cluster/project or job://cluster/org/project
                self._orgs_shared_any.add(org_name)
                self._projects_shared_any.add(project)
                self._clusters_shared_any[cluster_name][org_name][project] = set()
            else:
                # specific ids or names
                self._traverse_jobs(sub_tree, cluster_name, org_name, project)

    def _traverse_jobs(
        self,
        tree: ClientAccessSubTreeView,
        cluster_name: str,
        org_name: str,
        project_name: str,
    ) -> None:
        for name, sub_tree in tree.children.items():
            if sub_tree.children.keys():
                continue  # Not a leaf node
            if sub_tree.can_read():
                if maybe_job_id(name):
                    # NOTE: it is not entirely clear why we do not add the project to
                    # `_projects_shared_any` here.
                    self._shared_ids.add(name)
                    continue

                if self._query_filter.name and name != self._query_filter.name:
                    # skipping name
                    continue

                self._orgs_shared_any.add(org_name)
                self._projects_shared_any.add(project_name)
                self._clusters_shared_any[cluster_name][org_name][project_name].add(
                    name
                )

    def _create_bulk_filter(self) -> JobFilter | None:
        if not (
            self._has_access_to_all
            or self._clusters_shared_any
            or self._orgs_shared_any
            or self._projects_shared_any
        ):
            return None
        bulk_filter = self._query_filter
        # `self._orgs_shared_any` is already filtered against
        # `self._query_filter.orgs`.
        # if `self._orgs_shared_any` is empty and no clusters share full
        # access, we still want to try to limit the scope to the orgs
        # passed in the query, otherwise pull all.
        if not self._has_clusters_shared_all and self._orgs_shared_any:
            bulk_filter = replace(bulk_filter, orgs=self._orgs_shared_any)
        # `self._projects_shared_any` is already filtered against
        # `self._query_filter.projects`.
        # if `self._projects_shared_any` is empty and no org share full
        # access, we still want to try to limit the scope to the projects
        # passed in the query, otherwise pull all.
        if (
            not self._has_clusters_shared_all
            and not self._has_orgs_shared_all
            and self._projects_shared_any
        ):
            bulk_filter = replace(bulk_filter, projects=self._projects_shared_any)
        # `self._clusters_shared_any` is already filtered against
        # `self._query_filter.clusters`.
        # if `self._clusters_shared_any` is empty, we still want to try to limit
        # the scope to the clusters passed in the query, otherwise pull all.
        if not self._has_access_to_all:
            self._optimize_clusters_projects(
                bulk_filter.orgs, bulk_filter.projects, bulk_filter.name
            )
            bulk_filter = replace(
                bulk_filter,
                clusters={
                    cluster_name: {
                        org_name: dict(projects) for org_name, projects in orgs.items()
                    }
                    for cluster_name, orgs in self._clusters_shared_any.items()
                },
            )
        return bulk_filter

    def _optimize_clusters_projects(
        self,
        orgs: AbstractSet[str],
        projects: AbstractSet[str],
        name: str | None,
    ) -> None:
        if orgs or projects or name:
            names = {name}
            for cluster_orgs in self._clusters_shared_any.values():
                for org_projects in cluster_orgs.values():
                    if name:
                        for project_names in org_projects.values():
                            if project_names == names:
                                project_names.clear()
                    if (
                        projects
                        and org_projects.keys() == projects
                        and not any(org_projects.values())
                    ):
                        org_projects.clear()
                if (
                    orgs
                    and cluster_orgs.keys() == orgs
                    and not any(cluster_orgs.values())
                ):
                    cluster_orgs.clear()


def _parse_bool(value: str) -> bool:
    value = value.lower()
    if value in ("0", "false"):
        return False
    if value in ("1", "true"):
        return True

    raise ValueError('Required "0", "1", "false" or "true"')


def _permission_to_primitive(perm: Permission) -> dict[str, str]:
    return {"uri": perm.uri, "action": perm.action}


def _job_uri_with_name(uri: URL, name: str) -> URL:
    assert name
    assert uri.host
    assert uri.name
    return uri.with_name(name)
