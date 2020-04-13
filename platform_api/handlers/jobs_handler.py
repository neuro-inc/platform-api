import json
import logging
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
)
from platform_api.orchestrator.job_request import (
    Container,
    ContainerVolume,
    JobRequest,
    JobStatus,
)
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobFilter, JobStorageTransactionError
from platform_api.resource import TPUResource
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
    allowed_gpu_models: Sequence[str],
    allowed_tpu_resources: Sequence[TPUResource],
    cluster_name: str,
) -> t.Trafaret:
    return t.Dict(
        {
            "container": create_container_request_validator(
                allow_volumes=True,
                allowed_gpu_models=allowed_gpu_models,
                allowed_tpu_resources=allowed_tpu_resources,
            ),
            t.Key("name", optional=True): create_job_name_validator(),
            t.Key("description", optional=True): t.String,
            t.Key("tags", optional=True): t.List(
                create_job_tag_validator(), max_length=16
            ),
            t.Key("is_preemptible", optional=True, default=False): t.Bool,
            t.Key("schedule_timeout", optional=True): t.Float(gte=1, lt=30 * 24 * 3600),
            t.Key("max_run_time_minutes", optional=True): t.Int(gte=0),
            t.Key("cluster_name", default=cluster_name): t.Atom(cluster_name),
            t.Key("restart_policy", default=str(JobRestartPolicy.NEVER)): t.Enum(
                *[str(policy) for policy in JobRestartPolicy]
            )
            >> JobRestartPolicy,
        }
    )


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
            "ssh_server": t.String,
            "ssh_auth_server": t.String,  # deprecated
            "history": create_job_history_validator(),
            "container": create_container_response_validator(),
            "is_preemptible": t.Bool,
            t.Key("internal_hostname", optional=True): t.String,
            t.Key("name", optional=True): create_job_name_validator(max_length=None),
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
    if container.ssh_server is not None:
        ret["ssh"] = {"port": container.ssh_server.port}
    for volume in container.volumes:
        ret["volumes"].append(convert_container_volume_to_json(volume, storage_config))
    if container.tty:
        ret["tty"] = True
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
    job: Job, use_cluster_names_in_uris: bool = True
) -> Dict[str, Any]:
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
        },
        "container": convert_job_container_to_json(
            job.request.container, job.storage_config
        ),
        "ssh_server": job.ssh_server,
        "ssh_auth_server": job.ssh_server,  # deprecated
        "is_preemptible": job.is_preemptible,
        "uri": str(job.to_uri(use_cluster_names_in_uris)),
        "restart_policy": str(job.restart_policy),
    }
    if job.name:
        response_payload["name"] = job.name
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
    cluster_name: Optional[str],
    legacy_storage_acl: bool = False,
) -> List[Permission]:
    permissions = [Permission(uri=str(user.to_job_uri(cluster_name)), action="write")]
    if container.belongs_to_registry(registry_config):
        permissions.append(
            Permission(
                uri=str(container.to_image_uri(registry_config, cluster_name)),
                action="read",
            )
        )
    for volume in container.volumes:
        action = "read" if volume.read_only else "write"
        uri = volume.uri
        # XXX (serhiy 04-Feb-2020) Temporary patch the URI
        if cluster_name and not legacy_storage_acl:
            if uri.scheme == "storage" and uri.host != cluster_name:
                if uri.host:
                    assert uri.path[0] == "/"
                    uri = uri.with_path(f"/{uri.host}{uri.path}")
                    uri = uri.with_host(cluster_name)
                elif uri.path != "/":
                    assert uri.path == "" or uri.path[0] == "/"
                    uri = URL(f"{uri.scheme}://{cluster_name}{uri.path}")
                else:
                    uri = URL(f"{uri.scheme}://{cluster_name}")
        permission = Permission(uri=str(uri), action=action)
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
        self, cluster_config: ClusterConfig
    ) -> t.Trafaret:
        # TODO: rework `gpu_models` to be retrieved from `cluster_config`
        gpu_models = await self._jobs_service.get_available_gpu_models(
            cluster_config.name
        )
        return create_job_request_validator(
            allowed_gpu_models=gpu_models,
            allowed_tpu_resources=cluster_config.orchestrator.tpu_resources,
            cluster_name=cluster_config.name,
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
        job_request_validator = await self._create_job_request_validator(cluster_config)
        request_payload = job_request_validator.check(request_payload)

        container = create_container_from_payload(
            request_payload["container"],
            storage_config=cluster_config.storage,
            cluster_name=cluster_name,
        )

        if self._config.use_cluster_names_in_uris:
            permissions = infer_permissions_from_container(
                user, container, cluster_config.registry, cluster_name,
            )
            try:
                await check_permissions(request, permissions)
            except aiohttp.web.HTTPForbidden as exc:
                permissions = infer_permissions_from_container(
                    user,
                    container,
                    cluster_config.registry,
                    cluster_name,
                    legacy_storage_acl=True,
                )
                try:
                    await check_permissions(request, permissions)
                except aiohttp.web.HTTPForbidden:
                    # Re-raise the original exception containing information
                    # about extended ACL.
                    raise exc
        else:
            permissions = infer_permissions_from_container(
                user, container, cluster_config.registry, None,
            )
            await check_permissions(request, permissions)

        name = request_payload.get("name")
        tags = sorted(set(request_payload.get("tags", [])))
        description = request_payload.get("description")
        is_preemptible = request_payload["is_preemptible"]
        schedule_timeout = request_payload.get("schedule_timeout")
        max_run_time_minutes = request_payload.get("max_run_time_minutes")
        job_request = JobRequest.create(container, description)
        job, _ = await self._jobs_service.create_job(
            job_request,
            user=user,
            cluster_name=cluster_name,
            job_name=name,
            tags=tags,
            is_preemptible=is_preemptible,
            schedule_timeout=schedule_timeout,
            max_run_time_minutes=max_run_time_minutes,
            restart_policy=request_payload["restart_policy"],
        )
        response_payload = convert_job_to_job_response(
            job, self._config.use_cluster_names_in_uris
        )
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code
        )

    async def handle_get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(
            uri=str(job.to_uri(self._config.use_cluster_names_in_uris)), action="read"
        )
        await check_permissions(request, [permission])

        response_payload = convert_job_to_job_response(
            job, self._config.use_cluster_names_in_uris
        )
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
                use_cluster_names_in_uris=self._config.use_cluster_names_in_uris,
            ).build()
        except JobFilterException:
            bulk_job_filter = BulkJobFilter(
                bulk_filter=None, shared_ids=set(), shared_ids_filter=None,
            )

        if self._accepts_ndjson(request):
            response = aiohttp.web.StreamResponse()
            response.headers["Content-Type"] = "application/x-ndjson"
            await response.prepare(request)
            async for job in self._iter_filtered_jobs(bulk_job_filter):
                response_payload = convert_job_to_job_response(
                    job, self._config.use_cluster_names_in_uris
                )
                self._job_response_validator.check(response_payload)
                await response.write(json.dumps(response_payload).encode() + b"\n")
            await response.write_eof()
            return response
        else:
            response_payload = {
                "jobs": [
                    convert_job_to_job_response(
                        job, self._config.use_cluster_names_in_uris,
                    )
                    async for job in self._iter_filtered_jobs(bulk_job_filter)
                ]
            }
            self._bulk_jobs_response_validator.check(response_payload)
            return aiohttp.web.json_response(
                data=response_payload, status=aiohttp.web.HTTPOk.status_code
            )

    async def _iter_filtered_jobs(
        self, bulk_job_filter: "BulkJobFilter"
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
                shared_jobs.sort(reverse=True)
        else:
            shared_jobs = []

        if bulk_job_filter.bulk_filter:
            with log_debug_time(f"Read bulk jobs with {bulk_job_filter.bulk_filter}"):
                async for job in self._jobs_service.iter_all_jobs(
                    bulk_job_filter.bulk_filter
                ):
                    key = job_key(job)
                    # Merge shared jobs and bulk jobs in the creation order
                    while shared_jobs and shared_jobs[-1] < key:
                        yield shared_jobs.pop()[-1]
                    yield job

        for key in reversed(shared_jobs):
            yield key[-1]

    async def handle_delete(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        permission = Permission(
            uri=str(job.to_uri(self._config.use_cluster_names_in_uris)), action="write"
        )
        await check_permissions(request, [permission])

        await self._jobs_service.delete_job(job_id)
        raise aiohttp.web.HTTPNoContent()

    async def handle_put_status(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        job_id = request.match_info["job_id"]
        job = await self._jobs_service.get_job(job_id)

        if self._config.use_cluster_names_in_uris and job.cluster_name:
            permission = Permission(uri=f"job://{job.cluster_name}", action="manage")
        else:
            permission = Permission(uri="job:", action="manage")
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
            clusters: Dict[Any, AbstractSet[Any]] = {
                self._cluster_name_validator.check(cluster_name): set()
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
        self,
        query_filter: JobFilter,
        access_tree: ClientSubTreeViewRoot,
        use_cluster_names_in_uris: bool = True,
    ) -> None:
        self._query_filter = query_filter
        self._access_tree = access_tree
        self._use_cluster_names_in_uris = use_cluster_names_in_uris

        self._has_access_to_all: bool = False
        self._has_clusters_shared_all: bool = False
        self._clusters_shared_any: Dict[str, Set[str]] = {}
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
        tree = self._access_tree.sub_tree

        if not tree.can_list():
            # no job resources whatsoever
            raise JobFilterException("no jobs")

        if tree.can_read():
            # read access to all jobs = job:
            self._has_access_to_all = True
            return

        if self._use_cluster_names_in_uris:
            self._traverse_clusters(tree)
        else:
            self._traverse_owners(tree, "")

        if (
            not self._clusters_shared_any
            and not self._owners_shared_all
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
                self._clusters_shared_any[cluster_name] = set()
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
                self._owners_shared_all.add(owner)
                if cluster_name:
                    if cluster_name not in self._clusters_shared_any:
                        self._clusters_shared_any[cluster_name] = set()
                    self._clusters_shared_any[cluster_name].add(owner)
            else:
                # specific ids
                self._traverse_jobs(sub_tree)

    def _traverse_jobs(self, tree: ClientAccessSubTreeView) -> None:
        self._shared_ids.update(
            job_id for job_id, sub_tree in tree.children.items() if sub_tree.can_read()
        )

    def _create_bulk_filter(self) -> Optional[JobFilter]:
        if not (
            self._has_access_to_all
            or self._clusters_shared_any
            or self._owners_shared_all
        ):
            return None
        bulk_filter = self._query_filter
        # `self._owners_shared_all` is already filtered against
        # `self._query_filter.owners`.
        # if `self._owners_shared_all` is empty and no clusters share full
        # access, we still want to try to limit the scope to the owners
        # passed in the query, otherwise pull all.
        if not self._has_clusters_shared_all and self._owners_shared_all:
            bulk_filter = replace(bulk_filter, owners=self._owners_shared_all)
        # `self._clusters_shared_any` is already filtered against
        # `self._query_filter.clusters`.
        # if `self._clusters_shared_any` is empty, we still want to try to limit
        # the scope to the clusters passed in the query, otherwise pull all.
        if not self._has_access_to_all:
            self._optimize_clusters_owners(bulk_filter.owners)
            bulk_filter = replace(bulk_filter, clusters=self._clusters_shared_any)
        return bulk_filter

    def _optimize_clusters_owners(self, owners: AbstractSet[str]) -> None:
        if owners:
            for cluster_owners in self._clusters_shared_any.values():
                if cluster_owners == owners:
                    cluster_owners.clear()
