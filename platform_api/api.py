import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from typing import Any

import aiohttp.web
from aiohttp.web import HTTPUnauthorized
from aiohttp.web_urldispatcher import AbstractRoute
from apolo_events_client import from_config as create_events_client_from_config
from neuro_admin_client import AdminClient, OrgUser, ProjectUser
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_config_client import (
    AppsConfig,
    Cluster,
    ConfigClient,
    EnergySchedule,
    EnergySchedulePeriod,
    ResourcePoolType,
    ResourcePreset,
    VolumeConfig,
)
from neuro_logging import init_logging, setup_sentry
from neuro_notifications_client import Client as NotificationsClient

from platform_api import __version__
from platform_api.orchestrator.job_policy_enforcer import (
    CreditsLimitEnforcer,
    JobPolicyEnforcePoller,
    RetentionPolicyEnforcer,
    RuntimeLimitEnforcer,
    StopOnClusterRemoveEnforcer,
)

from .cluster import ClusterConfigRegistry, ClusterUpdater
from .config import Config
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler
from .orchestrator.job_request import JobError, JobException
from .orchestrator.jobs_service import (
    JobsService,
    JobsServiceException,
    UserClusterConfig,
)
from .orchestrator.jobs_storage import JobsStorage, PostgresJobsStorage
from .orchestrator.jobs_storage.base import JobStorageTransactionError
from .postgres import make_async_engine
from .project_deleter import ProjectDeleter
from .user import authorized_user

logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


class ConfigApiHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes((aiohttp.web.get("", self.handle_config),))

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    async def handle_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Return platform configuration.

        If the requesting user is authorized, the response will contain the details
        about the user's orgs, clusters, and projects.

        In case the user has direct access to a cluster outside of any org,
        the list of orgs will not have a None entry, but the cluster will have a None
        entry in its orgs list.

        Similarly, a project in the response can have a None org.
        """
        data: dict[str, Any] = {}

        try:
            user = await authorized_user(request)
            data["authorized"] = True
            user_config = await self._jobs_service.get_user_config(user)
            data["orgs"] = [
                self._convert_org_user_to_payload(o) for o in user_config.orgs
            ]
            data["clusters"] = [
                self._convert_cluster_config_to_payload(c) for c in user_config.clusters
            ]
            data["projects"] = [
                self._convert_project_user_to_payload(p) for p in user_config.projects
            ]

            if self._config.admin_public_url:
                data["admin_url"] = str(self._config.admin_public_url)
        except HTTPUnauthorized:
            data["authorized"] = False

        if self._config.oauth:
            data["auth_url"] = str(self._config.oauth.auth_url)
            data["token_url"] = str(self._config.oauth.token_url)
            data["logout_url"] = str(self._config.oauth.logout_url)
            data["client_id"] = self._config.oauth.client_id
            data["audience"] = self._config.oauth.audience
            data["callback_urls"] = [str(u) for u in self._config.oauth.callback_urls]
            data["headless_callback_url"] = str(
                self._config.oauth.headless_callback_url
            )
            redirect_url = self._config.oauth.success_redirect_url
            if redirect_url:
                data["success_redirect_url"] = str(redirect_url)

        return aiohttp.web.json_response(data)

    def _convert_org_user_to_payload(self, org_user: OrgUser) -> dict[str, Any]:
        return {
            "name": org_user.org_name,
            "role": str(org_user.role),
        }

    def _convert_cluster_config_to_payload(
        self, user_cluster_config: UserClusterConfig
    ) -> dict[str, Any]:
        cluster_config = user_cluster_config.config
        orgs = user_cluster_config.orgs
        resource_pool_types = [
            self._convert_resource_pool_type_to_payload(r)
            for r in cluster_config.orchestrator.resource_pool_types
        ]
        presets = [
            self._convert_preset_to_payload(preset)
            for preset in cluster_config.orchestrator.resource_presets
        ]
        result = {
            "name": cluster_config.name,
            "registry_url": str(cluster_config.registry.url),
            "storage_url": str(cluster_config.storage.url),
            "monitoring_url": str(cluster_config.monitoring.url),
            "secrets_url": str(cluster_config.secrets.url),
            "metrics_url": str(cluster_config.metrics.url),
            "disks_url": str(cluster_config.disks.url),
            "buckets_url": str(cluster_config.buckets.url),
            "resource_pool_types": resource_pool_types,
            "resource_presets": presets,
            "orgs": orgs,
            "timezone": str(cluster_config.timezone),
            "energy_schedules": [
                self._convert_energy_schedule_to_payload(schedule)
                for schedule in cluster_config.energy.schedules
            ],
            "storage_volumes": [
                self._convert_storage_volume_to_payload(volume)
                for volume in cluster_config.storage.volumes
            ],
            "apps": self._convert_apps_config_to_payload(cluster_config.apps),
        }
        if cluster_config.location:
            result["location"] = cluster_config.location
        if cluster_config.logo_url:
            result["logo_url"] = str(cluster_config.logo_url)
        if self._config.auth.public_endpoint_url:
            result["users_url"] = str(self._config.auth.public_endpoint_url)
        return result

    def _convert_resource_pool_type_to_payload(  # noqa: C901
        self, resource_pool_type: ResourcePoolType
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": resource_pool_type.name,
            "min_size": resource_pool_type.min_size,
            "max_size": resource_pool_type.max_size,
            "cpu": resource_pool_type.cpu,
            "available_cpu": resource_pool_type.available_cpu or resource_pool_type.cpu,
            "memory": resource_pool_type.memory,
            "available_memory": (
                resource_pool_type.available_memory or resource_pool_type.memory
            ),
            "disk_size": resource_pool_type.disk_size,
            "available_disk_size": (
                resource_pool_type.available_disk_size or resource_pool_type.disk_size
            ),
        }
        if resource_pool_type.idle_size:
            payload["idle_size"] = resource_pool_type.idle_size
        if resource_pool_type.nvidia_gpu is not None:
            payload["nvidia_gpu"] = {
                "count": resource_pool_type.nvidia_gpu.count,
                "model": resource_pool_type.nvidia_gpu.model,
            }
            if resource_pool_type.nvidia_gpu.memory:
                payload["nvidia_gpu"]["memory"] = resource_pool_type.nvidia_gpu.memory
        if resource_pool_type.amd_gpu is not None:
            payload["amd_gpu"] = {
                "count": resource_pool_type.amd_gpu.count,
                "model": resource_pool_type.amd_gpu.model,
            }
            if resource_pool_type.amd_gpu.memory:
                payload["amd_gpu"]["memory"] = resource_pool_type.amd_gpu.memory
        if resource_pool_type.intel_gpu is not None:
            payload["intel_gpu"] = {
                "count": resource_pool_type.intel_gpu.count,
                "model": resource_pool_type.intel_gpu.model,
            }
            if resource_pool_type.intel_gpu.memory:
                payload["intel_gpu"]["memory"] = resource_pool_type.intel_gpu.memory
        if resource_pool_type.tpu:
            payload["tpu"] = {
                "types": resource_pool_type.tpu.types,
                "software_versions": resource_pool_type.tpu.software_versions,
                "ipv4_cidr_block": resource_pool_type.tpu.ipv4_cidr_block,
            }
        if resource_pool_type.is_preemptible:
            payload["is_preemptible"] = resource_pool_type.is_preemptible
        if resource_pool_type.cpu_min_watts:
            payload["cpu_min_watts"] = resource_pool_type.cpu_min_watts
        if resource_pool_type.cpu_max_watts:
            payload["cpu_max_watts"] = resource_pool_type.cpu_max_watts
        return payload

    def _convert_preset_to_payload(self, preset: ResourcePreset) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": preset.name,
            "credits_per_hour": str(preset.credits_per_hour),
            "cpu": preset.cpu,
            "memory": preset.memory,
            "memory_mb": preset.memory // 2**20,
            "scheduler_enabled": preset.scheduler_enabled,
            "preemptible_node": preset.preemptible_node,
            "is_preemptible": preset.scheduler_enabled,
            "is_preemptible_node_required": preset.preemptible_node,
        }
        if preset.nvidia_gpu is not None:
            payload["nvidia_gpu"] = {
                "count": preset.nvidia_gpu.count,
            }
            if preset.nvidia_gpu.model:
                payload["nvidia_gpu"]["model"] = preset.nvidia_gpu.model
            if preset.nvidia_gpu.memory:
                payload["nvidia_gpu"]["memory"] = preset.nvidia_gpu.memory
        if preset.amd_gpu is not None:
            payload["amd_gpu"] = {
                "count": preset.amd_gpu.count,
            }
            if preset.amd_gpu.model:
                payload["amd_gpu"]["model"] = preset.amd_gpu.model
            if preset.amd_gpu.memory:
                payload["amd_gpu"]["memory"] = preset.amd_gpu.memory
        if preset.intel_gpu is not None:
            payload["intel_gpu"] = {
                "count": preset.intel_gpu.count,
            }
            if preset.intel_gpu.model:
                payload["intel_gpu"]["model"] = preset.intel_gpu.model
            if preset.intel_gpu.memory:
                payload["intel_gpu"]["memory"] = preset.intel_gpu.memory
        if preset.tpu:
            payload["tpu"] = {
                "type": preset.tpu.type,
                "software_version": preset.tpu.software_version,
            }
        if preset.resource_pool_names:
            payload["resource_pool_names"] = preset.resource_pool_names
        if preset.available_resource_pool_names:
            payload["available_resource_pool_names"] = (
                preset.available_resource_pool_names
            )
        return payload

    def _convert_energy_schedule_to_payload(
        self, schedule: EnergySchedule
    ) -> dict[str, Any]:
        return {
            "name": schedule.name,
            "periods": [
                self._convert_energy_schedule_period_to_payload(p)
                for p in schedule.periods
            ],
        }

    def _convert_energy_schedule_period_to_payload(
        self, period: EnergySchedulePeriod
    ) -> dict[str, Any]:
        return {
            "weekday": period.weekday,
            "start_time": period.start_time.replace(tzinfo=None).isoformat(
                timespec="minutes"
            ),
            "end_time": period.end_time.replace(tzinfo=None).isoformat(
                timespec="minutes"
            ),
        }

    def _convert_project_user_to_payload(
        self, project_user: ProjectUser
    ) -> dict[str, Any]:
        return {
            "name": project_user.project_name,
            "role": str(project_user.role),
            "cluster_name": project_user.cluster_name,
            "org_name": project_user.org_name,
        }

    def _convert_storage_volume_to_payload(
        self, volume: VolumeConfig
    ) -> dict[str, Any]:
        return {
            "name": volume.name,
            "credits_per_hour_per_gb": str(volume.credits_per_hour_per_gb),
        }

    def _convert_apps_config_to_payload(
        self, apps_config: AppsConfig
    ) -> dict[str, Any]:
        return {
            "apps_hostname_templates": apps_config.apps_hostname_templates,
        }


@aiohttp.web.middleware
async def handle_exceptions(
    request: aiohttp.web.Request,
    handler: Callable[[aiohttp.web.Request], Awaitable[aiohttp.web.StreamResponse]],
) -> aiohttp.web.StreamResponse:
    try:
        return await handler(request)
    except JobException as e:
        payload = {"error": str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code
        )
    except JobsServiceException as e:
        payload = {"error": str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code
        )
    except ValueError as e:
        payload = {"error": str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code
        )
    except aiohttp.web.HTTPException:
        raise
    except Exception as e:
        msg_str = (
            f"Unexpected exception {e.__class__.__name__}: {str(e)}. "
            f"Path with query: {request.path_qs}."
        )
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPInternalServerError.status_code
        )


async def create_config_app(config: Config) -> aiohttp.web.Application:
    config_app = aiohttp.web.Application()
    config_handler = ConfigApiHandler(app=config_app, config=config)
    config_handler.register(config_app)
    return config_app


async def create_jobs_app(config: Config) -> aiohttp.web.Application:
    jobs_app = aiohttp.web.Application()
    jobs_handler = JobsHandler(app=jobs_app, config=config)
    jobs_handler.register(jobs_app)
    return jobs_app


async def add_version_to_header(
    request: aiohttp.web.Request, response: aiohttp.web.StreamResponse
) -> None:
    response.headers["X-Service-Version"] = f"platform-api/{__version__}"


async def create_app(
    config: Config, clusters: Sequence[Cluster] = ()
) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url,
                    token=config.auth.service_token,
                )
            )
            app["jobs_app"]["auth_client"] = auth_client

            admin_client = await exit_stack.enter_async_context(
                AdminClient(
                    base_url=config.admin_url,
                    service_token=config.auth.service_token,
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            logger.info("Initializing Notifications client")
            notifications_client = await exit_stack.enter_async_context(
                NotificationsClient(
                    url=config.notifications.url or None,
                    token=config.notifications.token,
                )
            )

            logger.info("Initializing Cluster Registry")
            cluster_config_registry = ClusterConfigRegistry()

            logger.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    url=config.config_url,
                    token=config.auth.service_token,
                )
            )

            if clusters:
                client_clusters = clusters
            else:
                client_clusters = await config_client.list_clusters()

            logger.info("Loading clusters")
            for cluster in client_clusters:
                await cluster_config_registry.replace(cluster)

            logger.info("Initializing SQLAlchemy engine")
            engine = make_async_engine(config.database.postgres)
            exit_stack.push_async_callback(engine.dispose)

            logger.info("Initializing JobsStorage")
            jobs_storage: JobsStorage = PostgresJobsStorage(engine)

            logger.info("Initializing EventsClient")
            events_client = await exit_stack.enter_async_context(
                create_events_client_from_config(config.events)
            )

            logger.info("Initializing JobsService")
            jobs_service = JobsService(
                cluster_config_registry=cluster_config_registry,
                jobs_storage=jobs_storage,
                jobs_config=config.jobs,
                notifications_client=notifications_client,
                auth_client=auth_client,
                admin_client=admin_client,
                api_base_url=config.api_base_url,
            )

            logger.info("Initializing ClusterUpdater")
            cluster_updater = ClusterUpdater(
                events_client=events_client,
                config_client=config_client,
                cluster_registry=cluster_config_registry,
            )
            await exit_stack.enter_async_context(cluster_updater)

            logger.info("Initializing ProjectDeleter")  # pragma: no cover
            project_deleter = ProjectDeleter(  # pragma: no cover
                events_client=events_client,
                jobs_service=jobs_service,
            )
            await exit_stack.enter_async_context(project_deleter)  # pragma: no cover

            app["config_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service

            logger.info("Initializing JobPolicyEnforcePoller")

            await exit_stack.enter_async_context(
                JobPolicyEnforcePoller(
                    config.job_policy_enforcer,
                    enforcers=[
                        RuntimeLimitEnforcer(jobs_service),
                        CreditsLimitEnforcer(jobs_service, admin_client),
                        StopOnClusterRemoveEnforcer(
                            jobs_service=jobs_service,
                            auth_client=auth_client,
                            cluster_config_registry=cluster_config_registry,
                        ),
                        RetentionPolicyEnforcer(
                            jobs_service=jobs_service,
                            retention_delay=config.job_policy_enforcer.retention_delay,
                        ),
                    ],
                )
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    app["api_v1_app"] = api_v1_app

    config_app = await create_config_app(config)
    app["config_app"] = config_app
    api_v1_app.add_subapp("/config", config_app)

    jobs_app = await create_jobs_app(config=config)
    app["jobs_app"] = jobs_app
    api_v1_app.add_subapp("/jobs", jobs_app)

    app.add_subapp("/api/v1", api_v1_app)

    app.router.add_get("/ping", api_v1_handler.handle_ping)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:
    init_logging(health_check_url_path="/ping")
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    loop = asyncio.get_event_loop()
    setup_sentry(ignore_errors=[JobError, JobStorageTransactionError])
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
