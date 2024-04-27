import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack
from importlib.metadata import version
from typing import Any

import aiohttp.web
import aiohttp_cors
from aiohttp.web import HTTPUnauthorized
from aiohttp.web_urldispatcher import AbstractRoute
from aiohttp_security import check_permission
from neuro_admin_client import AdminClient, OrgUser, ProjectUser
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)
from neuro_notifications_client import Client as NotificationsClient

from platform_api.orchestrator.job_policy_enforcer import (
    BillingEnforcer,
    CreditsLimitEnforcer,
    CreditsNotificationsEnforcer,
    JobPolicyEnforcePoller,
    RetentionPolicyEnforcer,
    RuntimeLimitEnforcer,
    StopOnClusterRemoveEnforcer,
)

from .cluster import ClusterConfig, ClusterConfigRegistry, ClusterUpdater
from .cluster_config import EnergySchedule, EnergySchedulePeriod
from .config import Config, CORSConfig
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler
from .orchestrator.billing_log.service import BillingLogService, BillingLogWorker
from .orchestrator.billing_log.storage import PostgresBillingLogStorage
from .orchestrator.job_request import JobError, JobException
from .orchestrator.jobs_service import (
    JobsService,
    JobsServiceException,
    UserClusterConfig,
)
from .orchestrator.jobs_storage import JobsStorage, PostgresJobsStorage
from .orchestrator.jobs_storage.base import JobStorageTransactionError
from .postgres import make_async_engine
from .resource import Preset, ResourcePoolType
from .user import authorized_user, untrusted_user
from .utils.update_notifier import (
    Notifier,
    PostgresChannelNotifier,
    ResubscribingNotifier,
)

logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    @notrace
    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


class ConfigApiHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.get("", self.handle_config),
                aiohttp.web.post("/clusters/sync", self.handle_clusters_sync),
            )
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _cluster_update_notifier(self) -> Notifier:
        return self._app["cluster_update_notifier"]

    async def handle_clusters_sync(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        user = await untrusted_user(request)
        permission = Permission(uri="cluster://", action="manage")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        await self._cluster_update_notifier.notify()

        return aiohttp.web.Response(text="OK")

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
            for preset in cluster_config.orchestrator.presets
        ]
        result = {
            "name": cluster_config.name,
            "registry_url": str(cluster_config.ingress.registry_url),
            "storage_url": str(cluster_config.ingress.storage_url),
            "monitoring_url": str(cluster_config.ingress.monitoring_url),
            "secrets_url": str(cluster_config.ingress.secrets_url),
            "metrics_url": str(cluster_config.ingress.metrics_url),
            "disks_url": str(cluster_config.ingress.disks_url),
            "buckets_url": str(cluster_config.ingress.buckets_url),
            "resource_pool_types": resource_pool_types,
            "resource_presets": presets,
            "orgs": orgs,
            "timezone": str(cluster_config.timezone),
            "energy_schedules": [
                self._convert_energy_schedule_to_payload(schedule)
                for schedule in cluster_config.energy.schedules
            ],
        }
        if self._config.auth.public_endpoint_url:
            result["users_url"] = str(self._config.auth.public_endpoint_url)
        return result

    def _convert_resource_pool_type_to_payload(
        self, resource_pool_type: ResourcePoolType
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": resource_pool_type.name,
            "min_size": resource_pool_type.min_size,
            "max_size": resource_pool_type.max_size,
            "cpu": resource_pool_type.cpu,
            "available_cpu": resource_pool_type.available_cpu,
            "memory": resource_pool_type.memory,
            "available_memory": resource_pool_type.available_memory,
            "disk_size": resource_pool_type.disk_size,
        }
        if resource_pool_type.nvidia_gpu is not None:
            payload["nvidia_gpu"] = resource_pool_type.nvidia_gpu
        if resource_pool_type.amd_gpu is not None:
            payload["amd_gpu"] = resource_pool_type.amd_gpu
        if resource_pool_type.tpu:
            payload["tpu"] = {
                "types": resource_pool_type.tpu.types,
                "software_versions": resource_pool_type.tpu.software_versions,
                "ipv4_cidr_block": resource_pool_type.tpu.ipv4_cidr_block,
            }
        if resource_pool_type.is_preemptible:
            payload["is_preemptible"] = resource_pool_type.is_preemptible
        return payload

    def _convert_preset_to_payload(self, preset: Preset) -> dict[str, Any]:
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
            payload["nvidia_gpu"] = preset.nvidia_gpu
            payload["gpu"] = preset.nvidia_gpu
        if preset.amd_gpu is not None:
            payload["amd_gpu"] = preset.amd_gpu
        if preset.gpu_model is not None:
            payload["gpu_model"] = preset.gpu_model
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


package_version = version(__package__)


async def add_version_to_header(
    request: aiohttp.web.Request, response: aiohttp.web.StreamResponse
) -> None:
    response.headers["X-Service-Version"] = f"platform-api/{package_version}"


def make_tracing_trace_configs(config: Config) -> list[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


async def create_app(
    config: Config, clusters: Sequence[ClusterConfig] = ()
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
                    trace_configs=make_tracing_trace_configs(config),
                )
            )
            app["jobs_app"]["auth_client"] = auth_client

            admin_client = await exit_stack.enter_async_context(
                AdminClient(
                    base_url=config.admin_url,
                    service_token=config.auth.service_token,
                    trace_configs=make_tracing_trace_configs(config),
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
                    trace_configs=make_tracing_trace_configs(config),
                )
            )

            logger.info("Initializing Cluster Registry")
            cluster_config_registry = ClusterConfigRegistry()

            logger.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    base_url=config.config_url,
                    service_token=config.auth.service_token,
                    trace_configs=make_tracing_trace_configs(config),
                )
            )

            if clusters:
                client_clusters = clusters
            else:
                client_clusters = await config_client.get_clusters()

            logger.info("Loading clusters")
            for cluster in client_clusters:
                await cluster_config_registry.replace(cluster)

            logger.info("Initializing SQLAlchemy engine")
            engine = make_async_engine(config.database.postgres)
            exit_stack.push_async_callback(engine.dispose)

            logger.info("Initializing JobsStorage")
            jobs_storage: JobsStorage = PostgresJobsStorage(engine)

            cluster_update_notifier = ResubscribingNotifier(
                PostgresChannelNotifier(engine, "cluster_update_required"),
                check_interval=15,
            )
            app["config_app"]["cluster_update_notifier"] = cluster_update_notifier

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
                notifier=cluster_update_notifier,
                config=config,
                config_client=config_client,
                cluster_registry=cluster_config_registry,
            )
            await exit_stack.enter_async_context(cluster_updater)

            app["config_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service

            logger.info("Initializing JobPolicyEnforcePoller")

            logger.info("Initializing BillingLogStorage")
            billing_log_storage = PostgresBillingLogStorage(engine)

            billing_log_new_entry_notifier = ResubscribingNotifier(
                PostgresChannelNotifier(engine, "billing_log_new_entry"),
                check_interval=15,
            )

            billing_log_entry_done_notifier = ResubscribingNotifier(
                PostgresChannelNotifier(engine, "billing_log_entry_done_notifier"),
                check_interval=15,
            )

            logger.info("Initializing BillingLogService")
            billing_log_service = await exit_stack.enter_async_context(
                BillingLogService(
                    storage=billing_log_storage,
                    new_entry=billing_log_new_entry_notifier,
                    entry_done=billing_log_entry_done_notifier,
                )
            )

            logger.info("Initializing BillingLogWorker")
            await exit_stack.enter_async_context(
                BillingLogWorker(
                    storage=billing_log_storage,
                    new_entry=billing_log_new_entry_notifier,
                    entry_done=billing_log_entry_done_notifier,
                    admin_client=admin_client,
                    jobs_service=jobs_service,
                )
            )

            await exit_stack.enter_async_context(
                JobPolicyEnforcePoller(
                    config.job_policy_enforcer,
                    enforcers=[
                        RuntimeLimitEnforcer(jobs_service),
                        CreditsLimitEnforcer(jobs_service, admin_client),
                        BillingEnforcer(jobs_service, billing_log_service),
                        CreditsNotificationsEnforcer(
                            jobs_service=jobs_service,
                            admin_client=admin_client,
                            notifications_client=notifications_client,
                            notification_threshold=(
                                config.job_policy_enforcer.credit_notification_threshold
                            ),
                        ),
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
    probes_routes = api_v1_handler.register(api_v1_app)
    app["api_v1_app"] = api_v1_app

    config_app = await create_config_app(config)
    app["config_app"] = config_app
    api_v1_app.add_subapp("/config", config_app)

    jobs_app = await create_jobs_app(config=config)
    app["jobs_app"] = jobs_app
    api_v1_app.add_subapp("/jobs", jobs_app)

    app.add_subapp("/api/v1", api_v1_app)

    _setup_cors(app, config.cors)

    app.on_response_prepare.append(add_version_to_header)

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    return app


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True, expose_headers="*", allow_headers="*"
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    for route in app.router.routes():
        logger.debug(f"Setting up CORS for {route}")
        cors.add(route)


def setup_tracing(config: Config) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
            exclude=[JobError, JobStorageTransactionError],
        )


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    loop = asyncio.get_event_loop()
    setup_tracing(config)
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
