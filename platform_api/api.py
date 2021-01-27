import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Sequence

import aiohttp.web
import aiohttp_cors
import aiozipkin
import pkg_resources
import sentry_sdk
from aiohttp.web import HTTPUnauthorized
from aiohttp_security import check_permission
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthScheme, setup_security
from notifications_client import Client as NotificationsClient
from platform_logging import init_logging
from sentry_sdk import set_tag
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from platform_api.orchestrator.job_policy_enforcer import (
    JobPolicyEnforcePoller,
    PlatformApiClient,
    QuotaEnforcer,
    RuntimeLimitEnforcer,
)

from .cluster import (
    Cluster,
    ClusterConfig,
    ClusterRegistry,
    ClusterUpdateNotifier,
    ClusterUpdater,
    get_cluster_configs,
)
from .config import Config, CORSConfig
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler
from .handlers.stats_handler import StatsHandler
from .handlers.tags_handler import TagsHandler
from .kube_cluster import KubeCluster
from .orchestrator.job_request import JobException
from .orchestrator.jobs_poller import JobsPoller
from .orchestrator.jobs_service import JobsScheduler, JobsService, JobsServiceException
from .orchestrator.jobs_storage import JobsStorage, PostgresJobsStorage
from .postgres import create_postgres_pool
from .resource import Preset
from .trace import store_span_middleware
from .user import authorized_user, untrusted_user


logger = logging.getLogger(__name__)


class ApiHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/config", self.handle_config),
                aiohttp.web.post("/config/clusters/sync", self.handle_clusters_sync),
            )
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _cluster_update_notifier(self) -> ClusterUpdateNotifier:
        return self._app["cluster_update_notifier"]

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()

    async def handle_clusters_sync(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        user = await untrusted_user(request)
        permission = Permission(uri="cluster://", action="manage")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        await self._cluster_update_notifier.notify_cluster_update()

        return aiohttp.web.Response(text="OK")

    async def handle_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        data: Dict[str, Any] = {}

        try:
            user = await authorized_user(request)
            cluster_configs = await self._jobs_service.get_user_cluster_configs(user)
            data["clusters"] = [
                self._convert_cluster_config_to_payload(c) for c in cluster_configs
            ]
            # NOTE: adding the cluster payload to the root document for
            # backward compatibility
            data.update(data["clusters"][0])

            data["admin_url"] = str(self._config.admin_url)
        except HTTPUnauthorized:
            pass

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

    def _convert_cluster_config_to_payload(
        self, cluster_config: ClusterConfig
    ) -> Dict[str, Any]:
        presets = [
            self._convert_preset_to_payload(preset)
            for preset in cluster_config.orchestrator.presets
        ]
        return {
            "name": cluster_config.name,
            "registry_url": str(cluster_config.registry.url),
            "storage_url": str(cluster_config.ingress.storage_url),
            "blob_storage_url": str(cluster_config.ingress.blob_storage_url),
            "users_url": str(self._config.auth.public_endpoint_url),
            "monitoring_url": str(cluster_config.ingress.monitoring_url),
            "secrets_url": str(cluster_config.ingress.secrets_url),
            "metrics_url": str(cluster_config.ingress.metrics_url),
            "disks_url": str(cluster_config.ingress.disks_url),
            "resource_presets": presets,
        }

    def _convert_preset_to_payload(self, preset: Preset) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": preset.name,
            "cpu": preset.cpu,
            "memory_mb": preset.memory_mb,
            "scheduler_enabled": preset.scheduler_enabled,
            "preemptible_node": preset.preemptible_node,
            "is_preemptible": preset.scheduler_enabled,
            "is_preemptible_node_required": preset.preemptible_node,
        }
        if preset.gpu is not None:
            payload["gpu"] = preset.gpu
        if preset.gpu_model is not None:
            payload["gpu_model"] = preset.gpu_model

        if preset.tpu:
            payload["tpu"] = {
                "type": preset.tpu.type,
                "software_version": preset.tpu.software_version,
            }
        return payload


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


async def create_api_v1_app(config: Config) -> aiohttp.web.Application:
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler(app=api_v1_app, config=config)
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_jobs_app(config: Config) -> aiohttp.web.Application:
    jobs_app = aiohttp.web.Application()
    jobs_handler = JobsHandler(app=jobs_app, config=config)
    jobs_handler.register(jobs_app)
    return jobs_app


async def create_stats_app(config: Config) -> aiohttp.web.Application:
    stats_app = aiohttp.web.Application()
    stats_handler = StatsHandler(app=stats_app, config=config)
    stats_handler.register(stats_app)
    return stats_app


async def create_tags_app(config: Config) -> aiohttp.web.Application:
    tags_app = aiohttp.web.Application()
    tags_handler = TagsHandler(app=tags_app, config=config)
    tags_handler.register(tags_app)
    return tags_app


def create_cluster(config: ClusterConfig) -> Cluster:
    return KubeCluster(config)


async def create_tracer(config: Config) -> aiozipkin.Tracer:
    endpoint = aiozipkin.create_endpoint(
        "platformapi",  # the same name as pod prefix on a cluster
        ipv4=config.server.host,
        port=config.server.port,
    )

    zipkin_address = config.zipkin.url / "api/v2/spans"
    tracer = await aiozipkin.create(
        str(zipkin_address), endpoint, sample_rate=config.zipkin.sample_rate
    )
    return tracer


package_version = pkg_resources.get_distribution("platform-api").version


async def add_version_to_header(
    request: aiohttp.web.Request, response: aiohttp.web.StreamResponse
) -> None:
    response.headers["X-Service-Version"] = f"platform-api/{package_version}"


async def create_app(
    config: Config, clusters: Sequence[ClusterConfig] = ()
) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    tracer = await create_tracer(config)

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            trace_config = aiozipkin.make_trace_config(tracer)

            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url,
                    token=config.auth.service_token,
                    trace_config=trace_config,
                )
            )
            app["jobs_app"]["auth_client"] = auth_client
            app["stats_app"]["auth_client"] = auth_client

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            logger.info("Initializing Notifications client")
            notifications_client = NotificationsClient(
                url=config.notifications.url,
                token=config.notifications.token,
                trace_config=trace_config,
            )
            await exit_stack.enter_async_context(notifications_client)

            logger.info("Initializing Cluster Registry")
            cluster_registry = await exit_stack.enter_async_context(
                ClusterRegistry(factory=create_cluster)
            )

            logger.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    base_url=config.config_url,
                    service_token=config.auth.service_token,
                    trace_config=trace_config,
                )
            )

            if clusters:
                client_clusters = clusters
            else:
                client_clusters = await get_cluster_configs(config, config_client)

            logger.info("Loading clusters")
            for cluster in client_clusters:
                await cluster_registry.replace(cluster)

            assert config.database.postgres, (
                "Postgres config should be available when "
                "NP_DB_POSTGRES_ENABLED is set"
            )
            logger.info("Initializing Postgres connection pool")
            postgres_pool = await exit_stack.enter_async_context(
                create_postgres_pool(config.database.postgres)
            )

            logger.info("Initializing JobsStorage")
            jobs_storage: JobsStorage = PostgresJobsStorage(postgres_pool)
            await jobs_storage.migrate()

            cluster_update_notifier = ClusterUpdateNotifier(postgres_pool)
            app["api_v1_app"]["cluster_update_notifier"] = cluster_update_notifier

            logger.info("Initializing JobsService")
            jobs_service = JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=jobs_storage,
                jobs_config=config.jobs,
                notifications_client=notifications_client,
                scheduler=JobsScheduler(config.scheduler, auth_client=auth_client),
                auth_client=auth_client,
                api_base_url=config.api_base_url,
            )

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(jobs_service=jobs_service)
            await exit_stack.enter_async_context(jobs_poller)

            logger.info("Initializing ClusterUpdater")
            cluster_updater = ClusterUpdater(
                notifier=cluster_update_notifier,
                config=config,
                config_client=config_client,
                cluster_registry=cluster_registry,
            )
            await exit_stack.enter_async_context(cluster_updater)

            app["api_v1_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service
            app["stats_app"]["jobs_service"] = jobs_service
            app["tags_app"]["jobs_service"] = jobs_service

            logger.info("Initializing JobPolicyEnforcePoller")
            api_client = await exit_stack.enter_async_context(
                PlatformApiClient(config.job_policy_enforcer)
            )
            await exit_stack.enter_async_context(
                JobPolicyEnforcePoller(
                    config.job_policy_enforcer,
                    enforcers=[
                        QuotaEnforcer(
                            api_client, notifications_client, config.job_policy_enforcer
                        ),
                        RuntimeLimitEnforcer(api_client),
                    ],
                )
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app(config)
    app["api_v1_app"] = api_v1_app

    jobs_app = await create_jobs_app(config=config)
    app["jobs_app"] = jobs_app
    api_v1_app.add_subapp("/jobs", jobs_app)

    stats_app = await create_stats_app(config=config)
    app["stats_app"] = stats_app
    api_v1_app.add_subapp("/stats", stats_app)

    tags_app = await create_tags_app(config=config)
    app["tags_app"] = tags_app
    api_v1_app.add_subapp("/tags", tags_app)

    app.add_subapp("/api/v1", api_v1_app)

    _setup_cors(app, config.cors)

    aiozipkin.setup(app, tracer)
    app.middlewares.append(store_span_middleware)

    app.on_response_prepare.append(add_version_to_header)

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


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    sentry_url = config.sentry_url
    if sentry_url:
        sentry_sdk.init(dsn=sentry_url, integrations=[AioHttpIntegration()])

    set_tag("cluster", config.cluster_name)
    set_tag("app", "platformapi")

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
