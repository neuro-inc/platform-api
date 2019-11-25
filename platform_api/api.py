import asyncio
import logging
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Sequence

import aiohttp.web
from aiohttp.web import HTTPUnauthorized
from aiohttp_security import check_permission
from async_exit_stack import AsyncExitStack
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthScheme, setup_security
from notifications_client import Client as NotificationsClient
from platform_logging import init_logging

from platform_api.orchestrator.job_policy_enforcer import (
    JobPolicyEnforcePoller,
    PlatformApiClient,
    QuotaEnforcer,
)

from .cluster import Cluster, ClusterConfig, ClusterRegistry
from .config import Config
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler
from .handlers.stats_handler import StatsHandler
from .kube_cluster import KubeCluster
from .orchestrator.job_request import JobException
from .orchestrator.jobs_poller import JobsPoller
from .orchestrator.jobs_service import JobsService, JobsServiceException
from .orchestrator.jobs_storage import RedisJobsStorage
from .redis import create_redis_client
from .resource import Preset
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

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()

    async def handle_clusters_sync(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        user = await untrusted_user(request)
        permission = Permission(uri="cluster://", action="manage")
        logger.info("Checking whether %r has %r", user, permission)
        await check_permission(request, permission.action, [permission])

        cluster_configs_future = get_cluster_configs(self._config)
        cluster_configs = [
            cluster_config for cluster_config in await cluster_configs_future
        ]
        cluster_registry = self._jobs_service._cluster_registry
        old_record_count = len(cluster_registry)
        [
            await cluster_registry.add(cluster_config)
            for cluster_config in cluster_configs
        ]
        await cluster_registry.cleanup(cluster_configs)

        new_record_count = len(cluster_registry)

        return aiohttp.web.json_response(
            {"old_record_count": old_record_count, "new_record_count": new_record_count}
        )

    async def handle_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        data: Dict[str, Any] = {}

        try:
            user = await authorized_user(request)
            cluster_config = await self._jobs_service.get_cluster_config(user)
            cluster_payload = self._convert_cluster_config_to_payload(cluster_config)
            data["clusters"] = [cluster_payload]
            # NOTE: adding the cluster payload to the root document for
            # backward compatibility
            data.update(cluster_payload)
        except HTTPUnauthorized:
            pass

        if self._config.oauth:
            data["auth_url"] = str(self._config.oauth.auth_url)
            data["token_url"] = str(self._config.oauth.token_url)
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
            "users_url": str(self._config.auth.public_endpoint_url),
            "monitoring_url": str(cluster_config.ingress.monitoring_url),
            "resource_presets": presets,
        }

    def _convert_preset_to_payload(self, preset: Preset) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": preset.name,
            "cpu": preset.cpu,
            "memory_mb": preset.memory_mb,
            "is_preemptible": preset.is_preemptible,
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


def create_cluster(config: ClusterConfig) -> Cluster:
    return KubeCluster(config)


async def create_app(
    config: Config, cluster_configs_future: Awaitable[Sequence[ClusterConfig]]
) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:

            logger.info("Initializing Notifications client")
            notifications_client = NotificationsClient(
                url=config.notifications.url, token=config.notifications.token
            )
            await exit_stack.enter_async_context(notifications_client)

            logger.info("Initializing Redis client")
            redis_client = await exit_stack.enter_async_context(
                create_redis_client(config.database.redis)
            )

            logger.info("Initializing Cluster Registry")
            cluster_registry = await exit_stack.enter_async_context(
                ClusterRegistry(factory=create_cluster)
            )

            [
                await cluster_registry.add(cluster_config)
                for cluster_config in await cluster_configs_future
            ]

            logger.info("Initializing JobsStorage")
            jobs_storage = RedisJobsStorage(redis_client)
            await jobs_storage.migrate()

            logger.info("Initializing JobsService")
            jobs_service = JobsService(
                cluster_registry=cluster_registry,
                jobs_storage=jobs_storage,
                jobs_config=config.jobs,
                notifications_client=notifications_client,
            )

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(jobs_service=jobs_service)
            await exit_stack.enter_async_context(jobs_poller)

            app["api_v1_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service
            app["stats_app"]["jobs_service"] = jobs_service

            logger.info("Initializing JobPolicyEnforcePoller")
            api_client = await exit_stack.enter_async_context(
                PlatformApiClient(config.job_policy_enforcer)
            )
            await exit_stack.enter_async_context(
                JobPolicyEnforcePoller(
                    config.job_policy_enforcer, enforcers=[QuotaEnforcer(api_client)]
                )
            )

            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url, token=config.auth.service_token
                )
            )
            app["jobs_app"]["auth_client"] = auth_client
            app["stats_app"]["auth_client"] = auth_client

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
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

    app.add_subapp("/api/v1", api_v1_app)
    return app


async def get_cluster_configs(config: Config) -> Sequence[ClusterConfig]:
    async with config.config_client as client:
        return await client.get_clusters(
            jobs_ingress_class=config.jobs.jobs_ingress_class,
            jobs_ingress_oauth_url=config.jobs.jobs_ingress_oauth_url,
            registry_username=config.auth.service_name,
            registry_password=config.auth.service_token,
        )


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    app = loop.run_until_complete(create_app(config, get_cluster_configs(config)))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
