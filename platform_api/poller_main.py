import asyncio
import logging
from contextlib import AsyncExitStack
from typing import AsyncIterator, Sequence

import aiohttp.web
import sentry_sdk
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import init_logging
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from .api import add_version_to_header, handle_exceptions
from .cluster import (
    Cluster,
    ClusterConfig,
    ClusterRegistry,
    ClusterUpdateNotifier,
    ClusterUpdater,
    get_cluster_configs,
)
from .config import Config
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .kube_cluster import KubeCluster
from .orchestrator.jobs_poller import HttpJobsPollerApi, JobsPoller, JobsPollerService
from .orchestrator.jobs_service import JobsScheduler
from .postgres import create_postgres_pool


logger = logging.getLogger(__name__)


class PingHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


async def create_ping_app(config: Config) -> aiohttp.web.Application:
    ping_app = aiohttp.web.Application()
    ping_handler = PingHandler(app=ping_app, config=config)
    ping_handler.register(ping_app)
    return ping_app


def create_cluster(config: ClusterConfig) -> Cluster:
    return KubeCluster(config)


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
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            logger.info("Initializing Cluster Registry")
            cluster_registry = await exit_stack.enter_async_context(
                ClusterRegistry(factory=create_cluster)
            )

            logger.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    base_url=config.config_url,
                    service_token=config.auth.service_token,
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

            cluster_update_notifier = ClusterUpdateNotifier(postgres_pool)

            logger.info("Initializing JobsPollerApi")
            poller_api = await exit_stack.enter_async_context(
                HttpJobsPollerApi(
                    url=config.job_policy_enforcer.platform_api_url,
                    token=config.auth.service_token,
                )
            )

            logger.info("Initializing JobsPollerService")
            jobs_poller_service = JobsPollerService(
                cluster_registry=cluster_registry,
                jobs_config=config.jobs,
                scheduler=JobsScheduler(config.scheduler, auth_client=auth_client),
                auth_client=auth_client,
                api=poller_api,
            )

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(jobs_poller_service=jobs_poller_service)
            await exit_stack.enter_async_context(jobs_poller)

            logger.info("Initializing ClusterUpdater")
            cluster_updater = ClusterUpdater(
                notifier=cluster_update_notifier,
                config=config,
                config_client=config_client,
                cluster_registry=cluster_registry,
            )
            await exit_stack.enter_async_context(cluster_updater)

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_ping_app(config)

    app.add_subapp("/api/v1", api_v1_app)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    if config.sentry:
        sentry_sdk.init(dsn=config.sentry.url, integrations=[AioHttpIntegration()])
        sentry_sdk.set_tag("cluster", config.sentry.cluster)
        sentry_sdk.set_tag("app", "platformapi")

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
