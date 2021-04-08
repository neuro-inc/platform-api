import asyncio
import logging
from contextlib import AsyncExitStack
from typing import AsyncIterator, Callable, Optional

import aiohttp.web
import sentry_sdk
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import init_logging
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from .api import add_version_to_header, handle_exceptions
from .cluster import Cluster, ClusterConfig, ClusterHolder, SingleClusterUpdater
from .config import PollerConfig
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .kube_cluster import KubeCluster
from .orchestrator.jobs_poller import HttpJobsPollerApi, JobsPoller, JobsPollerService
from .orchestrator.poller_service import JobsScheduler


logger = logging.getLogger(__name__)


class PingHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: PollerConfig):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


async def create_ping_app(config: PollerConfig) -> aiohttp.web.Application:
    ping_app = aiohttp.web.Application()
    ping_handler = PingHandler(app=ping_app, config=config)
    ping_handler.register(ping_app)
    return ping_app


def create_cluster_factory(
    config: PollerConfig,
) -> Callable[[ClusterConfig], Cluster]:
    def _create_cluster(cluster_config: ClusterConfig) -> Cluster:
        return KubeCluster(
            registry_config=config.registry_config,
            storage_config=config.storage_config,
            cluster_config=cluster_config,
            kube_config=config.kube_config,
        )

    return _create_cluster


async def create_app(
    config: PollerConfig, cluster: Optional[ClusterConfig] = None
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
            cluster_holder = await exit_stack.enter_async_context(
                ClusterHolder(factory=create_cluster_factory(config))
            )

            logger.info("Initializing Config client")
            config_client = await exit_stack.enter_async_context(
                ConfigClient(
                    base_url=config.config_url,
                    service_token=config.auth.service_token,
                )
            )

            logger.info("Initializing JobsPollerApi")
            poller_api = await exit_stack.enter_async_context(
                HttpJobsPollerApi(
                    url=config.platform_api_url,
                    token=config.auth.service_token,
                    cluster_name=config.cluster_name,
                )
            )

            logger.info("Initializing JobsPollerService")
            jobs_poller_service = JobsPollerService(
                cluster_holder=cluster_holder,
                jobs_config=config.jobs,
                scheduler=JobsScheduler(config.scheduler, auth_client=auth_client),
                auth_client=auth_client,
                api=poller_api,
            )

            logger.info("Initializing ClusterUpdater")
            cluster_updater = SingleClusterUpdater(
                config_client=config_client,
                cluster_holder=cluster_holder,
                cluster_name=config.cluster_name,
            )

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(
                jobs_poller_service=jobs_poller_service, cluster_updater=cluster_updater
            )
            await exit_stack.enter_async_context(jobs_poller)

            if cluster:
                await cluster_holder.update(cluster)
            else:
                await cluster_updater.do_update()

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_ping_app(config)

    app.add_subapp("/api/v1", api_v1_app)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create_poller()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    if config.sentry:
        sentry_sdk.init(dsn=config.sentry.url, integrations=[AioHttpIntegration()])
        sentry_sdk.set_tag("cluster", config.sentry.cluster)
        sentry_sdk.set_tag("app", "platformapi")

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
