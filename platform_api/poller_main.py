import asyncio
import logging
from contextlib import AsyncExitStack
from typing import AsyncIterator, Callable, Optional

import aiohttp.web
import aiozipkin
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import (
    init_logging,
    make_sentry_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
)
from platform_logging.trace import create_zipkin_tracer

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

    @notrace
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

    trace_configs = []

    if config.zipkin:
        tracer = await create_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )
        trace_configs.append(aiozipkin.make_trace_config(tracer))

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:

            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url,
                    token=config.auth.service_token,
                    trace_configs=trace_configs,
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
    api_v1_routes = (
        api_v1_app.router.routes()
    )  # save ping endpoints to skip later for zipkin

    app.add_subapp("/api/v1", api_v1_app)

    if config.zipkin:
        assert tracer
        setup_zipkin(app, tracer, skip_routes=api_v1_routes)

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create_poller()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    if config.sentry:
        setup_sentry(
            config.sentry.url,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
        )

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
