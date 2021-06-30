import asyncio
import logging
from contextlib import AsyncExitStack
from typing import AsyncIterator, Callable, List, Optional

import aiohttp.web
from aiohttp.web_urldispatcher import AbstractRoute
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)

from .api import add_version_to_header, handle_exceptions
from .cluster import Cluster, ClusterConfig, ClusterHolder, SingleClusterUpdater
from .config import PollerConfig
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .kube_cluster import KubeCluster
from .orchestrator.jobs_poller import HttpJobsPollerApi, JobsPoller, JobsPollerService
from .orchestrator.poller_service import JobsScheduler


logger = logging.getLogger(__name__)


class Handler:
    def __init__(self, app: aiohttp.web.Application):
        self._app = app

    def register(self, app: aiohttp.web.Application) -> List[AbstractRoute]:
        return app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    @notrace
    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


def create_cluster_factory(
    config: PollerConfig,
) -> Callable[[ClusterConfig], Cluster]:
    def _create_cluster(cluster_config: ClusterConfig) -> Cluster:
        return KubeCluster(
            registry_config=config.registry_config,
            storage_config=config.storage_config,
            cluster_config=cluster_config,
            kube_config=config.kube_config,
            trace_configs=make_tracing_trace_configs(config),
        )

    return _create_cluster


def make_tracing_trace_configs(config: PollerConfig) -> List[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


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
                    trace_configs=make_tracing_trace_configs(config),
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
                    trace_configs=make_tracing_trace_configs(config),
                )
            )

            logger.info("Initializing JobsPollerApi")
            poller_api = await exit_stack.enter_async_context(
                HttpJobsPollerApi(
                    url=config.platform_api_url,
                    token=config.auth.service_token,
                    cluster_name=config.cluster_name,
                    trace_configs=make_tracing_trace_configs(config),
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

            if cluster:
                await cluster_holder.update(cluster)
                cluster_updater.disable_updates_for_test = True
            else:
                await cluster_updater.do_update()

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(
                jobs_poller_service=jobs_poller_service, cluster_updater=cluster_updater
            )
            await exit_stack.enter_async_context(jobs_poller)

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = Handler(api_v1_app)
    probes_routes = api_v1_handler.register(api_v1_app)

    app.add_subapp("/api/v1", api_v1_app)

    app.on_response_prepare.append(add_version_to_header)

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    return app


def setup_tracing(config: PollerConfig) -> None:
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
        )


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create_poller()
    logging.info("Loaded config: %r", config)
    loop = asyncio.get_event_loop()
    setup_tracing(config)
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
