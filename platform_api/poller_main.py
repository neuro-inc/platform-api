import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack

import aiohttp.web
from aiohttp.web_urldispatcher import AbstractRoute
from neuro_admin_client import AdminClient
from neuro_auth_client import AuthClient
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

from .api import add_version_to_header, handle_exceptions
from .cluster import Cluster, ClusterConfig, ClusterHolder, SingleClusterUpdater
from .config import PollerConfig
from .config_client import ConfigClient
from .config_factory import EnvironConfigFactory
from .kube_cluster import KubeCluster
from .orchestrator.job_request import JobError
from .orchestrator.jobs_poller import HttpJobsPollerApi, JobsPoller, JobsPollerService
from .orchestrator.jobs_storage.base import JobStorageTransactionError
from .orchestrator.kube_client import KubeClient
from .orchestrator.poller_service import JobsScheduler

logger = logging.getLogger(__name__)


class Handler:
    def __init__(self, app: aiohttp.web.Application):
        self._app = app

    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    @notrace
    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()


def create_cluster_factory(
    config: PollerConfig, kube_client: KubeClient
) -> Callable[[ClusterConfig], Cluster]:
    def _create_cluster(cluster_config: ClusterConfig) -> Cluster:
        return KubeCluster(
            kube_client=kube_client,
            kube_config=config.kube_config,
            registry_config=config.registry_config,
            storage_configs=config.storage_configs,
            cluster_config=cluster_config,
        )

    return _create_cluster


def make_tracing_trace_configs(config: PollerConfig) -> list[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


async def create_app(
    config: PollerConfig, cluster: ClusterConfig | None = None
) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing KubeClient")
            kube_config = config.kube_config
            kube_client = await exit_stack.enter_async_context(
                KubeClient(
                    base_url=kube_config.endpoint_url,
                    cert_authority_data_pem=kube_config.cert_authority_data_pem,
                    cert_authority_path=kube_config.cert_authority_path,
                    auth_type=kube_config.auth_type,
                    auth_cert_path=kube_config.auth_cert_path,
                    auth_cert_key_path=kube_config.auth_cert_key_path,
                    token=kube_config.token,
                    token_path=kube_config.token_path,
                    namespace=kube_config.namespace,
                    conn_timeout_s=kube_config.client_conn_timeout_s,
                    read_timeout_s=kube_config.client_read_timeout_s,
                    conn_pool_size=kube_config.client_conn_pool_size,
                    trace_configs=make_tracing_trace_configs(config),
                )
            )

            logger.info("Initializing AuthClient")
            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url,
                    token=config.auth.service_token,
                    trace_configs=make_tracing_trace_configs(config),
                )
            )
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

            logger.info("Initializing ClusterHolder")
            cluster_holder = await exit_stack.enter_async_context(
                ClusterHolder(factory=create_cluster_factory(config, kube_client))
            )

            logger.info("Initializing ConfigClient")
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
                scheduler=JobsScheduler(
                    config=config.scheduler,
                    admin_client=admin_client,
                    cluster_holder=cluster_holder,
                ),
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
                cluster_updater.disable_updates_for_test = True

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
            ignore_errors=[JobError, JobStorageTransactionError],
        )


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create_poller()
    logging.info("Loaded config: %r", config)
    loop = asyncio.get_event_loop()
    setup_tracing(config)
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
