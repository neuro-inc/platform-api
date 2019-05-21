import asyncio
import logging
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Sequence

import aiohttp.web
from aiohttp.web import HTTPUnauthorized
from async_exit_stack import AsyncExitStack
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security

from .cluster import Cluster, ClusterConfig, ClusterRegistry
from .config import Config
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler, ModelsHandler
from .kube_cluster import KubeCluster
from .orchestrator.job_request import JobException
from .orchestrator.jobs_poller import JobsPoller
from .orchestrator.jobs_service import JobsService, JobsServiceException
from .orchestrator.jobs_storage import RedisJobsStorage
from .redis import create_redis_client
from .resource import GKEGPUModels
from .user import authorized_user


logger = logging.getLogger(__name__)

RESOURCE_PRESETS = {
    "gpu-small": dict(
        gpu=1, cpu=7, memory=30 * 1024, gpu_model=next(iter(GKEGPUModels)).value.id
    ),
    "gpu-large": dict(
        gpu=1, cpu=7, memory=60 * 1024, gpu_model=next(reversed(GKEGPUModels)).value.id
    ),
    "cpu-small": dict(cpu=2, memory=2 * 1024),
    "cpu-large": dict(cpu=3, memory=14 * 1024),
}


class ApiHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config):
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            (
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/config", self.handle_config),
            )
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    async def handle_ping(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response()

    async def handle_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        data: Dict[str, Any] = {}

        try:
            user = await authorized_user(request)
            cluster_config = await self._jobs_service.get_cluster_config(user)
            data.update(
                {
                    "registry_url": str(cluster_config.registry.url),
                    "storage_url": str(cluster_config.ingress.storage_url),
                    "users_url": str(cluster_config.ingress.users_url),
                    "monitoring_url": str(cluster_config.ingress.monitoring_url),
                    "resource_presets": RESOURCE_PRESETS,
                }
            )
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


def init_logging() -> None:
    logging.basicConfig(
        # TODO (A Danshyn 06/01/18): expose in the Config
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


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
            f"Unexpected exception: {str(e)}. " f"Path with query: {request.path_qs}."
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


async def create_models_app(config: Config) -> aiohttp.web.Application:
    models_app = aiohttp.web.Application()
    models_handler = ModelsHandler(app=models_app, config=config)
    models_handler.register(models_app)
    return models_app


async def create_jobs_app(config: Config) -> aiohttp.web.Application:
    jobs_app = aiohttp.web.Application()
    jobs_handler = JobsHandler(app=jobs_app, config=config)
    jobs_handler.register(jobs_app)
    return jobs_app


def create_cluster(config: ClusterConfig) -> Cluster:
    return KubeCluster(config)


async def create_app(
    config: Config, cluster_configs_future: Awaitable[Sequence[ClusterConfig]]
) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:

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
            )

            logger.info("Initializing JobsPoller")
            jobs_poller = JobsPoller(jobs_service=jobs_service)
            await exit_stack.enter_async_context(jobs_poller)

            app["api_v1_app"]["jobs_service"] = jobs_service
            app["models_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service

            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url, token=config.auth.service_token
                )
            )
            app["jobs_app"]["auth_client"] = auth_client

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app(config)
    app["api_v1_app"] = api_v1_app

    models_app = await create_models_app(config=config)
    app["models_app"] = models_app
    api_v1_app.add_subapp("/models", models_app)

    jobs_app = await create_jobs_app(config=config)
    app["jobs_app"] = jobs_app
    api_v1_app.add_subapp("/jobs", jobs_app)

    app.add_subapp("/api/v1", api_v1_app)
    return app


async def get_cluster_configs(config: Config) -> Sequence[ClusterConfig]:
    async with config.config_client as client:
        return await client.get_clusters(users_url=config.cluster.ingress.users_url)


def main() -> None:
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    app = loop.run_until_complete(create_app(config, get_cluster_configs(config)))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
