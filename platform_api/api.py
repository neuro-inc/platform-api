import asyncio
import logging

import aiohttp.web
from async_exit_stack import AsyncExitStack
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security

from .config import Config
from .config_factory import EnvironConfigFactory
from .handlers import JobsHandler, ModelsHandler
from .orchestrator import JobException, JobsService, JobsStatusPooling, KubeOrchestrator
from .orchestrator.jobs_storage import RedisJobsStorage
from .redis import create_redis_client


logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app):
        app.add_routes((aiohttp.web.get("/ping", self.handle_ping),))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


def init_logging():
    logging.basicConfig(
        # TODO (A Danshyn 06/01/18): expose in the Config
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@aiohttp.web.middleware
async def handle_exceptions(request, handler):
    try:
        return await handler(request)
    except JobException as e:
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


async def create_api_v1_app():
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_models_app(config: Config):
    models_app = aiohttp.web.Application()
    models_handler = ModelsHandler(app=models_app, config=config)
    models_handler.register(models_app)
    return models_app


async def create_jobs_app(config: Config):
    jobs_app = aiohttp.web.Application()
    jobs_handler = JobsHandler(app=jobs_app, config=config)
    jobs_handler.register(jobs_app)
    return jobs_app


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application):
        async with AsyncExitStack() as exit_stack:

            logger.info("Initializing Redis client")
            redis_client = await exit_stack.enter_async_context(
                create_redis_client(config.database.redis)
            )

            logger.info("Initializing Orchestrator")
            orchestrator = KubeOrchestrator(config=config.orchestrator, loop=app.loop)
            await exit_stack.enter_async_context(orchestrator)

            logger.info("Initializing JobsStorage")
            jobs_storage = RedisJobsStorage(redis_client, orchestrator=orchestrator)

            logger.info("Initializing JobsService")
            jobs_service = JobsService(
                orchestrator=orchestrator, jobs_storage=jobs_storage
            )

            logger.info("Initializing JobsStatusPolling")
            jobs_status_polling = JobsStatusPooling(
                jobs_service=jobs_service, loop=app.loop
            )
            await exit_stack.enter_async_context(jobs_status_polling)

            app["models_app"]["jobs_service"] = jobs_service
            app["jobs_app"]["jobs_service"] = jobs_service

            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    url=config.auth.server_endpoint_url, token=config.auth.service_token
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app()

    models_app = await create_models_app(config=config)
    app["models_app"] = models_app
    api_v1_app.add_subapp("/models", models_app)

    jobs_app = await create_jobs_app(config=config)
    app["jobs_app"] = jobs_app
    api_v1_app.add_subapp("/jobs", jobs_app)

    app.add_subapp("/api/v1", api_v1_app)
    return app


def main():
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    loop = asyncio.get_event_loop()

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
