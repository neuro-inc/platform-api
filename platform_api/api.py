import asyncio
import logging

import aiohttp.web

from .config import Config, EnvironConfigFactory
from .handlers import (
    ModelsHandler, JobsHandler)
from .orchestrator import (
    KubeOrchestrator, KubeConfig, JobsService, InMemoryJobsService, JobError, Orchestrator, JobsStatusPooling)


class ApiHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/ping', self.handle_ping),
        ))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


def init_logging():
    logging.basicConfig(
        # TODO (A Danshyn 06/01/18): expose in the Config
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@aiohttp.web.middleware
async def handle_exceptions(request, handler):
    try:
        return await handler(request)
    except JobError as ex:
        return aiohttp.web.json_response(data={'error': str(ex)}, status=aiohttp.web.HTTPBadRequest.status_code)
    except ValueError as e:
        payload = {'error': str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code)
    except Exception as e:
        msg_str = (
            f'Unexpected exception: {str(e)}. '
            f'Path with query: {request.path_qs}.')
        logging.exception(msg_str)
        payload = {'error': msg_str}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPInternalServerError.status_code)


async def create_orchestrator(app: aiohttp.web.Application, kube_config: KubeConfig):
    orchestrator = KubeOrchestrator(config=kube_config, loop=app.loop)

    async def _init_orchestrator(_):
        async with orchestrator:
            yield orchestrator
    app.cleanup_ctx.append(_init_orchestrator)
    return orchestrator


async def create_job_service(app: aiohttp.web.Application, orchestrator: Orchestrator) -> JobsService:
    jobs_service = InMemoryJobsService(orchestrator=orchestrator)
    jobs_status_pooling = JobsStatusPooling(jobs_service=jobs_service, loop=app.loop)

    async def _init_background_pooling(_):
        async with jobs_status_pooling:
            yield jobs_status_pooling

    app.cleanup_ctx.append(_init_background_pooling)
    return jobs_service


async def create_models_app(config: Config, jobs_service: JobsService):
    models_app = aiohttp.web.Application()
    models_handler = ModelsHandler(
        config=config, jobs_service=jobs_service)
    models_handler.register(models_app)
    return models_app


async def create_jobs_app(jobs_service: JobsService):
    jobs_app = aiohttp.web.Application()
    jobs_handler = JobsHandler(jobs_service=jobs_service)
    jobs_handler.register(jobs_app)
    return jobs_app


async def create_app(config: Config):
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app['config'] = config

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    orchestrator = await create_orchestrator(app=app, kube_config=config.orchestrator)
    jobs_service = await create_job_service(app=app, orchestrator=orchestrator)

    models_app = await create_models_app(config=config, jobs_service=jobs_service)
    api_v1_app.add_subapp('/models', models_app)
    jobs_app = await create_jobs_app(jobs_service=jobs_service)
    api_v1_app.add_subapp('/jobs', jobs_app)

    app.add_subapp('/api/v1', api_v1_app)
    return app


def main():
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info('Loaded config: %r', config)

    loop = asyncio.get_event_loop()

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
