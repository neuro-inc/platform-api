import asyncio
import logging

import aiohttp.web

from .config import Config, EnvironConfigFactory
from .handlers import ModelsHandler, StatusesHandler, JobsHandler
from .orchestrator import (
    KubeOrchestrator, KubeConfig, JobsService, InMemoryJobsService, JobError)


class ApiHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/ping', self.handle_ping),
        ))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@aiohttp.web.middleware
async def handle_exceptions(request, handler):
    try:
        return await handler(request)
    except JobError as ex:
        return aiohttp.web.json_response(data={'error': str(ex)}, status=400)
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


async def create_orchestrator(loop: asyncio.AbstractEventLoop, kube_config: KubeConfig):
    kube_orchestrator = KubeOrchestrator(config=kube_config, loop=loop)
    return kube_orchestrator


async def create_job_service(config: Config, app: aiohttp.web.Application) -> JobsService:
    orchestrator = await create_orchestrator(
        app.loop, kube_config=config.orchestrator)

    async def _init_orchestrator(_):
        async with orchestrator:
            yield orchestrator
    app.cleanup_ctx.append(_init_orchestrator)
    jobs_service = InMemoryJobsService(orchestrator=orchestrator)
    return jobs_service


async def create_models_app(config: Config, jobs_service: JobsService):
    models_app = aiohttp.web.Application()
    models_handler = ModelsHandler(
        config=config, jobs_service=jobs_service)
    models_handler.register(models_app)
    return models_app


async def create_statuses_app(jobs_service: JobsService):
    statuses_app = aiohttp.web.Application()
    statuses_handler = StatusesHandler(jobs_service=jobs_service)
    statuses_handler.register(statuses_app)
    return statuses_app


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

    jobs_service = await create_job_service(config=config, app=app)

    models_app = await create_models_app(config=config, jobs_service=jobs_service)
    api_v1_app.add_subapp('/models', models_app)
    statuses_app = await create_statuses_app(jobs_service=jobs_service)
    api_v1_app.add_subapp('/statuses', statuses_app)
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
