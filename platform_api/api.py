import asyncio
import logging

import aiohttp.web

from .config import Config, EnvironConfigFactory
from .handlers import ModelsHandler, StatusesHandler
from .orchestrator import (
    KubeOrchestrator, KubeConfig, InMemoryStatusService, StatusService)


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


async def create_models_app(config: Config, status_service: StatusService):
    models_app = aiohttp.web.Application()

    orchestrator = await create_orchestrator(
        models_app.loop, kube_config=config.orchestrator)

    async def _init_orchestrator(_):
        async with orchestrator:
            yield orchestrator
    models_app.cleanup_ctx.append(_init_orchestrator)

    models_handler = ModelsHandler(
        config=config, orchestrator=orchestrator, status_service=status_service)
    models_handler.register(models_app)
    return models_app


async def create_statuses_app(status_service: StatusService):
    statuses_app = aiohttp.web.Application()
    statuses_handler = StatusesHandler(status_service=status_service)
    statuses_handler.register(statuses_app)
    return statuses_app


async def create_app(config: Config):
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app['config'] = config

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    status_service = InMemoryStatusService()
    models_app = await create_models_app(config=config, status_service=status_service)
    api_v1_app.add_subapp('/models', models_app)
    statuses_app = await create_statuses_app(status_service=status_service)
    api_v1_app.add_subapp('/statuses', statuses_app)

    app.add_subapp('/api/v1', api_v1_app)
    return app


def main():
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info('Loaded config: %r', config)

    loop = asyncio.get_event_loop()

    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
