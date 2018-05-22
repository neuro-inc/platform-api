import asyncio
import logging

import aiohttp.web


from .config import Config
from .models import ModelsHandler


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


async def create_app(config: Config):
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app['config'] = config

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    models_app = aiohttp.web.Application()
    models_handler = ModelsHandler()
    models_handler.register(models_app)
    api_v1_app.add_subapp('/models', models_app)

    app.add_subapp('/api/v1', api_v1_app)
    return app


def main():
    init_logging()
    config = Config.from_environ()
    logging.info('Loaded config: %r', config)

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(create_app(config))
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
