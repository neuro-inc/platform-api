import asyncio
import logging
import json

import aiohttp.web


from .config import Config
from .models import ModelsHandler
from .orchestrator import KubeOrchestrator, KubeConfig


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


async def create_kube_orchestrator(loop: asyncio.AbstractEventLoop):
    # TODO remove it
    process = await asyncio.create_subprocess_exec(
        'kubectl', 'config', 'view', '-o', 'json',
        stdout=asyncio.subprocess.PIPE)
    output, _ = await process.communicate()
    payload_str = output.decode().rstrip()
    kube_config_payload = json.loads(payload_str)

    cluster_name = 'minikube'
    clusters = {
        cluster['name']: cluster['cluster']
        for cluster in kube_config_payload['clusters']}
    cluster = clusters[cluster_name]

    user_name = 'minikube'
    users = {
        user['name']: user['user']
        for user in kube_config_payload['users']}
    user = users[user_name]

    kube_config = KubeConfig(
        endpoint_url=cluster['server'],
        cert_authority_path=cluster['certificate-authority'],
        auth_cert_path=user['client-certificate'],
        auth_cert_key_path=user['client-key']
    )

    kube_orchestrator = KubeOrchestrator(config=kube_config, loop=loop)
    return kube_orchestrator


async def create_models_app(config: Config):
    models_app = aiohttp.web.Application()
    kube_orchestrator = await create_kube_orchestrator(models_app.loop)
    models_handler = ModelsHandler(orchestrator=kube_orchestrator)
    models_handler.register(models_app)
    return models_app


async def create_app(config: Config):
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app['config'] = config

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    models_app = create_models_app(config)
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
