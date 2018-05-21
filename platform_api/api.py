import asyncio
import logging


import aiohttp


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def main():
    init_logging()
    config = Config.from_environ()
    logging.info('Loaded config: %r', config)

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(create_app(config, storage))
    app.cleanup_ctx.append(_init_storage)
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)