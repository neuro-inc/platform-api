import asyncio

import aiodocker
import pytest
from async_timeout import timeout

from platform_api.redis import RedisConfig, create_redis_client


@pytest.fixture(scope='session')
async def redis_server(docker, reuse_docker):
    image_name = 'redis:4'
    container_name = 'redis'
    container_config = {
        'Image': image_name,
        'AttachStdout': False,
        'AttachStderr': False,
        'HostConfig': {
            'PublishAllPorts': True,
        },
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container['State']['Running']:
                redis_config = await create_redis_config(container)
                await wait_for_redis_server(redis_config)
                yield redis_config
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.get(image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config)
    await container.start()

    redis_config = await create_redis_config(container)
    await wait_for_redis_server(redis_config)
    yield redis_config

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


async def create_redis_config(container) -> RedisConfig:
    host = 'localhost'
    port = int((await container.port(6379))[0]['HostPort'])
    db = 0
    uri = f'redis://{host}:{port}/{db}'
    return RedisConfig(uri=uri)  # type: ignore


async def wait_for_redis_server(
        redis_config: RedisConfig,
        timeout_s: float = 30, interval_s: float = 1):
    async with timeout(timeout_s):
        while True:
            try:
                async with create_redis_client(redis_config) as redis_client:
                    response = await redis_client.ping()
                    if response == b'PONG':
                        break
            except OSError:
                pass
            await asyncio.sleep(interval_s)


@pytest.fixture(scope='session')
def redis_config(redis_server):
    return redis_server


@pytest.fixture
async def redis_client(redis_config):
    async with create_redis_client(redis_config) as client:
        yield client


@pytest.fixture
async def clear_redis(redis_client):
    await redis_client.flushdb()
    yield
    await redis_client.flushdb()
