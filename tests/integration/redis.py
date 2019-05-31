import asyncio
from typing import AsyncIterator

import aiodocker
import aioredis
import pytest
from async_timeout import timeout

from platform_api.redis import RedisConfig, create_redis_client


IMAGE_NAME = "redis:4"
CONTAINER_NAME = "redis"


@pytest.fixture(scope="session")
async def _redis_server(
    docker: aiodocker.Docker, reuse_docker: bool, network: str
) -> AsyncIterator[RedisConfig]:
    container_config = {
        "Image": IMAGE_NAME,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "NetworkingConfig": {
            "EndpointsConfig": {network: {"Aliases": [CONTAINER_NAME]}}
        },
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(CONTAINER_NAME)
            if container["State"]["Running"]:
                redis_config = await create_redis_config(container)
                await wait_for_redis_server(redis_config)
                yield redis_config
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(IMAGE_NAME)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(IMAGE_NAME)

    container = await docker.containers.create_or_replace(
        name=CONTAINER_NAME, config=container_config
    )
    await container.start()

    redis_config = await create_redis_config(container)
    await wait_for_redis_server(redis_config)
    yield redis_config

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


@pytest.fixture
async def redis_server(_redis_server: RedisConfig) -> AsyncIterator[RedisConfig]:
    async with create_redis_client(_redis_server) as client:
        await client.flushall()
        yield _redis_server
        await client.flushall()


async def create_redis_config(
    container: aiodocker.containers.DockerContainer
) -> RedisConfig:
    host = "0.0.0.0"
    port = int((await container.port(6379))[0]["HostPort"])
    db = 0
    uri = f"redis://{host}:{port}/{db}"
    return RedisConfig(uri=uri)


async def wait_for_redis_server(
    redis_config: RedisConfig, timeout_s: float = 30, interval_s: float = 1
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                async with create_redis_client(redis_config) as redis_client:
                    response = await redis_client.ping()
                    if response == b"PONG":
                        break
            except (OSError, aioredis.errors.RedisError):
                pass
            await asyncio.sleep(interval_s)


@pytest.fixture
def redis_config(redis_server: RedisConfig) -> RedisConfig:
    return redis_server


@pytest.fixture
async def redis_client(redis_config: RedisConfig) -> AsyncIterator[aioredis.Redis]:
    async with create_redis_client(redis_config) as client:
        yield client
