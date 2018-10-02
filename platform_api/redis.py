from dataclasses import dataclass

import aioredis
from async_generator import asynccontextmanager


@dataclass(frozen=True)
class RedisConfig:
    uri: str

    conn_pool_size: int = 10
    conn_timeout_s: float = 10.0


@asynccontextmanager
async def create_redis_client(config: RedisConfig) -> aioredis.Redis:
    client = await aioredis.create_redis_pool(
        config.uri, maxsize=config.conn_pool_size, timeout=config.conn_timeout_s
    )
    try:
        yield client
    finally:
        client.close()
        await client.wait_closed()
