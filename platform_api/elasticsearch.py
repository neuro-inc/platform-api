from dataclasses import dataclass
from typing import Optional, Sequence

from aioelasticsearch import Elasticsearch
from aiohttp import BasicAuth
from async_generator import asynccontextmanager


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]
    user: Optional[str] = None
    password: Optional[str] = None


@asynccontextmanager
async def create_elasticsearch_client(config: ElasticsearchConfig) -> Elasticsearch:
    http_auth: Optional[BasicAuth]
    if config.user:
        http_auth = BasicAuth(config.user, config.password)  # type: ignore
    else:
        http_auth = None

    async with Elasticsearch(hosts=config.hosts, http_auth=http_auth) as es_client:
        await es_client.ping()
        yield es_client
