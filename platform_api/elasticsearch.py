from dataclasses import dataclass
from typing import Sequence

from aioelasticsearch import AIOHttpTransport, Elasticsearch
from aiohttp import BasicAuth
from async_generator import asynccontextmanager


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]


@dataclass(frozen=True)
class ElasticsearchAuthConfig:
    user: str
    password: str


@dataclass(frozen=True)
class LoggingConfig:
    elasticsearch: ElasticsearchConfig
    elasticsearch_auth: ElasticsearchAuthConfig


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig, auth_config: ElasticsearchAuthConfig
) -> Elasticsearch:
    http_auth = BasicAuth(auth_config.user, auth_config.password)
    async with Elasticsearch(hosts=config.hosts, http_auth=http_auth) as es_client:
        await es_client.ping()
        yield es_client
