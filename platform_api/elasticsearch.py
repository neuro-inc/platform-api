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


class ElasticsearchAIOHttpBasicAuthTransport(AIOHttpTransport):
    def __init__(self, login: str, password: str, hosts, **kwargs) -> None:
        self._auth_header_value: str = BasicAuth(login, password).encode()
        super().__init__(hosts, **kwargs)

    async def perform_request(self, method, url, headers=None, params=None, body=None):
        headers = headers or dict()
        headers["Authorization"] = self._auth_header_value
        return await super().perform_request(method, url, headers, params, body)


@asynccontextmanager
async def create_elasticsearch_client(
    config: ElasticsearchConfig, auth_config: ElasticsearchAuthConfig
) -> Elasticsearch:
    def _factory(hosts, **kwargs):
        return ElasticsearchAIOHttpBasicAuthTransport(
            login=auth_config.user, password=auth_config.password, hosts=hosts, **kwargs
        )

    async with Elasticsearch(hosts=config.hosts, transport_class=_factory) as es_client:
        await es_client.ping()
        yield es_client
