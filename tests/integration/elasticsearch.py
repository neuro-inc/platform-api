import subprocess
import time
from typing import List

import pytest

from platform_api.api import create_elasticsearch_client
from platform_api.elasticsearch import ElasticsearchAuthConfig, ElasticsearchConfig


def wait_for_service(service_name: str) -> List[str]:  # type: ignore
    timeout_s = 60
    interval_s = 10

    while timeout_s:
        process = subprocess.run(
            ("minikube", "service", "-n", "kube-system", service_name, "--url"),
            stdout=subprocess.PIPE,
        )
        output = process.stdout
        if output:
            return [output.decode().strip()]
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"{service_name} is unavailable.")


@pytest.fixture(scope="session")
def es_hosts_logging():
    return wait_for_service("elasticsearch-logging")


@pytest.fixture(scope="session")
def es_hosts_auth():
    return wait_for_service("elasticsearch-auth")


@pytest.fixture
def es_config(es_hosts_auth):
    return ElasticsearchConfig(hosts=es_hosts_auth)


@pytest.fixture
def es_auth_config(es_hosts_logging, es_hosts_auth):
    # need to wait for "es_hosts" and "es_hosts_auth" until the service is up
    return ElasticsearchAuthConfig(user="testuser", password="password")


@pytest.fixture
async def es_client(es_config, es_auth_config):
    async with create_elasticsearch_client(es_config, es_auth_config) as es_client:
        yield es_client


@pytest.fixture
async def es_client_no_auth(es_hosts_logging):
    es_config = ElasticsearchConfig(hosts=es_hosts_logging)
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client
