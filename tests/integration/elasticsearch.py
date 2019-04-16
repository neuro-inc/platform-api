import subprocess
import time

import pytest

from platform_api.api import create_elasticsearch_client
from platform_api.config import ElasticsearchConfig


def wait_for_service(service_name: str) -> None:
    timeout_s = 60
    interval_s = 20

    while timeout_s:
        process = subprocess.run(
            (
                "minikube",
                "service",
                "-n",
                "kube-system",
                service_name,
                "--url",
            ),
            stdout=subprocess.PIPE,
        )
        output = process.stdout
        if output:
            return [output.decode()]
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail(f"{service_name} is unavailable.")


@pytest.fixture(scope="session")
def es_hosts():
    return wait_for_service("elasticsearch-logging")


@pytest.fixture(scope="session")
def es_hosts_auth():
    return wait_for_service("elasticsearch-auth")


@pytest.fixture
def es_config(es_hosts):
    return ElasticsearchConfig(hosts=es_hosts)


@pytest.fixture
def es_auth_config(es_hosts_auth):
    return ElasticsearchConfig(hosts=es_hosts_auth)


@pytest.fixture
async def es_client(es_config):
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client


@pytest.fixture
async def es_client_auth(es_hosts_auth):
    async with create_elasticsearch_client(es_hosts_auth) as es_client:
        yield es_client
