import subprocess
import time
from typing import List

import pytest

from platform_api.api import create_elasticsearch_client
from platform_api.elasticsearch import ElasticsearchConfig


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
    """ Waits for elasticsearch logging service and returns its URLs.
    """
    return wait_for_service("elasticsearch-logging")


@pytest.fixture(scope="session")
def es_hosts_auth():
    """ Waits for elasticsearch authentication proxy service and returns its URLs.
    """
    return wait_for_service("elasticsearch-auth")


@pytest.fixture
def es_config_with_auth(es_hosts_logging, es_hosts_auth):
    """ Config to access Elasticsearch directly via the elasticsearch-auth proxy.
    This fixture waits for es_hosts and es_hosts_auth so that the services are up.
    """
    return ElasticsearchConfig(
        hosts=es_hosts_auth, user="testuser", password="password"
    )


@pytest.fixture
def es_config(es_hosts_logging):
    """ Config to access Elasticsearch directly, bypassing the elasticsearch-auth proxy.
    """
    return ElasticsearchConfig(hosts=es_hosts_logging)


@pytest.fixture
async def es_client_with_auth(es_config_with_auth):
    """ Elasticsearch client that goes through elasticsearch-auth proxy.
    """
    async with create_elasticsearch_client(es_config_with_auth) as es_client_with_auth:
        yield es_client_with_auth


@pytest.fixture
async def es_client(es_config):
    """ Elasticsearch client that goes directly to elasticsearch-logging service
    without any authentication.
    """
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client
