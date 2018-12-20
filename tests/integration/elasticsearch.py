import subprocess
import time

import pytest
from aioelasticsearch import Elasticsearch

from platform_api.api import create_elasticsearch_client
from platform_api.config import ElasticsearchConfig


@pytest.fixture(scope="session")
def es_hosts():
    timeout_s = 60
    interval_s = 60

    while timeout_s:
        process = subprocess.run(
            (
                "minikube",
                "service",
                "-n",
                "kube-system",
                "elasticsearch-logging",
                "--url",
            ),
            stdout=subprocess.PIPE,
        )
        output = process.stdout
        if output:
            return [output.decode()]
        time.sleep(interval_s)
        timeout_s -= interval_s

    pytest.fail("Elasticsearch is unavailable.")


@pytest.fixture
def es_config(es_hosts):
    return ElasticsearchConfig(hosts=es_hosts)


@pytest.fixture
async def es_client(es_config):
    async with create_elasticsearch_client(es_config) as es_client:
        yield es_client
