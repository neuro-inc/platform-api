import subprocess
import time

import pytest
from aioelasticsearch import Elasticsearch


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
async def es_client(es_hosts):
    es_client = Elasticsearch(hosts=es_hosts)
    async with es_client:
        await es_client.ping()
        yield es_client
