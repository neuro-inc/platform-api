from typing import Any, AsyncIterator

import aiodocker
import pytest


PYTEST_REUSE_DOCKER_OPT = "--reuse-docker"
NETWORK_NAME = "neuromation"


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        PYTEST_REUSE_DOCKER_OPT,
        action="store_true",
        help="Reuse existing docker containers",
    )


@pytest.fixture(scope="session")
def reuse_docker(request: Any) -> Any:
    return request.config.getoption(PYTEST_REUSE_DOCKER_OPT)


@pytest.fixture(scope="session")
async def docker() -> AsyncIterator[aiodocker.Docker]:
    client = aiodocker.Docker(api_version="v1.34")
    yield client
    await client.close()


@pytest.fixture(scope="session")
async def network(docker: aiodocker.Docker, reuse_docker: bool) -> str:
    networks = await docker.networks.list()
    exists = list(filter(lambda n: n["Name"] == NETWORK_NAME, networks))
    if exists:
        network = aiodocker.docker.DockerNetwork(docker, exists[0]["Id"])
        if not reuse_docker:
            await network.delete()
            exists = False

    if not exists:
        network_config = {"Name": NETWORK_NAME}
        network = await docker.networks.create(network_config)

    yield NETWORK_NAME
    #
    # filtered = list(filter(lambda n: n["Name"] == NETWORK_NAME, networks))
    # if filtered:
    #     network = aiodocker.docker.DockerNetwork(filtered.pop())
    # else:
    #
    # yield network["Name"]
    await network.delete()
