from typing import Any, AsyncIterator

import aiodocker
import pytest


PYTEST_REUSE_DOCKER_OPT = "--reuse-docker"


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
