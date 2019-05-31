import aiodocker
import pytest
from yarl import URL

from .auth import CONTAINER_NAME as AUTH_CONTAINER_NAME, create_token
from .conftest import NOTIFICATIONS_CONTAINER_NAME as CONTAINER_NAME
from .test_api import ApiConfig, api_with_oauth


IMAGE_NAME = "gcr.io/light-reality-205619/platformnotificationsapi:latest"


@pytest.fixture()
async def notifications_server(
    docker: aiodocker.Docker,
    reuse_docker: bool,
    api_with_oauth: ApiConfig,
    forwarded_api: URL,
    network: str,
    forwarded_notifications_server: URL,
) -> None:
    container_config = {
        "Image": IMAGE_NAME,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": [
            f"NP_NOTIFICATIONS_PLATFORM_API_URL={forwarded_api}/api/v1",
            f"NP_NOTIFICATIONS_PLATFORM_API_TOKEN={create_token('compute')}",
            f"NP_NOTIFICATIONS_PLATFORM_AUTH_URL=http://{AUTH_CONTAINER_NAME}:8080",
            f"NP_NOTIFICATIONS_PLATFORM_AUTH_TOKEN={create_token('compute')}",
            f"NP_NOTIFICATIONS_ZAPIER_URL=http://127.0.0.1:1234",  # TODO
        ],
        "NetworkingConfig": {
            "EndpointsConfig": {network: {"Aliases": [CONTAINER_NAME]}}
        },
    }
    container = await docker.containers.create_or_replace(
        name=CONTAINER_NAME, config=container_config
    )

    await container.start()
    # TODO: add ping here
    yield
    try:
        await container.kill()
        await container.delete(force=True)
    except aiodocker.exceptions.DockerError:
        pass
