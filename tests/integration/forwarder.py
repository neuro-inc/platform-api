import pytest
import aiodocker
from pathlib import Path
from .test_api import api, ApiConfig
import asyncio
from typing import List, Any
from aiodocker.containers import DockerContainer
from yarl import URL
import hashlib

IMAGE_ASSET = Path(__file__).parent / "assets/proxy.tar"
IMAGE_NAME = "proxy:latest"

sha1sum = hashlib.sha1()
with open(IMAGE_ASSET, 'rb') as source:
  block = source.read(2**16)
  while len(block) != 0:
    sha1sum.update(block)
    block = source.read(2**16)
IMAGE_NAME = 'proxy:' + sha1sum.hexdigest()

SSH_KEY_ASSET = Path(__file__).parent / "assets/root_rsa"
CONTAINER_NAME = "proxy"
FORWARDED_API_PORT = 8080


@pytest.fixture(scope="session")
async def forwarder_image(docker: aiodocker.Docker) -> str:
    image_name = IMAGE_NAME
    try:
        await docker.images.inspect(IMAGE_NAME)
    except aiodocker.exceptions.DockerError:
        with IMAGE_ASSET.open(mode="r+b") as fileobj:
            await docker.images.build(
                fileobj=fileobj, tag=image_name, encoding="identity"
            )
    except Exception as e:
        pass
    return image_name


@pytest.fixture(scope="session")
async def forwarder_container(
    docker: aiodocker.Docker, forwarder_image: str, network: str
) -> DockerContainer:
    container_config = {
        "Image": forwarder_image,
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
        "HostConfig": {"PublishAllPorts": True},
        "NetworkingConfig": {
            "EndpointsConfig": {network: {"Aliases": [CONTAINER_NAME]}}
        },
    }
    container = await docker.containers.create_or_replace(
        name=CONTAINER_NAME, config=container_config
    )
    await container.start()

    yield container

    await container.kill()
    await container.delete(force=True)


@pytest.fixture
async def forwarded_api(
    api: ApiConfig, forwarder_container: DockerContainer, tmp_path
) -> URL:
    """
    ssh -p 32801 -R 8080:localhost:2080 root@localhost -i root_rsa  -o "StrictHostKeyChecking=no" sleep 1h
    """
    port_info: List[Any] = await forwarder_container.port(22)
    port_number = port_info.pop()["HostPort"]

    cmd = [
        "ssh",
        "-p",
        port_number,
        "-R",
        f"0.0.0.0:{FORWARDED_API_PORT}:{api.host}:{api.port}",
        "-i",
        f"{SSH_KEY_ASSET}",        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        f"UserKnownHostsFile={tmp_path / 'known_hosts'}",
        "root@localhost",
        "sleep",
        "1h",
    ]
    process = await asyncio.create_subprocess_exec(*cmd)
    await asyncio.sleep(5) # TODO Remove
    yield URL(f"http://{CONTAINER_NAME}:{FORWARDED_API_PORT}")
    try:
        process.kill()
    except ProcessLookupError:
        pass
