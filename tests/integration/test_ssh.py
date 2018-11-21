from pathlib import PurePath
from typing import NamedTuple

import asyncssh
import pytest

from platform_api.config import Config, DatabaseConfig, ServerConfig, StorageConfig
from platform_api.orchestrator.job import JobRequest
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.kube_orchestrator import KubeOrchestrator, PodDescriptor
from platform_api.ssh.server import SSHServer


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self):
        return f"http://{self.host}:{self.port}/api/v1"

    @property
    def model_base_url(self):
        return self.endpoint + "/models"

    @property
    def jobs_base_url(self):
        return self.endpoint + "/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"

    @property
    def ping_url(self):
        return self.endpoint + "/ping"


@pytest.fixture
def config(kube_config, redis_config, auth_config):
    server_config = ServerConfig()
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
    database_config = DatabaseConfig(redis=redis_config)  # type: ignore
    return Config(
        server=server_config,
        storage=storage_config,
        orchestrator=kube_config,
        database=database_config,
        auth=auth_config,
    )


@pytest.fixture
async def ssh_server(config):
    async with KubeOrchestrator(config=config.orchestrator) as orchestrator:
        srv = SSHServer("0.0.0.0", 8022, orchestrator)
        await srv.start()
        yield srv
        await srv.stop()


@pytest.fixture
async def delete_pod_later(kube_client):
    pods = []

    async def _add_pod(pod):
        pods.append(pod)

    yield _add_pod

    for pod in pods:
        try:
            await kube_client.delete_pod(pod.name)
        except Exception:
            pass


@pytest.mark.asyncio
async def test_simple(ssh_server, kube_client, kube_config, delete_pod_later):
    container = Container(
        image="ubuntu",
        command="sleep 10",
        resources=ContainerResources(cpu=0.1, memory_mb=16),
    )
    job_request = JobRequest.create(container)
    pod = PodDescriptor.from_job_request(
        kube_config.create_storage_volume(), job_request
    )
    await delete_pod_later(pod)
    await kube_client.create_pod(pod)
    await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)

    async with asyncssh.connect(
        ssh_server.host, ssh_server.port, username=pod.name
    ) as conn:
        proc = await conn.create_process("pwd")
        stdout = await proc.stdout.read()
        assert stdout == "/\r\n"


@pytest.mark.asyncio
async def test_shell(ssh_server, kube_client, kube_config, delete_pod_later):
    container = Container(
        image="ubuntu",
        command="sleep 100",
        resources=ContainerResources(cpu=0.1, memory_mb=16),
    )
    job_request = JobRequest.create(container)
    pod = PodDescriptor.from_job_request(
        kube_config.create_storage_volume(), job_request
    )
    await delete_pod_later(pod)
    await kube_client.create_pod(pod)
    await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)

    async with asyncssh.connect(
        ssh_server.host, ssh_server.port, username=pod.name
    ) as conn:
        proc = await conn.create_process("bash")
        proc.stdin.write("pwd\n")

        await proc.stdout.readuntil("\r\n/\r\n")

        proc.stdin.write_eof()


@pytest.mark.asyncio
async def test_exit_code(ssh_server, kube_client, kube_config, delete_pod_later):
    container = Container(
        image="ubuntu",
        command="sleep 10",
        resources=ContainerResources(cpu=0.1, memory_mb=16),
    )
    job_request = JobRequest.create(container)
    pod = PodDescriptor.from_job_request(
        kube_config.create_storage_volume(), job_request
    )
    await delete_pod_later(pod)
    await kube_client.create_pod(pod)
    await kube_client.wait_pod_is_running(pod_name=pod.name, timeout_s=60.0)

    async with asyncssh.connect(
        ssh_server.host, ssh_server.port, username=pod.name
    ) as conn:
        proc = await conn.create_process("bash")
        proc.stdin.write("exit 42\n")

        ret = await proc.wait()
        assert ret.exit_status == 42
