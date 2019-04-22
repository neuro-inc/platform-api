import asyncio
import io
from pathlib import Path, PurePath
from textwrap import dedent
from typing import AsyncIterator, Awaitable, Callable, NamedTuple, Optional, cast

import aiodocker.utils
import asyncssh
import pytest
from aioelasticsearch import Elasticsearch
from yarl import URL

from platform_api.config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    IngressConfig,
    LoggingConfig,
    ServerConfig,
    StorageConfig,
)
from platform_api.elasticsearch import ElasticsearchConfig
from platform_api.orchestrator.job import JobRequest
from platform_api.orchestrator.job_request import Container, ContainerResources
from platform_api.orchestrator.kube_orchestrator import (
    KubeClient,
    KubeConfig,
    KubeOrchestrator,
    PodDescriptor,
)
from platform_api.redis import RedisConfig
from platform_api.ssh.server import SSHServer


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}/api/v1"

    @property
    def model_base_url(self) -> str:
        return self.endpoint + "/models"

    @property
    def jobs_base_url(self) -> str:
        return self.endpoint + "/jobs"

    def generate_job_url(self, job_id: str) -> str:
        return f"{self.jobs_base_url}/{job_id}"

    @property
    def ping_url(self) -> str:
        return self.endpoint + "/ping"


@pytest.fixture
def config(
    kube_config: KubeConfig,
    redis_config: RedisConfig,
    auth_config: AuthConfig,
    es_config: ElasticsearchConfig,
) -> Config:
    server_config = ServerConfig()
    storage_config = StorageConfig(host_mount_path=PurePath("/tmp"))  # type: ignore
    database_config = DatabaseConfig(redis=redis_config)  # type: ignore
    logging_config = LoggingConfig(elasticsearch=es_config)
    ingress_config = IngressConfig(
        storage_url=URL("https://neu.ro/api/v1/storage"),
        users_url=URL("https://neu.ro/api/v1/users"),
        monitoring_url=URL("https://neu.ro/api/v1/monitoring"),
    )
    return Config(
        server=server_config,
        storage=storage_config,
        orchestrator=kube_config,
        database=database_config,
        auth=auth_config,
        logging=logging_config,
        ingress=ingress_config,
    )


@pytest.fixture
async def ssh_server(
    config: Config, es_client: Optional[Elasticsearch]
) -> AsyncIterator[SSHServer]:
    async with KubeOrchestrator(
        config=cast(KubeConfig, config.orchestrator), es_client=es_client  # noqa
    ) as orchestrator:
        srv = SSHServer("0.0.0.0", 8022, orchestrator)
        await srv.start()
        yield srv
        await srv.stop()


@pytest.fixture
async def delete_pod_later(
    kube_client: KubeClient
) -> AsyncIterator[Callable[[PodDescriptor], Awaitable[None]]]:
    pods = []

    async def _add_pod(pod: PodDescriptor) -> None:
        pods.append(pod)

    yield _add_pod

    for pod in pods:
        try:
            await kube_client.delete_pod(pod.name)
        except Exception:
            pass


DOCKERFILE = """
FROM ubuntu
RUN apt-get update
RUN apt-get install -y openssh-sftp-server
CMD ["/bin/bash"]
"""


@pytest.fixture(scope="session")
async def sftp_image_name(docker: aiodocker.Docker) -> str:
    # To prepare a tar context please edit Dockerfile.sftp and pack it
    f = io.BytesIO(DOCKERFILE.encode("utf-8"))
    tar_obj = aiodocker.utils.mktar_from_dockerfile(f)
    await docker.images.build(
        fileobj=tar_obj, encoding="gzip", tag="ubuntu-sftp-server:latest"
    )
    tar_obj.close()
    ret = await docker.images.list()
    filtered = [
        img["Id"] for img in ret if "ubuntu-sftp-server:latest" in img["RepoTags"]
    ]
    return filtered[0]


@pytest.mark.asyncio
async def test_simple(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> None:
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        proc = await conn.create_process("pwd")
        stdout = await proc.stdout.read()
        assert stdout == "/\r\n"


@pytest.mark.asyncio
async def test_shell(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> None:
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        proc = await conn.create_process("bash")
        proc.stdin.write("pwd\n")

        await proc.stdout.readuntil("\r\n/\r\n")

        proc.stdin.write_eof()


@pytest.mark.asyncio
async def test_shell_with_args(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> None:
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        proc = await conn.run("bash -c 'echo Hello'")
        assert proc.stdout == "Hello\r\n"


@pytest.mark.asyncio
async def test_exit_code(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> None:
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        proc = await conn.create_process("bash")
        proc.stdin.write("exit 42\n")

        ret = await proc.wait()
        assert ret.exit_status == 42


@pytest.mark.asyncio
async def test_pass_env(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
) -> None:
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        proc = await conn.run(
            "bash -c 'echo $CUSTOM_ENV'", env={"CUSTOM_ENV": "Custom value"}
        )
        assert proc.stdout == "'Custom value'\r\n"


@pytest.mark.asyncio
async def test_sftp_basic(
    ssh_server: SSHServer,
    kube_client: KubeClient,
    kube_config: KubeConfig,
    delete_pod_later: Callable[[PodDescriptor], Awaitable[None]],
    tmpdir: Path,
) -> None:
    container = Container(
        image="atmoz/sftp",
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
        ssh_server.host, ssh_server.port, username=pod.name, known_hosts=None
    ) as conn:
        async with conn.start_sftp_client() as sftp:
            ret = await sftp.listdir()
            assert sorted(ret) == [
                ".",
                "..",
                ".dockerenv",
                "bin",
                "boot",
                "dev",
                "entrypoint",
                "etc",
                "home",
                "lib",
                "lib64",
                "media",
                "mnt",
                "opt",
                "proc",
                "root",
                "run",
                "sbin",
                "srv",
                "sys",
                "tmp",
                "usr",
                "var",
            ]

            await sftp.get("/etc/os-release", tmpdir, follow_symlinks=True)
            body = (tmpdir / "os-release").read_text("utf-8")
            assert body == dedent(
                """\
                 PRETTY_NAME="Debian GNU/Linux 9 (stretch)"
                 NAME="Debian GNU/Linux"
                 VERSION_ID="9"
                 VERSION="9 (stretch)"
                 ID=debian
                 HOME_URL="https://www.debian.org/"
                 SUPPORT_URL="https://www.debian.org/support"
                 BUG_REPORT_URL="https://bugs.debian.org/"
            """
            )
            await asyncio.sleep(0.1)
    await asyncio.sleep(0.1)
