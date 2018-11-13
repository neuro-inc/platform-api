from typing import NamedTuple

from platform_api.config import Config, DatabaseConfig, ServerConfig, StorageConfig
from platform_api.ssh.server import SSHServer
from platform_api.orchestrator.job import Job, JobRequest

import pytest
from pathlib import PurePath


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
    orchestrator = config.orchestrator
    srv = SSHServer('0.0.0.0', 8022, orchestrator)
    await srv.start
    yield srv
    await srv.stop()


@pytest.fixture
async def job(kube_orchestrator):
    job_id = str(uuid.uuid4())
    container = Container(
        image="ubuntu",
        command="sleep 5",
        resources=ContainerResources(cpu=0.1, memory_mb=16),
    )
    job_request = JobRequest(job_id=job_id, container=container)
    job = Job(orchestrator=kube_orchestrator, job_request=job_request, owner="test-owner")
    return job


async def wait_for_completion(
    job: Job, interval_s: float = 1.0, max_attempts: int = 30
):
    for _ in range(max_attempts):
        status = await job.query_status()
        if status.is_finished:
            return status
        else:
            await asyncio.sleep(interval_s)
    else:
        pytest.fail("too long")


async def wait_for_failure(*args, **kwargs):
    status = await wait_for_completion(*args, **kwargs)
    assert status == JobStatus.FAILED


async def wait_for_success(*args, **kwargs):
    status = await wait_for_completion(*args, **kwargs)
    assert status == JobStatus.SUCCEEDED


async def test_simple(ssh_server, job):
    status = await job.start()
    await wait_for_success(job)

    async with asyncssh.connect(ssh_server.host, ssh_server.port) as conn:
        result = await conn.run('echo "Hello!"', check=True)
        stdout = str(result.stdout)
        assert stdout == "Hello!"

    status = await job.delete()
    assert status == JobStatus.SUCCEEDED
