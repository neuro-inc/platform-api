import time
from typing import AsyncIterator

import aiodocker
import aiodocker.containers
import asyncpg
import pytest
from asyncpg import Connection
from asyncpg.pool import Pool

from platform_api.config import PostgresConfig
from platform_api.config_factory import EnvironConfigFactory
from platform_api.postgres import MigrationRunner, create_postgres_pool


@pytest.fixture(scope="session")
async def admin_postgres_dsn(
    docker: aiodocker.Docker, reuse_docker: bool
) -> AsyncIterator[str]:
    image_name = "postgres:11.3"
    container_name = "postgres-admin"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
    }
    dsn = "postgresql://postgres@postgres:5432/postgres"

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                postgres_dsn = await _make_postgres_dsn(container)
                await _wait_for_postgres_server(postgres_dsn)
                yield dsn
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    postgres_dsn = await _make_postgres_dsn(container)
    await _wait_for_postgres_server(postgres_dsn)
    yield dsn

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


@pytest.fixture(scope="session")
async def postgres_dsn(
    docker: aiodocker.Docker, reuse_docker: bool
) -> AsyncIterator[str]:
    image_name = "postgres:11.3"
    container_name = "postgres"
    container_config = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
    }

    if reuse_docker:
        try:
            container = await docker.containers.get(container_name)
            if container["State"]["Running"]:
                postgres_dsn = await _make_postgres_dsn(container)
                await _wait_for_postgres_server(postgres_dsn)
                yield postgres_dsn
                return
        except aiodocker.exceptions.DockerError:
            pass

    try:
        await docker.images.inspect(image_name)
    except aiodocker.exceptions.DockerError:
        await docker.images.pull(image_name)

    container = await docker.containers.create_or_replace(
        name=container_name, config=container_config
    )
    await container.start()

    postgres_dsn = await _make_postgres_dsn(container)
    await _wait_for_postgres_server(postgres_dsn)
    yield postgres_dsn

    if not reuse_docker:
        await container.kill()
        await container.delete(force=True)


async def _make_postgres_dsn(container: aiodocker.containers.DockerContainer) -> str:
    host = "0.0.0.0"
    port = int((await container.port(5432))[0]["HostPort"])
    return f"postgresql://postgres@{host}:{port}/postgres"


async def _wait_for_postgres_server(
    postgres_dsn: str, attempts: int = 5, interval_s: float = 1
) -> None:
    attempt = 0
    while attempt < attempts:
        try:
            attempt = attempt + 1
            conn: Connection = await asyncpg.connect(postgres_dsn, timeout=5.0)
            await conn.close()
            return
        except Exception:
            pass
        time.sleep(interval_s)


@pytest.fixture
async def postgres_config(postgres_dsn: str) -> AsyncIterator[PostgresConfig]:

    db_config = PostgresConfig(
        postgres_dsn=postgres_dsn,
        alembic=EnvironConfigFactory().create_alembic(postgres_dsn),
    )
    migration_runner = MigrationRunner(db_config)
    await migration_runner.upgrade()

    yield db_config

    await migration_runner.downgrade()


@pytest.fixture
async def postgres_pool(postgres_config: PostgresConfig) -> AsyncIterator[Pool]:
    async with create_postgres_pool(postgres_config) as pool:
        yield pool
