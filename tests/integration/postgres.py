import asyncio
from collections.abc import AsyncIterator

import aiodocker
import aiodocker.containers
import asyncpg
import pytest
from aiodocker.types import JSONObject
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_api.config import PostgresConfig
from platform_api.config_factory import EnvironConfigFactory
from platform_api.postgres import MigrationRunner, make_async_engine


@pytest.fixture(scope="session")
async def admin_postgres_dsn(
    docker: aiodocker.Docker, reuse_docker: bool
) -> AsyncIterator[str]:
    image_name = "postgres:16.11"
    container_name = "postgres-admin"
    container_config: JSONObject = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": ["POSTGRES_HOST_AUTH_METHOD=trust"],
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
    image_name = "postgres:16.11"
    container_name = "postgres"
    container_config: JSONObject = {
        "Image": image_name,
        "AttachStdout": False,
        "AttachStderr": False,
        "HostConfig": {"PublishAllPorts": True},
        "Env": ["POSTGRES_HOST_AUTH_METHOD=trust"],
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
    val = await container.port(5432)
    assert val is not None
    port = int(val[0]["HostPort"])
    return f"postgresql+asyncpg://postgres@{host}:{port}/postgres"


async def _wait_for_postgres_server(
    postgres_dsn: str, attempts: int = 5, interval_s: float = 1
) -> None:
    if postgres_dsn.startswith("postgresql+asyncpg://"):
        postgres_dsn = "postgresql" + postgres_dsn[len("postgresql+asyncpg") :]
    attempt = 0
    while attempt < attempts:
        try:
            attempt = attempt + 1
            conn = await asyncpg.connect(postgres_dsn, timeout=5.0)
            await conn.close()
            return
        except Exception:
            pass
        await asyncio.sleep(interval_s)


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
async def sqalchemy_engine(
    postgres_config: PostgresConfig,
) -> AsyncIterator[AsyncEngine]:
    engine = make_async_engine(postgres_config)
    try:
        yield engine
    finally:
        await engine.dispose()
