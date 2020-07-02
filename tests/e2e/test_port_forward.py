import asyncio
import random
import time
from typing import AsyncIterator

import aiohttp
import pytest

from .conftest import PlatformConfig, SSHAuthConfig, _User


MIN_PORT = 49152
MAX_PORT = 65535
LOCALHOST = "127.0.0.1"


@pytest.fixture
async def alice_job(
    api_config: PlatformConfig, alice: _User, client: aiohttp.ClientSession
) -> AsyncIterator[str]:
    job_request_payload = {
        "container": {"image": "nginx", "resources": {"cpu": 0.1, "memory_mb": 16}}
    }
    headers = {"Authorization": f"Bearer {alice.token}"}
    response = await client.post(
        api_config.jobs_url, headers=headers, json=job_request_payload
    )
    assert response.status == 202, await response.text()
    payload = await response.json()
    job_id = payload["id"]
    assert isinstance(job_id, str)
    job_url = f"{api_config.jobs_url}/{job_id}"

    for i in range(30):
        response = await client.get(job_url, headers=headers)
        assert response.status == 200, await response.text()
        jobs_payload = await response.json()
        assert jobs_payload
        status_name = jobs_payload["status"]
        if status_name == "running":
            break
        if status_name == "failed":
            pytest.fail(f"Job failed: {jobs_payload}")
        time.sleep(1)

    yield job_id

    response = await client.delete(job_url, headers=headers)
    assert response.status == 204, await response.text()


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_port_forward_no_job_namespace(
    ssh_auth_config: SSHAuthConfig,
    api_config: PlatformConfig,
    alice: _User,
    alice_job: str,
    client: aiohttp.ClientSession,
) -> None:
    # TODO (artem 16-May-2019) this is temporary test, remove after port-forwarding
    #  for the jobs without namespace is disabled (see issue #700)
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:80",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "{alice.token}",'
            f'"params": {{"job": "{alice_job}", "port": 80}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        assert proc.stderr
        try:
            line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            assert b"in use" in line
            continue
        except asyncio.TimeoutError:
            break
    response = await client.get(f"http://{LOCALHOST}:{port}")
    assert response.status == 200, await response.text()
    text = await response.text()
    assert "Welcome to nginx!" in text
    proc.kill()


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_port_forward(
    ssh_auth_config: SSHAuthConfig,
    api_config: PlatformConfig,
    alice: _User,
    alice_job: str,
    client: aiohttp.ClientSession,
) -> None:
    retries = 5
    job_domain_name = f"{alice_job}.{ssh_auth_config.jobs_namespace}"
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{job_domain_name}:80",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "{alice.token}",'
            f'"params": {{"job": "{alice_job}", "port": 80}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        assert proc.stderr
        try:
            line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            assert b"in use" in line
            continue
        except asyncio.TimeoutError:
            break
    response = await client.get(f"http://{LOCALHOST}:{port}")
    assert response.status == 200, await response.text()
    text = await response.text()
    assert "Welcome to nginx!" in text
    proc.kill()


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_wrong_user(
    ssh_auth_config: SSHAuthConfig,
    api_config: PlatformConfig,
    bob: _User,
    alice_job: str,
) -> None:
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:22",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "{bob.token}",'
            f'"params": {{"job": "{alice_job}", "port": 22}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        assert proc.stderr
        try:
            line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"in use" not in line:
                break
        except asyncio.TimeoutError:
            break
    exit_code = await proc.wait()
    assert exit_code == 255


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_incorrect_token(
    ssh_auth_config: SSHAuthConfig, api_config: PlatformConfig, alice_job: str
) -> None:
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:22",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "token",'
            f'"params": {{"job": "{alice_job}", "port": 22}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        assert proc.stderr
        try:
            line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"in use" not in line:
                break
        except asyncio.TimeoutError:
            break
    exit_code = await proc.wait()
    assert exit_code == 255


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_port_forward_nonexposed(
    ssh_auth_config: SSHAuthConfig,
    api_config: PlatformConfig,
    alice: _User,
    alice_job: str,
    client: aiohttp.ClientSession,
) -> None:
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:23",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "{alice.token}",'
            f'"params": {{"job": "{alice_job}", "port": 23}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        assert proc.stderr
        try:
            line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"Warning: Permanently added" in line:
                line = await asyncio.wait_for(proc.stderr.readline(), 30)
            if b"in use" not in line:
                break
        except asyncio.TimeoutError:
            break
    with pytest.raises(aiohttp.ClientConnectionError):
        await client.get(f"http://{LOCALHOST}:{port}")
    proc.kill()
    await proc.wait()
