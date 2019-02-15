import asyncio
import random
import time

import aiohttp
import pytest


MIN_PORT = 49152
MAX_PORT = 65535
LOCALHOST = "127.0.0.1"


@pytest.fixture
async def alice_job(api_config, alice, client):
    job_request_payload = {
        "container": {
            "image": "nginx",
            "resources": {"cpu": 0.1, "memory_mb": 16},
            "ssh": {"port": 80},
        }
    }
    headers = {"Authorization": f"Bearer {alice.token}"}
    response = await client.post(
        api_config.jobs_url, headers=headers, json=job_request_payload
    )
    payload = await response.json()
    assert response.status == 202
    job_id = payload["id"]
    job_url = f"{api_config.jobs_url}/{job_id}"

    for i in range(30):
        response = await client.get(job_url, headers=headers)
        assert response.status == 200
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
    assert response.status == 204


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_port_forward(ssh_auth_config, api_config, alice, alice_job, client):
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:22",
            "-o",
            f"ProxyCommand=ssh -o StrictHostKeyChecking=no -p "
            f"{str(ssh_auth_config.port)} nobody@{ssh_auth_config.ip} "
            f'\'{{"method": "job_port_forward", "token": "{alice.token}",'
            f'"params": {{"job": "{alice_job}", "port": 22}}}}\'',
            "-o",
            "ExitOnForwardFailure=yes",
            "nobody@127.0.0.1",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
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
    assert response.status == 200
    text = await response.text()
    assert "Welcome to nginx!" in text
    proc.kill()


@pytest.mark.usefixtures("api")
@pytest.mark.asyncio
async def test_wrong_user(ssh_auth_config, api_config, bob, alice_job):
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:22",
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
async def test_incorrect_token(ssh_auth_config, api_config, alice_job):
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:22",
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
    ssh_auth_config, api_config, alice, alice_job, client
):
    retries = 5
    for i in range(retries):
        port = random.randint(MIN_PORT, MAX_PORT)
        command = [
            "ssh",
            "-NL",
            f"{port}:{alice_job}:23",
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
    with pytest.raises(aiohttp.ClientOSError):
        response = await client.get(f"http://{LOCALHOST}:{port}")
    proc.kill()
    exit_code = await proc.wait()
