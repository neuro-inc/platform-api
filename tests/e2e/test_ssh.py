import asyncio
import time

import pytest


@pytest.fixture
async def alice_job(api_config, alice, client):
    job_request_payload = {
        "container": {
            "image": "ubuntu",
            "command": "tail -f /dev/null",
            "resources": {"cpu": 0.1, "memory_mb": 16},
        }
    }
    headers = {"Authorization": f"Bearer {alice.token}"}
    response = await client.post(
        api_config.jobs_url, headers=headers, json=job_request_payload
    )
    payload = await response.json()
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


@pytest.mark.asyncio
async def test_simple_command(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["true"]}}',
    ]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 0


@pytest.mark.asyncio
async def test_wrong_user(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{bob.token}", "job": "{alice_job}", "command": ["true"]}}',
    ]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 77


@pytest.mark.asyncio
async def test_incorrect_token(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "some_token", "job": "{alice_job}", "command": ["true"]}}',
    ]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 77


@pytest.mark.asyncio
async def test_no_payload(ssh_auth_config, api_config, bob, alice_job):
    command = ["ssh", "-p", str(ssh_auth_config.port), f"nobody@{ssh_auth_config.ip}"]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 65


@pytest.mark.asyncio
async def test_incorrect_payload(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        "true",
    ]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 65


@pytest.mark.asyncio
async def test_nonzero_error_code(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["false"]}}',
    ]
    proc = await asyncio.create_subprocess_exec(*command)
    exit_code = await proc.wait()
    assert exit_code == 1


@pytest.mark.asyncio
async def test_pass_stdin(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["grep", "o"]}}',
    ]
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, stderr = await proc.communicate(input=b"one\ntwo\nthree\nfour\nfive")
    exit_code = await proc.wait()
    assert exit_code == 0
    output = bytes(stdout.decode())

    # filter logging out
    filtered_output = list(
        filter(
            lambda line: "platform_api.ssh_auth.authorize" not in line,
            output.splitlines(),
        )
    )
    assert filtered_output == ["one", "two", "four"]
