import time
import pytest
import requests
import subprocess


@pytest.fixture
async def alice_job(api_config, alice):
    job_request_payload = {
        "container": {
            "image": "ubuntu",
            "command": "tail -f /dev/null",
            "resources": {"cpu": 0.1, "memory_mb": 16},
        }
    }
    headers = {"Authorization": f"Bearer {alice.token}"}
    response = requests.post(
        api_config.jobs_url, headers=headers, json=job_request_payload
    )
    payload = response.json()
    job_id = payload["id"]
    job_url = f"{api_config.jobs_url}/{job_id}"

    for i in range(30):
        response = requests.get(job_url, headers=headers)
        assert response.status_code == 200, response.json()
        jobs_payload = response.json()
        status_name = jobs_payload["status"]
        if status_name == "running":
            break
        if status_name == "failed":
            pytest.fail(f"Job failed: {jobs_payload}")
        time.sleep(1)

    yield job_id

    response = requests.delete(job_url, headers=headers)
    assert response.status_code == 204


def test_simple_command(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["true"]}}',
    ]
    exit_code = subprocess.call(command)
    assert exit_code == 0


def test_wrong_user(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{bob.token}", "job": "{alice_job}", "command": ["true"]}}',
    ]
    exit_code = subprocess.call(command)
    assert exit_code == 77


def test_incorrect_token(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "some_token", "job": "{alice_job}", "command": ["true"]}}',
    ]
    exit_code = subprocess.call(command)
    assert exit_code == 77


def test_no_payload(ssh_auth_config, api_config, bob, alice_job):
    command = ["ssh", "-p", str(ssh_auth_config.port), f"nobody@{ssh_auth_config.ip}"]
    exit_code = subprocess.call(command)
    assert exit_code == 65


def test_incorrect_payload(ssh_auth_config, api_config, bob, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        "true",
    ]
    exit_code = subprocess.call(command)
    assert exit_code == 65


def test_nonzero_error_code(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["false"]}}',
    ]
    exit_code = subprocess.call(command)
    assert exit_code == 1


def test_pass_stdin(ssh_auth_config, api_config, alice, alice_job):
    command = [
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-p",
        str(ssh_auth_config.port),
        f"nobody@{ssh_auth_config.ip}",
        f'{{"token": "{alice.token}", "job": "{alice_job}", "command": ["grep", "o"]}}',
    ]
    p = subprocess.Popen(
        command, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    output = p.communicate(input=b"one\ntwo\nthree\nfour\nfive")[0].decode()
    # filter logging out
    filtered_output = list(
        filter(
            lambda line: "platform_api.ssh_auth.authorize" not in line,
            output.splitlines(),
        )
    )
    assert filtered_output == ["one", "two", "four"]
