import os
import time

import pytest
import requests


@pytest.fixture(scope="session")
def api_endpoint_url():
    return os.environ["PLATFORM_API_URL"]


@pytest.fixture(scope="session")
def api_ping_url(api_endpoint_url):
    return f"{api_endpoint_url}/ping"


@pytest.fixture(scope="session")
def api_models_url(api_endpoint_url):
    return f"{api_endpoint_url}/models"


@pytest.fixture(scope="session")
def api_jobs_url(api_endpoint_url):
    return f"{api_endpoint_url}/jobs"


@pytest.fixture(scope="session")
def api(api_ping_url):
    url = api_ping_url
    interval_s = 1
    max_attempts = 30
    for _ in range(max_attempts):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                break
        except OSError:
            pass
        time.sleep(interval_s)
    else:
        pytest.fail(f"Unable to connect to Platform API: {url}")


@pytest.mark.usefixtures("api")
def test_basic_command(api_models_url, api_jobs_url):
    model_request_payload = {
        "container": {
            "image": "ubuntu",
            "command": "true",
            "resources": {"cpu": 0.1, "memory_mb": 16},
        },
        "dataset_storage_uri": "storage://",
        "result_storage_uri": "storage://result",
    }
    response = requests.post(api_models_url, json=model_request_payload)
    assert response.status_code == 202, response.json()
    model_payload = response.json()
    job_id = model_payload["job_id"]
    jobs_url = f"{api_jobs_url}/{job_id}"

    for _ in range(30):
        response = requests.get(jobs_url)
        assert response.status_code == 200
        jobs_payload = response.json()
        status_name = jobs_payload["status"]
        if status_name == "succeeded":
            break
        if status_name == "failed":
            pytest.fail(f"Job failed: {jobs_payload}")
        time.sleep(1)
