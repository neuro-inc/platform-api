import os
import time

import pytest
import requests


@pytest.fixture(scope='session')
def api_endpoint_url():
    return os.environ['PLATFORM_API_URL']


@pytest.fixture(scope='session')
def api_ping_url(api_endpoint_url):
    return f'{api_endpoint_url}/ping'


@pytest.fixture(scope='session')
def api_models_url(api_endpoint_url):
    return f'{api_endpoint_url}/models'


@pytest.fixture(scope='session')
def api_statuses_url(api_endpoint_url):
    return f'{api_endpoint_url}/statuses'


@pytest.fixture(scope='session')
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
        pytest.fail(f'Unable to connect to Platform API: {url}')


@pytest.mark.usefixtures('api')
def test_basic_command(api_models_url, api_statuses_url):
    model_request_payload = {
        'container': {
            'image': 'ubuntu',
            'command': 'true',
            'resources': {
                'cpu': 0.1,
                'memory_mb': 16,
            },
        },
        'dataset_storage_uri': 'storage://',
        'result_storage_uri': 'storage://result',
    }
    response = requests.post(api_models_url, json=model_request_payload)
    assert response.status_code == 202
    model_payload = response.json()
    status_id = model_payload['status_id']
    status_url = f'{api_statuses_url}/{status_id}'

    for _ in range(30):
        response = requests.get(status_url)
        assert response.status_code == 200
        status_payload = response.json()
        status_name = status_payload['status']
        if status_name == 'succeeded':
            break
        if status_name == 'failed':
            pytest.fail(f'Job failed: {status_payload}')
        time.sleep(1)
