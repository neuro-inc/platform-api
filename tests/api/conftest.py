import time

import pytest
import requests


@pytest.fixture(scope='session')
def singularity_api_endpoint():
    return 'http://tests_singularity_1:7099/singularity/api'


@pytest.fixture(scope='session')
def singularity(singularity_api_endpoint):
    url = singularity_api_endpoint + '/state'

    delay_s = 1
    max_attempts = 30

    for _ in range(max_attempts):
        try:
            response = requests.get(url)

            if response.status_code == 200:
                payload = response.json()
                if payload['activeSlaves'] > 0:
                    break
        except OSError as exc:
            pass

        time.sleep(delay_s)
    else:
        pytest.fail(
            f'Could not reach Singularity. API endpoing: {url}.')
