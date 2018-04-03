import time
import uuid

import pytest
import requests


@pytest.fixture
def singularity_api_endpoint():
    return 'http://localhost:7099/singularity/api'


@pytest.fixture
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
            last_exc = exc

        time.sleep(delay_s)
    else:
        pytest.fail(
            f'Could not reach Singularity. API endpoing: {url}.')


class TestSingularity:
    def _send_request(self, singularity_api_endpoint, request_id):
        url = singularity_api_endpoint + '/requests'
        payload = {
            "id": request_id,
            "requestType": "RUN_ONCE",
        }
        response = requests.post(url, json=payload)
        assert response.status_code == 200

    def _send_deploy(self, singularity_api_endpoint, request_id, deploy_id):
        url = singularity_api_endpoint + '/deploys'
        payload = {
            "deploy": {
                "requestId": request_id,
                "id": deploy_id,
                "containerInfo": {
                    "type": "MESOS",
                },
                "command": "echo 'YAY!'",
                "shell": True,
            }
        }
        response = requests.post(url, json=payload)
        assert response.status_code == 200

    def _get_deploy(self, singularity_api_endpoint, request_id, deploy_id):
        url = (
            f'{singularity_api_endpoint}/history'
            f'/request/{request_id}/deploy/{deploy_id}'
        )
        response = requests.get(url)
        assert response.status_code == 200
        return response.json()

    def _poll_deploy(self, singularity_api_endpoint, request_id, deploy_id):
        delay_s = 1
        max_attempts = 30

        for _ in range(max_attempts):
            payload = self._get_deploy(
                singularity_api_endpoint, request_id, deploy_id)
            if 'deployResult' in payload:
                state = payload['deployResult']['deployState']
                if state == 'SUCCEEDED':
                    return
                elif state != 'WAITING':
                    pytest.fail(f'Deploy {request_id}:{deploy_id} failed')
            time.sleep(delay_s)
        else:
            pytest.fail(f'Deploy {request_id}:{deploy_id} took too long')

    @pytest.mark.usefixtures('singularity')
    def test_deploy(self, singularity_api_endpoint):
        request_id = str(uuid.uuid4()).replace('-', '')
        deploy_id = str(uuid.uuid4()).replace('-', '')

        self._send_request(singularity_api_endpoint, request_id)
        self._send_deploy(singularity_api_endpoint, request_id, deploy_id)
        self._poll_deploy(singularity_api_endpoint, request_id, deploy_id)
