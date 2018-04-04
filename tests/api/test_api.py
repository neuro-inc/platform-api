import enum
import time
from typing import NamedTuple
import uuid

import pytest
import requests
import responses


class ApiClient:

    def __init__(self, *, endpoint: str) -> None:
        self._endpoint = endpoint
        self._session = requests.Session()

        self._models_api = None
        self._statuses_api = None

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def models(self):
        if not self._models_api:
            self._models_api = ModelsApiClient(
                self._endpoint, self._session)
        return self._models_api

    @property
    def statuses(self):
        if not self._statuses_api:
            self._statuses_api = StatusesApiClient(
                self._endpoint, self._session)
        return self._statuses_api


class ModelsApiClient:
    def __init__(self, endpoint, session):
        self._session = session

        self._base_url = endpoint + '/models'

    def create(self, *args):
        payload = {
            'docker_image': '',
            'dataset_id': None,
        }
        response = self._session.post(self._base_url, json=payload)
        assert response.status_code == 202
        assert 'Location' in response.headers

    def get(self, model_id: str):
        url = f'{self._base_url}/{model_id}'
        response = self._session.get(url)
        assert response.status_code == 200
        # TODO: check the schema and wrap into Model
        return response.json()


class ApiClientException(Exception):
    pass


class StatusException(ApiClientException):
    pass


class StatusName(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class Status(NamedTuple):
    id: str
    name: StatusName

    @classmethod
    def from_response(cls, response):
        assert response.status_code in {200, 303}
        payload = response.json()
        # TODO: check schema
        id_ = payload['id']
        name = StatusName(payload['status'])
        return cls(id=id_, name=name)


class StatusesApiClient:
    def __init__(self, endpoint, session):
        self._session = session

        self._base_url = endpoint + '/statuses'

    def get(self, status_id: str):
        url = f'{self._base_url}/{status_id}'
        response = self._session.get(url)
        return Status.from_response(response)

    def wait(self, status_id: str, interval_s: int, max_attempts: int):
        # TODO: should be more complicated
        for _ in range(max_attempts):
            status = self.get(status_id)
            if status.name in {StatusName.SUCCEEDED, StatusName.FAILED}:
                return status
            time.sleep(interval_s)
        else:
            raise RuntimeError('too long')


@pytest.fixture
def api_endpoint():
    return 'http://localhost:8080/api/v1'


@pytest.fixture
def api_client(api_endpoint):
    with ApiClient(endpoint=api_endpoint) as client:
        yield client


class TestApi:
    def test_base(self, api_client):
        pass
        # status = api_client.models.create()


class TestStatusesApi:
    @responses.activate
    def test_get(self, api_client, api_endpoint):
        status_id = str(uuid.uuid4())
        url = f'{api_endpoint}/statuses/{status_id}'
        responses.add(responses.GET, url=url, status=200, json={
            'id': status_id, 'status': StatusName.PENDING.value})

        status = api_client.statuses.get(status_id)
        assert status.id == status_id
        assert status.name == StatusName.PENDING

    @responses.activate
    def test_wait(self, api_client, api_endpoint):
        status_id = str(uuid.uuid4())
        url = f'{api_endpoint}/statuses/{status_id}'
        responses.add(responses.GET, url=url, status=200, json={
            'id': status_id, 'status': StatusName.PENDING.value})
        responses.add(responses.GET, url=url, status=200, json={
            'id': status_id, 'status': StatusName.PENDING.value})
        responses.add(responses.GET, url=url, status=303, json={
            'id': status_id, 'status': StatusName.SUCCEEDED.value})

        status = api_client.statuses.wait(
            status_id, interval_s=0, max_attempts=3)
        assert status.id == status_id
        assert status.name == StatusName.SUCCEEDED
