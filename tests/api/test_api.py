import enum
import time
from typing import NamedTuple, Any
import uuid

import pytest
import requests
from responses import RequestsMock


class ApiClientException(Exception):
    pass


class ApiClient:

    def __init__(self, *, endpoint: str) -> None:
        self._endpoint = endpoint
        self._session = requests.Session()

        self._models_api = None
        self._statuses_api = None

    @property
    def endpoint(self):
        return self._endpoint

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
            self._models_api = ModelsApiClient(self, self._session)
        return self._models_api

    @property
    def statuses(self):
        if not self._statuses_api:
            self._statuses_api = StatusesApiClient(
                self._endpoint, self._session)
        return self._statuses_api


class ModelsApiClient:
    def __init__(self, client, session):
        self._client = client
        self._session = session

        self._base_url = self._client.endpoint + '/models'

    def create(self, *args):
        payload = {
            'docker_image': '',
            'dataset_id': None,
        }
        response = self._session.post(self._base_url, json=payload)
        assert response.status_code == 202
        assert 'Location' in response.headers

        status_id = response.json()['status_id']
        return StatusProxy(self._client.statuses, status_id)

    def get(self, model_id: str):
        url = f'{self._base_url}/{model_id}'
        response = self._session.get(url)
        assert response.status_code == 200
        # TODO: check the schema and wrap into Model
        payload = response.json()
        return Model(**payload)


class Model(NamedTuple):
    id: str
    meta: Any


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


# TODO: rename
class StatusProxy:
    def __init__(self, client, status_id):
        self._client = client
        self._status_id = status_id

    def get(self):
        return self._client.get(self._status_id)

    def wait(self):
        return self._client.wait(self._status_id)


class StatusesApiClient:
    def __init__(self, endpoint, session):
        self._session = session

        self._base_url = endpoint + '/statuses'

    def get(self, status_id: str):
        url = f'{self._base_url}/{status_id}'
        response = self._session.get(url)
        return Status.from_response(response)

    # TODO: better defaults (as constants)
    def wait(self, status_id: str, interval_s: int=1, max_attempts: int=5):
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


@pytest.fixture
def responses():
    with RequestsMock() as m:
        yield m


@pytest.fixture
def model(responses, api_endpoint):
    model_id = str(uuid.uuid4())
    status_id = str(uuid.uuid4())
    url = f'{api_endpoint}/models'
    headers = {
        'Location': f'{api_endpoint}/statuses/{status_id}'
    }
    payload = {'id': model_id, 'status_id': status_id}
    responses.add(
        responses.POST, url=url, status=202, headers=headers,
        json=payload)

    yield model_id, status_id


@pytest.fixture
def pending_model(responses, api_endpoint, model):
    _, status_id = model
    register_pending_status(responses, api_endpoint, status_id)


@pytest.fixture
def model_with_statuses(responses, api_endpoint, model):
    _, status_id = model
    register_successfull_status(responses, api_endpoint, status_id)
    yield model


@pytest.fixture
def succeeded_model(model_with_statuses):
    yield model_with_statuses


def register_pending_status(responses, api_endpoint, status_id, number=2):
    url = f'{api_endpoint}/statuses/{status_id}'
    responses.add(responses.GET, url=url, status=200, json={
        'id': status_id, 'status': StatusName.PENDING.value})
    responses.add(responses.GET, url=url, status=200, json={
        'id': status_id, 'status': StatusName.PENDING.value})


def register_successfull_status(responses, api_endpoint, status_id):
    # TODO: 303 + Location
    url = f'{api_endpoint}/statuses/{status_id}'
    responses.add(responses.GET, url=url, status=303, json={
        'id': status_id, 'status': StatusName.SUCCEEDED.value})


class TestApi:
    def test_flow(self, api_client, succeeded_model):
        model_id, status_id = succeeded_model
        status_proxy = api_client.models.create()
        status = status_proxy.wait()
        assert status.id == status_id
        assert status.name == StatusName.SUCCEEDED


class TestModelsApi:
    def test_create(self, api_client, succeeded_model):
        model_id, status_id = succeeded_model
        status_proxy = api_client.models.create()
        status = status_proxy.wait()
        assert status.id == status_id
        assert status.name == StatusName.SUCCEEDED

    def test_get(self, responses, api_client, api_endpoint):
        model_id = str(uuid.uuid4())
        url = f'{api_endpoint}/models/{model_id}'
        payload = {'id': model_id, 'meta': {}}
        responses.add(responses.GET, url=url, status=200, json=payload)

        model = api_client.models.get(model_id)
        assert model.id == model_id
        assert model.meta == {}


class TestStatusesApi:
    def test_get(self, responses, api_client, api_endpoint):
        status_id = str(uuid.uuid4())
        url = f'{api_endpoint}/statuses/{status_id}'
        responses.add(responses.GET, url=url, status=200, json={
            'id': status_id, 'status': StatusName.PENDING.value})

        status = api_client.statuses.get(status_id)
        assert status.id == status_id
        assert status.name == StatusName.PENDING

    def test_wait(self, responses, api_client, api_endpoint):
        status_id = str(uuid.uuid4())
        register_successfull_status(responses, api_endpoint, status_id)

        status = api_client.statuses.wait(
            status_id, interval_s=0, max_attempts=3)
        assert status.id == status_id
        assert status.name == StatusName.SUCCEEDED
