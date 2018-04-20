import enum
import time
from typing import NamedTuple, Any
import unittest.mock
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
        self._inference_api = None

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

    @property
    def inference(self):
        if not self._inference_api:
            self._inference_api = InferenceApiClient(self, self._session)
        return self._inference_api


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

        # TODO: handle Location properly
        payload = response.json()
        status_id = payload['status_id']
        return StatusProxy(self._client.statuses, status_id, payload)

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
    PENDING = 'PENDING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'


class Status(NamedTuple):
    id: str
    name: StatusName

    @classmethod
    def from_response(cls, response):
        assert response.status_code in {200, 303}
        payload = response.json()
        # TODO: check schema
        id_ = payload['status_id']
        name = StatusName(payload['status'])
        return cls(id=id_, name=name)


# TODO: rename
class StatusProxy:
    def __init__(self, client, status_id, payload=None):
        self._client = client
        self._status_id = status_id
        self._payload = payload

    @property
    def payload(self):
        return self._payload

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
        response = self._session.get(url, allow_redirects=False)
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


# TODO: consider renaming
class InferenceApiClient:
    def __init__(self, client, session):
        self._client = client
        self._session = session

        self._base_url = self._client.endpoint + '/inference'

    def create(self, model_id, **kwargs):
        payload = {
        }
        response = self._session.post(self._base_url, json=payload)
        assert response.status_code == 202
        assert 'Location' in response.headers

        # TODO: handle Location properly
        status_id = response.json()['status_id']
        return StatusProxy(self._client.statuses, status_id)

    def get(self, inference_id, **kwargs):
        pass

    def score(self, inference_id, payload):
        pass


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
    yield model


@pytest.fixture
def model_with_statuses(responses, api_endpoint, model):
    model_id, status_id = model

    # TODO: extract a function
    url = f'{api_endpoint}/models/{model_id}'
    payload = {'id': model_id, 'meta': {}}
    responses.add(responses.GET, url=url, status=200, json=payload)

    register_successfull_status(responses, api_endpoint, status_id)
    yield model


@pytest.fixture
def succeeded_model(model_with_statuses):
    yield model_with_statuses


@pytest.fixture
def inference(responses, api_endpoint):
    inference_id = str(uuid.uuid4())
    status_id = str(uuid.uuid4())
    url = f'{api_endpoint}/inference'
    headers = {
        'Location': f'{api_endpoint}/statuses/{status_id}'
    }
    payload = {'id': inference_id, 'status_id': status_id}
    responses.add(
        responses.POST, url=url, status=202, headers=headers,
        json=payload)

    yield inference_id, status_id


@pytest.fixture
def succeeded_inference(responses, api_endpoint, inference):
    _, status_id = inference
    register_successfull_status(responses, api_endpoint, status_id)
    yield inference


def register_pending_status(responses, api_endpoint, status_id, number=2):
    url = f'{api_endpoint}/statuses/{status_id}'
    responses.add(responses.GET, url=url, status=200, json={
        'status_id': status_id, 'status': StatusName.PENDING.value})
    responses.add(responses.GET, url=url, status=200, json={
        'status_id': status_id, 'status': StatusName.PENDING.value})


def register_successfull_status(responses, api_endpoint, status_id):
    # TODO: 303 + Location
    url = f'{api_endpoint}/statuses/{status_id}'
    responses.add(responses.GET, url=url, status=303, json={
        'status_id': status_id, 'status': StatusName.SUCCEEDED.value})


class TestApi:
    def test_flow(self, api_client, succeeded_model, succeeded_inference):
        model_id, model_status_id = succeeded_model
        inference_id, inference_status_id = succeeded_inference

        model_status_proxy = api_client.models.create()
        model_status = model_status_proxy.wait()
        assert model_status.id == model_status_id
        assert model_status.name == StatusName.SUCCEEDED

        model_id = model_status_proxy.payload['id']
        model = api_client.models.get(model_id)
        assert model.id == model_id

        inference_status_proxy = api_client.inference.create(model_id)
        inference_status = inference_status_proxy.wait()
        assert inference_status.id == inference_status_id
        assert inference_status.name == StatusName.SUCCEEDED

        # TODO: try to score


class TestModelsApi:
    def test_create(self, api_client, pending_model):
        model_id, status_id = pending_model
        status_proxy = api_client.models.create()

        status_proxy.get()
        status = status_proxy.get()
        assert status.id == status_id
        assert status.name == StatusName.PENDING

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
            'status_id': status_id, 'status': StatusName.PENDING.value})

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


class TestInferenceApi:
    def test_create(
            self, responses, api_client, api_endpoint, succeeded_inference):
        inference_id, status_id = succeeded_inference
        status_proxy = api_client.inference.create(model_id='1')
        status = status_proxy.wait()
        assert status.id == status_id
        assert status.name == StatusName.SUCCEEDED


@pytest.fixture(scope='session')
def real_api_endpoint():
    return 'http://platformapi:8080'


@pytest.mark.usefixtures('singularity')
class TestTrainingApi:
    def test_docker_image(self, real_api_endpoint):
        api_endpoint = real_api_endpoint
        # TODO: should we have the /api/v1 path prefix?
        url = f'{api_endpoint}/models'
        payload = {
            "code": {
                "env": {
                    "MODEL_PATH": "/var/user"
                },
                "image": (
                    "registry.neuromation.io/neuromationorg/platformapi-dummy"
                ),
            },
            "dataset_storage_uri": "storage://data",
            "result_storage_uri": "storage://data",
            "resources": {
                "cpus": 1,
                "memoryMb": 128
            }
        }
        response = requests.post(url, json=payload)
        assert response.status_code == 202
        status_payload = response.json()
        assert status_payload == {
            'model_id': unittest.mock.ANY,
            'status_id': unittest.mock.ANY,
            'status': 'PENDING',
        }
        status_id = status_payload['status_id']
        expected_location = f'{api_endpoint}/statuses/{status_id}'
        assert response.headers['Location'] == expected_location

        api_client = ApiClient(endpoint=api_endpoint)
        status = api_client.statuses.wait(status_id, max_attempts=10)
        assert status.name == StatusName.SUCCEEDED
