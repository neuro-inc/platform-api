import pytest
import requests


class ApiClient:

    def __init__(self, *, endpoint: str) -> None:
        self._endpoint = endpoint
        self._session = requests.Session()

        self._training_api = None

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def training(self):
        if not self._training_api:
            self._training_api = TrainingApiClient(
                self._endpoint, self._session)
        return self._training_api


class TrainingApiClient:
    def __init__(self, endpoint, session):
        self._session = session

        self._base_url = endpoint + '/training'
        self._train_url = self._base_url + '/train'

    def train(self):
        payload = {
        }
        self._session.post(self._train_url, json=payload)


@pytest.fixture
def api_endpoint():
    return 'http://localhost:8080/api/v1'


@pytest.fixture
def api_client(api_endpoint):
    with ApiClient(endpoint=api_endpoint) as client:
        yield client


class TestApi:
    def test_base(self, api_client):
        training = api_client.training
        training.train()
