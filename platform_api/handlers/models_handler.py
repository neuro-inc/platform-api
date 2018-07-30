from typing import Dict

import aiohttp.web
import trafaret as t

from platform_api.config import Config
from platform_api.orchestrator import JobRequest, JobsService
from platform_api.orchestrator.job_request import JobStatus

from .job_request_builder import ModelRequest
from .validators import create_container_request_validator


def create_model_request_validator() -> t.Trafaret:
    return t.Dict({
        'container': create_container_request_validator(),
        # TODO (A Danshyn 05/25/18): we may move the storage URI parsing
        # and validation here at some point
        'dataset_storage_uri': t.String,
        'result_storage_uri': t.String,
    })


def create_model_response_validator() -> t.Trafaret:
    return t.Dict({
        'job_id': t.String,
        'status': t.Enum(*JobStatus.values()),
        t.Key('http_url', optional=True): t.String,
    })


class ModelsHandler:
    def __init__(
            self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._storage_config = config.storage

        self._model_request_validator = create_model_request_validator()
        self._model_response_validator = create_model_response_validator()

    @property
    def _jobs_service(self) -> JobsService:
        return self._app['jobs_service']

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('', self.handle_post),
            # TODO add here get method for model not for job
        ))

    async def _create_job(self, model_request: ModelRequest) -> Dict:
        container = model_request.to_container()
        job_request = JobRequest.create(container)
        job, status = await self._jobs_service.create_job(job_request)
        payload = {
            'job_id': job.id,
            'status': status.value,
        }
        if container.has_http_server_exposed:
            payload['http_url'] = job.http_url
        return payload

    async def handle_post(self, request):
        request_payload = await request.json()
        self._model_request_validator.check(request_payload)

        model_request = ModelRequest(
            request_payload, storage_config=self._storage_config,
            env_prefix=self._config.env_prefix)

        response_payload = await self._create_job(model_request)
        self._model_response_validator.check(response_payload)

        return aiohttp.web.json_response(
            data=response_payload,
            status=aiohttp.web.HTTPAccepted.status_code)
