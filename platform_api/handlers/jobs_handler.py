from typing import Any, Dict

import aiohttp.web
import trafaret as t

from platform_api.config import Config
from platform_api.orchestrator import JobsService
from platform_api.orchestrator.job import Job
from platform_api.orchestrator.job_request import JobRequest

from .job_request_builder import ContainerBuilder
from .validators import (
    create_container_request_validator, create_job_history_validator,
    create_job_status_validator
)


def create_job_request_validator() -> t.Trafaret:
    return t.Dict({
        'container': create_container_request_validator(allow_volumes=True),
    })


def create_job_response_validator() -> t.Trafaret:
    return t.Dict({
        'id': t.String,
        # `status` is left for backward compat. the python client/cli still
        # relies on it.
        'status': create_job_status_validator(),
        t.Key('http_url', optional=True): t.String,
        'history': create_job_history_validator(),
    })


def convert_job_to_job_response(job: Job) -> Dict[str, Any]:
    history = job.status_history
    current_status = history.current
    response_payload = {
        'id': job.id,
        'status': current_status.status,
        'history': {
            'status': current_status.status,
            'reason': current_status.reason,
            'description': current_status.description,
            'created_at': history.created_at_str,
        },
    }
    if job.has_http_server_exposed:
        response_payload['http_url'] = job.http_url
    if history.started_at:
        response_payload['history']['started_at'] = history.started_at_str
    if history.is_finished:
        response_payload['history']['finished_at'] = history.finished_at_str
    return response_payload


class JobsHandler:
    def __init__(
            self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._storage_config = config.storage

        self._job_request_validator = create_job_request_validator()
        self._job_response_validator = create_job_response_validator()
        self._bulk_jobs_response_validator = t.Dict({
            'jobs': t.List(self._job_response_validator),
        })

    @property
    def _jobs_service(self) -> JobsService:
        return self._app['jobs_service']

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('', self.handle_get_all),
            aiohttp.web.post('', self.create_job),
            aiohttp.web.delete('/{job_id}', self.handle_delete),
            aiohttp.web.get('/{job_id}', self.handle_get),
            aiohttp.web.get('/{job_id}/log', self.stream_log),
        ))

    async def create_job(self, request):
        orig_payload = await request.json()
        request_payload = self._job_request_validator.check(orig_payload)
        container = ContainerBuilder.from_container_payload(
            request_payload['container'], storage_config=self._storage_config
        ).build()
        job_request = JobRequest.create(container)
        job, _ = await self._jobs_service.create_job(job_request)
        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPAccepted.status_code)

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        job = await self._jobs_service.get_job(job_id)
        response_payload = convert_job_to_job_response(job)
        self._job_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code)

    async def handle_get_all(self, _):
        # TODO use pagination. may eventually explode with OOM.
        jobs = await self._jobs_service.get_all_jobs()
        response_payload = {'jobs': [
            convert_job_to_job_response(job) for job in jobs]
        }
        self._bulk_jobs_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code)

    async def handle_delete(self, request):
        job_id = request.match_info['job_id']
        await self._jobs_service.delete_job(job_id)
        return aiohttp.web.HTTPNoContent()

    async def stream_log(self, request):
        job_id = request.match_info['job_id']
        log_reader = await self._jobs_service.get_job_log_reader(job_id)
        # TODO: expose. make configurable
        chunk_size = 1024

        response = aiohttp.web.StreamResponse(status=200)
        await response.prepare(request)

        async with log_reader:
            while True:
                chunk = await log_reader.read(size=chunk_size)
                if not chunk:
                    break
                await response.write(chunk)

        await response.write_eof()
        return response
