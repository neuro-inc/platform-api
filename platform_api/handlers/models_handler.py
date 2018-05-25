import aiohttp.web
import trafaret as t

from platform_api.orchestrator import Job, JobRequest, Orchestrator
from platform_api.orchestrator.job_request import Container


class ModelsHandler:
    def __init__(self, *, orchestrator: Orchestrator) -> None:
        self._orchestrator = orchestrator

        self._model_request_validator = self._create_model_request_validator()

    def _create_model_request_validator(self) -> t.Trafaret:
        return t.Dict({
            'container': t.Dict({
                'image': t.String,
                }),
            # TODO (A Danshyn 05/25/18): resources
            # 'dataset_storage_uri': t.String,
            # 'result_storage_uri': t.String,
        })

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('/', self.handle_post),
            aiohttp.web.get('/{job_id}', self.handle_get),
        ))

    async def _create_job(self, data: dict):
        container = Container(image=data['container']['image'])  # type: ignore
        job_request = JobRequest.create(container)
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        start_status = await job.start()
        return start_status, job.id

    async def handle_post(self, request):
        data = await request.json()
        self._model_request_validator.check(data)
        status, job_id = await self._create_job(data)
        return aiohttp.web.json_response(
            data={'status': status, 'job_id': job_id},
            status=aiohttp.web.HTTPAccepted.status_code)

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        status = await self._orchestrator.status_job(job_id)
        return aiohttp.web.json_response(
            data={'status': status}, status=aiohttp.web.HTTPOk.status_code)
