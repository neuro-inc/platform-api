import aiohttp.web
from trafaret.constructor import construct

from platform_api.orchestrator import Job, JobRequest, Orchestrator
from platform_api.orchestrator.job_request import Container


class ModelsHandler:
    def __init__(self, *, orchestrator: Orchestrator) -> None:
        self._orchestrator = orchestrator
        self.validator = construct({"container": {"image": str}})

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

    def _validation_request(self, data: dict):
        self.validator(data)

    async def handle_post(self, request):
        data = await request.json()
        self._validation_request(data)
        status, job_id = await self._create_job(data)
        return aiohttp.web.json_response(data={'status': status, 'job_id': job_id}, status=201)

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        status = await self._orchestrator.status_job(job_id)
        return aiohttp.web.json_response(data={'status': status}, status=200)
