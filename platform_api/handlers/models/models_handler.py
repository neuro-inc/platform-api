import uuid

import aiohttp.web
from trafaret.constructor import construct

from platform_api.orchestrator import Job, JobRequest, Orchestrator


class ModelsHandler:
    def __init__(self, *, orchestrator: Orchestrator):
        self._orchestrator = orchestrator
        self.validator = construct({"container": {"image": str}})

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('/', self.handle_post),
            aiohttp.web.post('/evaluation', self.handle_post),
            aiohttp.web.get('/{job_id}', self.handle_get),
        ))

    async def _create_job(self, data: dict):
        job_id = str(uuid.uuid4())
        job_request = JobRequest(job_id=job_id, container_name=job_id, docker_image=data['container']['image'])
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        start_status = await job.start()
        return start_status, job_id

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
