import uuid

import aiohttp.web
from decouple import config as decouple_config

from platform_api.orchestrator import Job, JobRequest, KubeOrchestrator, KubeConfig


class ModelsHandler:
    def __init__(self):
        self._orchestrator = KubeOrchestrator(config=KubeConfig(decouple_config('KUBE_PROXY_URL')))
        pass

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('/test1', self.handle_post),
            aiohttp.web.get('/test2', self.handle_get),
        ))

    def _validation_request(self, data: dict) -> dict:
        # TODO validation for request
        return data

    async def _create_job(self, data: dict):
        async with self._orchestrator as orchestrator:
            job_id = str(uuid.uuid4())
            job_request = JobRequest(job_id=job_id, container_name=job_id, docker_image=data['container']['image'])
            job = Job(orchestrator=orchestrator, job_request=job_request)
            status = await job.start()
            return status, job

    async def handle_post(self, request):
        data = await request.json()
        data = self._validation_request(data)
        status_job, job = await self._create_job(data)

        return aiohttp.web.Response(status=200)

    async def handle_get(self, request):
        return aiohttp.web.Response(status=200)
