import uuid
import asyncio

import aiohttp.web
from decouple import config as decouple_config
from trafaret.constructor import construct

from platform_api.orchestrator import Job, JobRequest, KubeOrchestrator, KubeConfig


class Models:
    def __init__(self):
        self._orchestrator = KubeOrchestrator(config=KubeConfig(decouple_config('KUBE_PROXY_URL')))

    async def create_job(self, data: dict):
        async with self._orchestrator as orchestrator:
            job_id = str(uuid.uuid4())
            job_request = JobRequest(job_id=job_id, container_name=job_id, docker_image=data['container']['image'])
            job = Job(orchestrator=orchestrator, job_request=job_request)
            start_status = await job.start()
            return start_status, job_id

    async def get_status(self, job_id: str):
        async with self._orchestrator as orchestrator:
            status = await orchestrator.status_job(job_id)
            return status

    async def delete_model(self, job_id: str):
        async with self._orchestrator as orchestrator:
            status = await orchestrator.delete_job(job_id)
            return status


class ModelsHandler:
    def __init__(self):
        self._models = Models()
        self.validator = construct({"container": {"image": str}})

    def register(self, app):
        app.add_routes((
            aiohttp.web.post('/train', self.handle_post),
            # TODO this is alias for train. do same like train. just run container
            aiohttp.web.post('/evaluation', self.handle_post),
            aiohttp.web.get('/{job_id}', self.handle_get),
            aiohttp.web.delete('/{job_id}', self.handle_delete),
        ))

    def _validation_request(self, data: dict):
        self.validator(data)

    async def handle_post(self, request):
        data = await request.json()
        self._validation_request(data)
        status, job_id = await self._models.create_job(data)
        return aiohttp.web.json_response(data={'status': status, 'job_id': job_id}, status=201)

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        status = await self._models.get_status(job_id)
        return aiohttp.web.json_response(data={'status': status}, status=200)

    async def handle_delete(self, request):
        job_id = request.match_info['job_id']
        status = await self._models.delete_model(job_id)
        return aiohttp.web.json_response(data={'status': status}, status=200)
