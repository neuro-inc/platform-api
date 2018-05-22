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
            aiohttp.web.post(r'/', self.handle_post),
            aiohttp.web.get(r'/', self.handle_get),
        ))

    async def handle_post(self, request):
        # TODO validation for request
        data = await request.json()

        job_id = str(uuid.uuid4())

        job_request = JobRequest(job_id=job_id, container_name=job_id, docker_image=data['container']['image'])
        job = Job(orchestrator=self._orchestrator, job_request=job_request)
        status = await job.start()
        print(data)
        print(status)
        return aiohttp.web.Response(status=200)

    async def handle_get(self, request):
        return aiohttp.web.Response(status=200)
