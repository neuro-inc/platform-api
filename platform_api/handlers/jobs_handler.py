from platform_api.orchestrator import JobsService


import aiohttp.web


class JobsHandler:
    def __init__(self, *, jobs_service: JobsService) -> None:
        self._jobs_service = jobs_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('', self.handle_get_jobs),
            aiohttp.web.delete('/delete/{job_id}', self.handle_delete),
            aiohttp.web.get('/{job_id}/status', self.handle_get_status),
        ))

    async def handle_get_status(self, request):
        job_id = request.match_info['job_id']
        job = await self._jobs_service.get(job_id)
        status = await job.status()
        return aiohttp.web.json_response(data={'status': status}, status=200)

    async def handle_get_jobs(self, request):
        jobs = await self._jobs_service.get_all()
        return aiohttp.web.json_response(data={'jobs': jobs}, status=200)

    async def handle_delete(self, request):
        job_id = request.match_info['job_id']
        status = await self._jobs_service.delete(job_id)
        return aiohttp.web.json_response(data={'status': status}, status=200)

