from platform_api.orchestrator import JobsService


import aiohttp.web


class JobsHandler:
    def __init__(self, *, jobs_service: JobsService) -> None:
        self._jobs_service = jobs_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('', self.handle_get_jobs),
            aiohttp.web.post('/create/{job_id}', self.handle_get),
            aiohttp.web.post('/cancel/{job_id}', self.handle_get),
            aiohttp.web.get('/{job_id}/status', self.handle_get),
        ))

    async def handle_get_jobs(self, request):
        jobs = await self._jobs_service.get_all()
        return aiohttp.web.json_response(data={'jobs': jobs}, status=200)

    async def handle_get(self, request):
        return {"test": "test"}
