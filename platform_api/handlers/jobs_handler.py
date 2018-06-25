import aiohttp.web

from platform_api.orchestrator import JobsService


class JobsHandler:
    def __init__(self, *, jobs_service: JobsService) -> None:
        self._jobs_service = jobs_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('', self.handle_get_all),
            aiohttp.web.delete('/{job_id}', self.handle_delete),
            aiohttp.web.get('/{job_id}', self.handle_get),
        ))

    async def handle_get(self, request):
        job_id = request.match_info['job_id']
        job = await self._jobs_service.get_job(job_id)
        return aiohttp.web.json_response(data=job.to_primitive(), status=200)

    async def handle_get_all(self, request):
        # TODO use pagination. may eventually explode with OOM.
        jobs = await self._jobs_service.get_all_jobs()
        primitive_jobs = [job.to_primitive() for job in jobs]
        return aiohttp.web.json_response(
            data={'jobs': primitive_jobs}, status=200)

    async def handle_delete(self, request):
        job_id = request.match_info['job_id']
        await self._jobs_service.delete_job(job_id)
        return aiohttp.web.HTTPNoContent()
