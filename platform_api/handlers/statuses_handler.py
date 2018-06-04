from platform_api.orchestrator.jobs_service import JobsService, JobError


import aiohttp.web


class StatusesHandler:
    def __init__(self, *, jobs_service: JobsService) -> None:
        self._jobs_service = jobs_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/{status_id}', self.handle_get),
        ))

    async def handle_get(self, request):
        status_id = request.match_info['status_id']
        status = await self._jobs_service.get_status_by_status_id(status_id)
        return aiohttp.web.json_response(data={'status': status}, status=200)