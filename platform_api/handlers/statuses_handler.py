from platform_api.orchestrator.status_service import StatusService


import aiohttp.web


class StatusHandler:
    def __init__(self, *, status_service: StatusService) -> None:
        self._status_service = status_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/{status_id}', self.handle_get),
        ))

    async def handle_get(self, request):
        status_id = request.match_info['status_id']
        status_info = await self._status_service.get(status_id)
        return aiohttp.web.json_response(data={'status': status_info}, status=200)
