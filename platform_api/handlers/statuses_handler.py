from platform_api.orchestrator.status_service import StatusService


import aiohttp.web


class StatusesHandler:
    def __init__(self, *, status_service: StatusService) -> None:
        self._status_service = status_service

    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/{status_id}', self.handle_get),
        ))

    async def handle_get(self, request):
        status_id = request.match_info['status_id']
        status = await self._status_service.get(status_id)
        if status is None:
            return aiohttp.web.json_response(data=f"not such status_id {status_id}", status=404)
        else:
            status_value = await status.value()
            return aiohttp.web.json_response(data={'status': status_value}, status=200)
