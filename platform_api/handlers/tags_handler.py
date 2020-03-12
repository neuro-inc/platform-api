import aiohttp.web
import trafaret as t
from aiohttp_security import check_authorized

from platform_api.config import Config
from platform_api.handlers.jobs_handler import JobFilterFactory
from platform_api.handlers.validators import create_tag_list_per_user_validator
from platform_api.orchestrator.jobs_service import JobsService
from platform_api.orchestrator.jobs_storage import JobsStorage
from platform_api.user import untrusted_user


class TagsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

        self._job_filter_factory = JobFilterFactory()
        self._tags_response_validator = t.Dict(
            {"tags": create_tag_list_per_user_validator()}
        )

    @property
    def _jobs_service(self) -> JobsService:
        return self._app["jobs_service"]

    @property
    def _jobs_storage(self) -> JobsStorage:
        return self._jobs_service.jobs_storage

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes([aiohttp.web.get("", self.handle_get_tags)])

    async def handle_get_tags(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        await check_authorized(request)
        user = await untrusted_user(request)

        tags = await self._jobs_storage.get_tags(user.name)
        response_payload = {"tags": tags}

        self._tags_response_validator.check(response_payload)
        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )
