import aiohttp.web
import trafaret as t
from neuro_auth_client import Permission, check_permissions
from neuromation.api import Action

from platform_api.config import Config
from platform_api.orchestrator.jobs_storage import JobFilter, JobsStorage
from platform_api.user import authorized_user


def create_aggregated_runtime_validator() -> t.Trafaret:
    return t.Dict(
        {
            t.Key("total_gpu_run_time_minutes", optional=True): t.Int,
            t.Key("total_non_gpu_run_time_minutes", optional=True): t.Int,
        }
    )


def create_stats_response_validator() -> t.Trafaret:
    return t.Dict(
        {
            "name": t.String,
            t.Key("quota", optional=True): create_aggregated_runtime_validator(),
            "jobs": create_aggregated_runtime_validator(),
        }
    )


class StatsHandler:
    def __init__(
        self, *, app: aiohttp.web.Application, config: Config, jobs_storage: JobsStorage
    ) -> None:
        self._app = app
        self._config = config

        self._jobs_storage = jobs_storage
        self._stats_response_validator = create_stats_response_validator()

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes([aiohttp.web.put("/users/{username}", self.handle_get_stats)])

    async def handle_get_stats(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        username = request.match_info["username"]

        permission = Permission(uri=f"user://{username}", action=Action.READ)
        await check_permissions(request, [permission])

        user = await authorized_user(request)

        response_payload = {"name": username}

        if user.has_quota():
            pass
            # response_payload["quota"] = user.quota.to_primitive()

        run_time_filter = JobFilter(owners={user.name})
        print(run_time_filter)
        # run_time = await self._jobs_storage.get_aggregated_run_time(run_time_filter)
        # response_payload["jobs"] = run_time.to_primivite()

        self._stats_response_validator.check(response_payload)

        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )
