from datetime import timedelta
from typing import Dict, Optional

import aiohttp.web
import trafaret as t
from aiohttp import ClientResponseError
from aiohttp.web_exceptions import HTTPNotFound
from neuro_auth_client import AuthClient, Permission, check_permissions

from platform_api.config import Config
from platform_api.orchestrator.job import AggregatedRunTime
from platform_api.orchestrator.jobs_storage import JobFilter, JobsStorage


TIMEDELTA_ONE_MINUTE = timedelta(minutes=1)


def create_aggregated_runtime_validator(optional_fields: bool) -> t.Trafaret:
    return t.Dict(
        {
            t.Key("total_gpu_run_time_minutes", optional=optional_fields): t.Int,
            t.Key("total_non_gpu_run_time_minutes", optional=optional_fields): t.Int,
        }
    )


def create_stats_response_validator() -> t.Trafaret:
    return t.Dict(
        {
            "name": t.String,
            "quota": create_aggregated_runtime_validator(True),
            "jobs": create_aggregated_runtime_validator(False),
        }
    )


class StatsHandler:
    def __init__(self, *, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

        self._stats_response_validator = create_stats_response_validator()

    @property
    def jobs_storage(self) -> JobsStorage:
        return self._app["jobs_service"].jobs_storage

    @property
    def auth_client(self) -> AuthClient:
        return self._app["auth_client"]

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes([aiohttp.web.get("/users/{username}", self.handle_get_stats)])

    async def handle_get_stats(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        username = request.match_info["username"]

        permission = Permission(uri=f"user://{username}", action="read")
        await check_permissions(request, [permission])

        try:
            user = await self.auth_client.get_user(username)
        except ClientResponseError:
            raise HTTPNotFound()

        response_payload = {"name": username}

        if user.quota is not None:
            response_payload["quota"] = convert_run_time_to_response(
                AggregatedRunTime.from_quota(user.quota)
            )
        else:
            response_payload["quota"] = dict()

        run_time_filter = JobFilter(owners={user.name})
        run_time = await self.jobs_storage.get_aggregated_run_time(run_time_filter)
        response_payload["jobs"] = convert_run_time_to_response(run_time)

        self._stats_response_validator.check(response_payload)

        return aiohttp.web.json_response(
            data=response_payload, status=aiohttp.web.HTTPOk.status_code
        )


def convert_run_time_to_response(run_time: AggregatedRunTime) -> Dict[str, int]:
    result: Dict[str, int] = {}
    gpu_minutes = timedelta_to_minutes(run_time.total_gpu_run_time_delta)
    if gpu_minutes is not None:
        result["total_gpu_run_time_minutes"] = gpu_minutes
    non_gpu_minutes = timedelta_to_minutes(run_time.total_non_gpu_run_time_delta)
    if non_gpu_minutes is not None:
        result["total_non_gpu_run_time_minutes"] = non_gpu_minutes
    return result


def timedelta_to_minutes(delta: timedelta) -> Optional[int]:
    if delta == timedelta.max:
        return None
    return round(delta / TIMEDELTA_ONE_MINUTE)
