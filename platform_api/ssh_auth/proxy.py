import json
import logging
from dataclasses import dataclass
from typing import List

import aiohttp
import trafaret as t
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthPolicy
from yarl import URL

from .executor import Executor


log = logging.getLogger(__name__)


def create_exec_request_validator() -> t.Trafaret:
    return t.Dict({"token": t.String, "job": t.String, "command": t.List(t.String)})


@dataclass(frozen=True)
class ExecRequest:
    token: str
    job: str
    command: List[str]


class AuthError(Exception):
    pass


class AuthenticationError(AuthError):
    pass


class AuthorizationError(AuthError):
    pass


class IllegalArgumentError(ValueError):
    pass


class ExecProxy:
    def __init__(
        self, auth_client: AuthClient, platform_url: URL, executor: Executor
    ) -> None:
        self._auth_client = auth_client
        self._jobs_url = platform_url / "jobs"
        self._exec_request_validator = create_exec_request_validator()
        self._executor = executor

    async def _get_owner(self, token: str, job_id: str) -> None:
        async with aiohttp.ClientSession() as session:
            job_url = self._jobs_url / job_id
            headers = {"Authorization": f"Bearer {token}"}
            response = await session.get(job_url, headers=headers)
            if response.status != 200:
                raise AuthorizationError(f"Return code:{response.status}")
            job_payload = await response.json()
            return job_payload["owner"]

    async def _authorize(self, token: str, job_id: str) -> None:
        auth_policy = AuthPolicy(self._auth_client)
        user = await auth_policy.authorized_userid(token)
        if not user:
            raise AuthenticationError(f"Incorrect token: token={token}, job={job_id}")
        log.debug(f"user {user}")
        owner = await self._get_owner(token, job_id)
        permission = Permission(uri=f"job://{owner}/{job_id}", action="write")
        log.debug(f"Checking permission: {permission}")
        result = await auth_policy.permits(token, None, [permission])
        if not result:
            raise AuthorizationError(f"Permission denied: user={user}, job={job_id}")

    def _parse(self, request: str) -> ExecRequest:
        dict_request = json.loads(request)
        self._exec_request_validator.check(dict_request)
        return ExecRequest(**dict_request)

    async def process(self, json_request: str) -> int:
        try:
            request = self._parse(json_request)
            log.debug(f"Request: {request}")
        except ValueError as e:
            raise IllegalArgumentError(f"Illegal Payload: {json_request} ({e})")

        await self._authorize(request.token, request.job)
        return await self._executor.exec_in_job(request.job, request.command)
