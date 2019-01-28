import json
import logging
from dataclasses import dataclass
from typing import List

import aiohttp
import trafaret as t
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthPolicy
from yarl import URL
import abc
from .executor import Executor
from .forwarder import Forwarder

log = logging.getLogger(__name__)


def create_exec_request_validator() -> t.Trafaret:
    return t.Dict({"token": t.String, "job": t.String, "command": t.List(t.String)})


def create_port_forward_request_validator() -> t.Trafaret:
    return t.Dict({"token": t.String, "job": t.String})


def create_ssh_request_validator() -> t.Trafaret:
    return t.Or(create_exec_request_validator(),
                create_port_forward_request_validator())


@dataclass(frozen=True)
class SSHRequest(abc.ABC):
    token: str
    job: str

    @abc.abstractmethod
    async def process(self, proxy: 'ExecProxy') -> int:
        pass


@dataclass(frozen=True)
class PortForwardRequest(SSHRequest):
    async def process(self, proxy: 'ExecProxy') -> int:
        return await proxy.process_port_forward_request(self)


@dataclass(frozen=True)
class ExecRequest(SSHRequest):
    command: List[str]

    async def process(self, proxy: 'ExecProxy') -> int:
        return await proxy.process_exec_request(self)


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
            self, auth_client: AuthClient, platform_url: URL,
            executor: Executor, forwarder: Forwarder
    ) -> None:
        self._auth_client = auth_client
        self._jobs_url = platform_url / "jobs"
        self._ssh_request_validator = create_ssh_request_validator()
        self._executor = executor
        self._forwarder = forwarder

    async def _get_owner(self, token: str, job_id: str) -> None:
        async with aiohttp.ClientSession() as session:
            job_url = self._jobs_url / job_id
            headers = {"Authorization": f"Bearer {token}"}
            response = await session.get(job_url, headers=headers)
            if response.status != 200:
                raise AuthorizationError(f"Response status: {response.status}")
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

    def _parse(self, request: str) -> SSHRequest:
        dict_request = json.loads(request)
        self._ssh_request_validator(dict_request)
        if "command" in dict_request:
            return ExecRequest(**dict_request)
        else:
            return PortForwardRequest(**dict_request)

    async def process_exec_request(self, request: ExecRequest) -> int:
        return await self._executor.exec_in_job(request.job, request.command)

    async def process_port_forward_request(self, request: PortForwardRequest) -> int:
        return await self._forwarder.forward()

    async def process(self, json_request: str) -> int:
        try:
            request = self._parse(json_request)
            log.debug(f"Request: {request}")
        except ValueError as e:
            raise IllegalArgumentError(f"Illegal Payload: {json_request} ({e})")

        await self._authorize(request.token, request.job)
        return await request.process(self)
