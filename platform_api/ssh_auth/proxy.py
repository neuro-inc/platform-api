import json
import logging
from abc import ABC, abstractmethod
from typing import List

import aiohttp
import trafaret as t
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthPolicy
from yarl import URL

from .executor import Executor
from .forwarder import Forwarder
from operator import attrgetter

log = logging.getLogger(__name__)


class Request(ABC):
    @abstractmethod
    async def process(self, proxy: 'ExecProxy'):
        pass

    @abstractmethod
    async def authorize(self, proxy: 'ExecProxy'):
        pass


class JobRequest(Request):
    _token: str
    _job: str
    _action: str

    def __init__(self, token, job, action):
        self._token = token
        self._job = job
        self._action = action

    async def authorize(self, proxy: 'ExecProxy'):
        await proxy.authorize(self.token, self.job, self.action)

    token = property(attrgetter('_token'))
    job = property(attrgetter('_job'))
    action = property(attrgetter('_action'))


class ExecRequest(JobRequest):
    _command: List[str]

    def __init__(self, token, job, command):
        super().__init__(token, job, "write")
        self._command = command

    async def process(self, proxy: 'ExecProxy') -> int:
        return await proxy.process_exec_request(self)

    command = property(attrgetter('_command'))


class PortForwardRequest(JobRequest):
    _port: int

    def __init__(self, token, job, port):
        super().__init__(token, job, "read")
        self._port = port

    async def process(self, proxy: 'ExecProxy') -> int:
        return await proxy.process_port_forward_request(self)

    port = property(attrgetter('_port'))


def create_exec_request_validator() -> t.Trafaret:
    return t.Dict(
        {
            "method": t.Atom("job_exec"),
            "token": t.String,
            "params": t.Dict({"job": t.String, "command": t.List(t.String)}),
        }
    ) >> (
        lambda d: ExecRequest(
            token=d["token"], job=d["params"]["job"], command=d["params"]["command"]
        )
    )


def create_port_forward_request_validator() -> t.Trafaret:
    return t.Dict(
        {
            "method": t.Atom("job_port_forward"),
            "token": t.String,
            "params": t.Dict({"job": t.String, "port": t.Int})
        }
    ) >> (
        lambda d: PortForwardRequest(
            token=d["token"], job=d["params"]["job"], port=d["params"]["port"]
        )
    )


def create_request_validator() -> t.Trafaret:
    return t.Or(create_exec_request_validator(),
                create_port_forward_request_validator())


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
            executor: Executor, forwarder: Forwarder,
            log_file: str
    ) -> None:
        self._auth_client = auth_client
        self._jobs_url = platform_url / "jobs"
        self._ssh_request_validator = create_request_validator()
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

    async def authorize(self, token: str, job_id: str, action: str) -> None:
        auth_policy = AuthPolicy(self._auth_client)
        user = await auth_policy.authorized_userid(token)
        if not user:
            raise AuthenticationError(f"Incorrect token: token={token}, job={job_id}")
        log.debug(f"user {user}")
        owner = await self._get_owner(token, job_id)
        permission = Permission(uri=f"job://{owner}/{job_id}", action=action)
        log.debug(f"Checking permission: {permission}")
        result = await auth_policy.permits(token, None, [permission])
        if not result:
            raise AuthorizationError(f"Permission denied: user={user}, job={job_id}")

    def _parse(self, request: str) -> Request:
        dict_request = json.loads(request)
        return self._ssh_request_validator(dict_request)

    async def process_exec_request(self, request: ExecRequest) -> int:
        return await self._executor.exec_in_job(request.job, request.command)

    async def process_port_forward_request(self, request: PortForwardRequest) -> int:
        return await self._forwarder.forward(request.job, request.port)

    async def process(self, json_request: str) -> int:
        try:
            request = self._parse(json_request)
            log.debug(f"Request: {request}")
        except ValueError as e:
            raise IllegalArgumentError(f"Illegal Payload: {json_request} ({e})")

        await request.authorize(self)
        return await request.process(self)
