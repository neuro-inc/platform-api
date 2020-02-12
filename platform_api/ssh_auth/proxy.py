import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

import aiohttp
import trafaret as t
from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthPolicy
from yarl import URL

from .executor import Executor
from .forwarder import Forwarder


log = logging.getLogger(__name__)


@dataclass(frozen=True)  # type: ignore
class JobRequest(ABC):
    token: str
    job: str

    @property
    @abstractmethod
    def action(self) -> str:
        pass


@dataclass(frozen=True)
class ExecRequest(JobRequest):
    command: List[str]
    action: str = "write"


@dataclass(frozen=True)
class PortForwardRequest(JobRequest):
    port: int
    action: str = "write"


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
            "params": t.Dict({"job": t.String, "port": t.Int}),
        }
    ) >> (
        lambda d: PortForwardRequest(
            token=d["token"], job=d["params"]["job"], port=d["params"]["port"]
        )
    )


def create_request_validator() -> t.Trafaret:
    return t.Or(
        create_exec_request_validator(), create_port_forward_request_validator()
    )


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
        self,
        auth_client: AuthClient,
        platform_url: URL,
        executor: Executor,
        forwarder: Forwarder,
        use_cluster_names_in_uris: bool,
    ) -> None:
        self._auth_client = auth_client
        self._jobs_url = platform_url / "jobs"
        self._ssh_request_validator = create_request_validator()
        self._executor = executor
        self._forwarder = forwarder
        self._use_cluster_names_in_uris = use_cluster_names_in_uris

    async def _get_job_uri(self, token: str, job_id: str) -> str:
        async with aiohttp.ClientSession() as session:
            job_url = self._jobs_url / job_id
            headers = {"Authorization": f"Bearer {token}"}
            response = await session.get(job_url, headers=headers)
            if response.status != 200:
                raise AuthorizationError(f"Response status: {response.status}")
            job_payload = await response.json()
            return job_payload["uri"]

    async def authorize_job(self, token: str, job_id: str, action: str) -> None:
        auth_policy = AuthPolicy(self._auth_client)
        user = await auth_policy.authorized_userid(token)
        if not user:
            raise AuthenticationError(f"Incorrect token: token={token}, job={job_id}")
        log.debug(f"user {user}")
        job_uri = await self._get_job_uri(token, job_id)
        permission = Permission(uri=job_uri, action=action)
        log.debug(f"Checking permission: {permission}")
        result = await auth_policy.permits(token, None, [permission])
        if not result:
            raise AuthorizationError(f"Permission denied: user={user}, job={job_id}")

    def _parse(self, request: str) -> JobRequest:
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
        if isinstance(request, JobRequest):
            await self.authorize_job(request.token, request.job, request.action)

        if isinstance(request, ExecRequest):
            return await self.process_exec_request(request)
        elif isinstance(request, PortForwardRequest):
            return await self.process_port_forward_request(request)
        else:
            raise IllegalArgumentError(f"Unknown request type")
