import asyncio
import json
import logging
import subprocess
import trafaret as t
from dataclasses import dataclass
from typing import List

from neuro_auth_client import AuthClient, Permission
from neuro_auth_client.security import AuthPolicy

from platform_api.orchestrator import KubeOrchestrator
from platform_api.orchestrator.job_request import JobError
from platform_api.orchestrator.jobs_storage import RedisJobsStorage
from platform_api.redis import create_redis_client


log = logging.getLogger(__name__)


def create_exec_request_validator() -> t.Trafaret:
    return t.Dict(
        {
            "token": t.String,
            "job": t.String,
            "command": t.List(t.String),
        }
    )


@dataclass
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
    def __init__(self, config, tty):
        self._config = config
        self._tty = tty
        self._exec_request_validator = create_exec_request_validator()

    async def authorize(self, token, job_id):
        async with AuthClient(
            url=self._config.auth.server_endpoint_url,
            token=self._config.auth.service_token,
        ) as auth_client:
            auth_policy = AuthPolicy(auth_client)
            user = await auth_policy.authorized_userid(token)
            if not user:
                raise AuthenticationError(
                    f"Incorrect token: token={token}, job={job_id}"
                )

            log.debug(f"user {user}")
            async with KubeOrchestrator(
                config=self._config.orchestrator
            ) as orchestrator:
                async with create_redis_client(
                    self._config.database.redis
                ) as redis_client:
                    jobs_storage = RedisJobsStorage(
                        redis_client, orchestrator=orchestrator
                    )
                    try:
                        job = await jobs_storage.get_job(job_id)
                    except JobError as error:
                        raise AuthorizationError(f"{error}")
                    permission = Permission(uri=str(job.to_uri()), action="write")
                    log.debug(f"Checking permission: {permission}")
                    result = await auth_policy.permits(token, None, [permission])
                    if not result:
                        raise AuthorizationError(
                            f"Permission denied: user={user}, job={job_id}"
                        )

    def parse(self, request):
        dict_request = json.loads(request)
        self._exec_request_validator.check(dict_request)
        return ExecRequest(**dict_request)

    def exec_pod(self, job, command):
        log.debug((f"Executing {command} in {job}"))
        if self._tty:
            kubectl_cmd = ["kubectl", "exec", "-it", job, "--"] + command
        else:
            kubectl_cmd = ["kubectl", "exec", "-i", job, "--"] + command
        log.debug(f"Running kubectl with command: {kubectl_cmd}")
        retcode = subprocess.call(kubectl_cmd)

        exit(retcode)

    def process(self, json_request):
        try:
            request = self.parse(json_request)
            log.debug(f"Request: {request}")
        except ValueError as e:
            raise IllegalArgumentError(f"Illegal Payload: {json_request} ({e})")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.authorize(request.token, request.job))
        self.exec_pod(request.job, request.command)
