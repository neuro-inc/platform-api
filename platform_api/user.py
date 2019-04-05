from dataclasses import dataclass, field
from typing import Optional

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY
from yarl import URL

from platform_api.orchestrator.job import (
    DEFAULT_QUOTA_NO_RESTRICTIONS,
    AggregatedRunTime,
)


@dataclass(frozen=True)
class User:
    name: str
    token: str = field(repr=False)
    quota: AggregatedRunTime = field(default=DEFAULT_QUOTA_NO_RESTRICTIONS)

    def has_quota(self) -> bool:
        return self.quota != DEFAULT_QUOTA_NO_RESTRICTIONS

    def to_job_uri(self) -> URL:
        return URL(f"job://{self.name}")


async def untrusted_user(request: Request) -> User:
    """Return a non-authorized `User` object based on the token in the request.

    The primary use case is to not perform an extra HTTP request just to
    retrieve the minimal information about the user.
    NOTE: non-authorization fields like `quota` will be not initialized!
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    name = autz_policy.get_user_name_from_identity(identity)
    if name is None:
        raise HTTPUnauthorized()

    return User(name=name, token=identity)  # type: ignore


async def authorized_user(request: Request) -> User:
    """Request auth-server for authenticated information on the user and
     return the `User` object with all necessary information
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    autz_user = await autz_policy.authorized_user(identity)
    if autz_user is None:
        raise HTTPUnauthorized()
    quota = AggregatedRunTime.from_quota(autz_user.quota)

    return User(name=autz_user.name, token=identity, quota=quota)


async def _get_identity(request: Request) -> str:
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()
    return identity
