import logging
from typing import Optional

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY
from neuro_auth_client import User as AuthUser
from yarl import URL

logger = logging.getLogger(__name__)


def make_job_uri(user: AuthUser, cluster_name: str, org_name: Optional[str]) -> URL:
    if org_name is None:
        return URL.build(scheme="job", host=cluster_name) / user.name
    return URL.build(scheme="job", host=cluster_name) / org_name / user.name


async def untrusted_user(request: Request) -> AuthUser:
    """Return a non-authorized `User` object based on the token in the request.

    The primary use case is to not perform an extra HTTP request just to
    retrieve the minimal information about the user.
    NOTE: non-authorization data about clusters will be not initialized!
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    name = autz_policy.get_user_name_from_identity(identity)
    if name is None:
        raise HTTPUnauthorized()

    return AuthUser(name=name)


async def authorized_user(request: Request) -> AuthUser:
    """Request auth-server for authenticated information on the user and
    return the `User` object with all necessary information
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    autz_user = await autz_policy.authorized_user(identity)
    if autz_user is None:
        raise HTTPUnauthorized()
    return autz_user


async def _get_identity(request: Request) -> str:
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()
    return identity
