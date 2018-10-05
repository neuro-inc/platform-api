from dataclasses import dataclass

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY
from yarl import URL


@dataclass(frozen=True)
class User:
    name: str
    token: str

    def to_job_uri(self) -> URL:
        return URL(f"job://{self.name}")


async def untrusted_user(request: Request) -> User:
    """Return a `User` object based on the token in the request.

    The primary use case is to not perform an extra HTTP request just to
    retrieve the minimal information about the user.
    """
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    autz_policy = request.config_dict.get(AUTZ_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()

    # TODO (A Danshyn 10/04/18): unfortunately we have to use the private
    # interface here until the corresponding method is exposed :(
    name = autz_policy._get_user_name_from_identity(identity)
    if name is None:
        raise HTTPUnauthorized()

    return User(name=name, token=identity)  # type: ignore
