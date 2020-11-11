import logging
from dataclasses import dataclass, field
from typing import List, Optional

from aiohttp.web import HTTPUnauthorized, Request
from aiohttp_security.api import AUTZ_KEY, IDENTITY_KEY
from neuro_auth_client import Cluster as AuthCluster, User as AuthUser
from yarl import URL

from platform_api.orchestrator.job import (
    DEFAULT_QUOTA_NO_RESTRICTIONS,
    AggregatedRunTime,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UserCluster:
    name: str
    runtime_quota: AggregatedRunTime = field(default=DEFAULT_QUOTA_NO_RESTRICTIONS)
    jobs_quota: Optional[int] = None

    def has_quota(self) -> bool:
        return self.runtime_quota != DEFAULT_QUOTA_NO_RESTRICTIONS

    @classmethod
    def create_from_auth_cluster(cls, cluster: AuthCluster) -> "UserCluster":
        return cls(
            name=cluster.name,
            jobs_quota=cluster.quota.total_running_jobs,
            runtime_quota=AggregatedRunTime.from_quota(cluster.quota),
        )


@dataclass(frozen=True)
class User:
    name: str
    token: str = field(repr=False)

    # NOTE: left this for backward compatibility with existing tests
    quota: AggregatedRunTime = field(default=DEFAULT_QUOTA_NO_RESTRICTIONS)
    cluster_name: str = ""

    clusters: List[UserCluster] = field(default_factory=list)

    # NOTE: left this for backward compatibility with existing tests
    def __post_init__(self) -> None:
        if self.clusters:
            object.__setattr__(self, "cluster_name", self.clusters[0].name)
            object.__setattr__(self, "quota", self.clusters[0].runtime_quota)
        else:
            self.clusters.append(
                UserCluster(name=self.cluster_name, runtime_quota=self.quota)
            )

    # NOTE: left this for backward compatibility with existing tests
    def has_quota(self) -> bool:
        return self.quota != DEFAULT_QUOTA_NO_RESTRICTIONS

    def get_cluster(self, name: str) -> Optional[UserCluster]:
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return None

    def to_job_uri(self, cluster_name: str) -> URL:
        assert cluster_name
        return URL(f"job://{cluster_name}/{self.name}")

    @classmethod
    def create_from_auth_user(cls, auth_user: AuthUser, *, token: str = "") -> "User":
        return cls(
            name=auth_user.name,
            token=token,
            clusters=[
                UserCluster.create_from_auth_cluster(c) for c in auth_user.clusters
            ],
        )


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

    return User(name=name, token=identity)


async def authorized_user(request: Request) -> User:
    """Request auth-server for authenticated information on the user and
    return the `User` object with all necessary information
    """
    identity = await _get_identity(request)

    autz_policy = request.config_dict.get(AUTZ_KEY)
    autz_user = await autz_policy.authorized_user(identity)
    if autz_user is None:
        raise HTTPUnauthorized()
    return User.create_from_auth_user(autz_user, token=identity)


async def _get_identity(request: Request) -> str:
    identity_policy = request.config_dict.get(IDENTITY_KEY)
    identity = await identity_policy.identify(request)
    if identity is None:
        raise HTTPUnauthorized()
    return identity
