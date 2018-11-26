from math import ceil

import aiohttp
from jose import jwt, constants
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from pathlib import PurePath
from typing import Optional, Sequence, Dict

from yarl import URL

from .redis import RedisConfig
from .resource import ResourcePoolType


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


class StorageType(str, Enum):
    HOST = "host"
    NFS = "nfs"


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    service_token: str = field(repr=False)
    service_name: str = "compute"


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: PurePath
    container_mount_path: PurePath = PurePath("/var/storage")

    type: StorageType = StorageType.HOST

    nfs_server: Optional[str] = None
    nfs_export_path: Optional[PurePath] = None

    uri_scheme: str = "storage"

    def __post_init__(self):
        self._check_nfs_attrs()

    def _check_nfs_attrs(self):
        nfs_attrs = (self.nfs_server, self.nfs_export_path)
        if self.is_nfs:
            if not all(nfs_attrs):
                raise ValueError("Missing NFS settings")
        else:
            if any(nfs_attrs):
                raise ValueError("Redundant NFS settings")

    @property
    def is_nfs(self) -> bool:
        return self.type == StorageType.NFS

    @classmethod
    def create_nfs(cls, *args, **kwargs) -> "StorageConfig":
        return cls(*args, type=StorageType.NFS, **kwargs)  # type: ignore

    @classmethod
    def create_host(cls, *args, **kwargs) -> "StorageConfig":
        return cls(*args, **kwargs)  # type: ignore


@dataclass()
class LogStorageConfig:
    env_vars: Dict[str, str]


@dataclass(frozen=True)
class GCPTokenStoreConfig:
    # extracted private key part from service key file
    private_key: str
    # Service email address, to be provided from the conf
    iss: str
    # Expiration time of the short living token, in seconds, to be provided from conf
    token_expiration: int = 600
    # What URL would get our JWT token, and provide with short living service token
    general_auth_url: URL = URL('https://www.googleapis.com/oauth2/v4/token')


class GCPTokenStore:
    def __init__(
            self, gcp_store_config: GCPTokenStoreConfig) -> None:

        self._token_generation_time = -1
        self._token_generated = None

        self._client = None
        self._gcp_store_config = gcp_store_config

        self._client = aiohttp.ClientSession()

    async def close(self) -> None:
        if self._client and not self._client.closed:
            await self._client.close()

    @classmethod
    def __create_signed_jwt_key(cls, key: str, iss: str, auth_url: str, issue_time: int, exp_time: int) -> str:
        # TODO expiration time
        return jwt.encode(
            headers={"alg": "RS256", "typ": "JWT"},
            claims={"iss": iss,
                    "scope": "https://www.googleapis.com/auth/logging.read",
                    "aud": auth_url,
                    "iat": issue_time,
                    "exp": exp_time,
                    },
            key=key,
            algorithm=constants.ALGORITHMS.RS256
        )

    async def _request(self, url: URL, current_time: int) -> Optional[str]:
        issue_time = current_time
        exp_time = current_time + self._gcp_store_config.token_expiration
        self._signed_jwt = GCPTokenStore.__create_signed_jwt_key(
            self._gcp_store_config.private_key, self._gcp_store_config.iss,
            str(self._gcp_store_config.general_auth_url), issue_time, exp_time)
        async with self._client.post(url, data=self._signed_jwt) as response:
            # TODO: check the status code
            # TODO: raise exceptions
            if 400 <= response.status:
                # TODO log here into the error log!
                return None
            payload = await response.json()
            return payload['access_token']

    async def get_token(self) -> str:
        import time
        now = time.time()
        token_age = now - self._token_generation_time
        if (not self._token_generated) \
                or (token_age > self._gcp_store_config.token_expiration):
            # token would expire a bit earlier
            token_value = await self._request(
                self._gcp_store_config.general_auth_url, ceil(now))
            if token_value:
                self._token_generation_time = now
                self._token_generated = token_value
        return self._token_generated


@dataclass(frozen=True)
class RegistryConfig:
    host: str = "registry.dev.neuromation.io"
    email: str = "registry@neuromation.io"


@dataclass(frozen=True)
class OrchestratorConfig:
    storage: StorageConfig
    registry: RegistryConfig

    jobs_domain_name: str
    ssh_domain_name: str

    resource_pool_types: Sequence[ResourcePoolType]

    job_deletion_delay_s: int = 0

    orphaned_job_owner: str = ""

    @property
    def job_deletion_delay(self) -> timedelta:
        return timedelta(seconds=self.job_deletion_delay_s)


@dataclass(frozen=True)
class DatabaseConfig:
    redis: Optional[RedisConfig] = None


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    orchestrator: OrchestratorConfig
    database: DatabaseConfig
    auth: AuthConfig

    registry: RegistryConfig = RegistryConfig()

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = "NP"  # stands for Neuromation Platform
