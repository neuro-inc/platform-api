from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from pathlib import PurePath
from typing import Container, Optional, Sequence

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

    is_http_ingress_secure: bool = False

    @property
    def job_deletion_delay(self) -> timedelta:
        return timedelta(seconds=self.job_deletion_delay_s)


@dataclass(frozen=True)
class ElasticsearchConfig:
    hosts: Sequence[str]


@dataclass(frozen=True)
class DatabaseConfig:
    redis: Optional[RedisConfig] = None


@dataclass(frozen=True)
class LoggingConfig:
    elasticsearch: ElasticsearchConfig


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    orchestrator: OrchestratorConfig
    database: DatabaseConfig
    auth: AuthConfig
    logging: LoggingConfig

    registry: RegistryConfig = RegistryConfig()

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = "NP"  # stands for Neuromation Platform


@dataclass(frozen=True)
class SSHServerConfig:
    host: str = "0.0.0.0"
    port: int = 8022  # use nonprivileged port for dev mode
    ssh_host_keys: Container[str] = ()


@dataclass(frozen=True)
class SSHConfig:
    server: SSHServerConfig
    storage: StorageConfig
    orchestrator: OrchestratorConfig
    database: DatabaseConfig
    auth: AuthConfig
    registry: RegistryConfig = RegistryConfig()

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = "NP"  # stands for Neuromation Platform


@dataclass(frozen=True)
class PlatformConfig:
    server_endpoint_url: URL


@dataclass(frozen=True)
class SSHAuthConfig:
    platform: PlatformConfig
    auth: AuthConfig
    env_prefix: str = "NP"
