from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from pathlib import PurePath
from typing import Optional, Sequence

from yarl import URL

from .resource import ResourcePoolType


class StorageType(str, Enum):
    HOST = "host"
    NFS = "nfs"


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
    is_secure: bool = True

    @property
    def url(self) -> URL:
        scheme = "https" if self.is_secure else "http"
        return URL(f"{scheme}://{self.host}")


@dataclass(frozen=True)
class OrchestratorConfig:
    storage: StorageConfig
    registry: RegistryConfig

    jobs_domain_name_template: str
    named_jobs_domain_name_template: str

    ssh_domain_name: str
    ssh_auth_domain_name: str

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
class LoggingConfig:
    elasticsearch: ElasticsearchConfig


@dataclass(frozen=True)
class IngressConfig:
    storage_url: URL
    users_url: URL
    monitoring_url: URL


@dataclass(frozen=True)
class ClusterConfig:
    name: str

    orchestrator: OrchestratorConfig
    logging: LoggingConfig
    ingress: IngressConfig

    @property
    def storage(self) -> StorageConfig:
        return self.orchestrator.storage

    @property
    def registry(self) -> RegistryConfig:
        return self.orchestrator.registry
