from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import Optional, Sequence

from yarl import URL

from .elasticsearch import ElasticsearchConfig
from .resource import ResourcePoolType


class StorageType(str, Enum):
    HOST = "host"
    NFS = "nfs"


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: Optional[PurePath] = None
    container_mount_path: PurePath = PurePath("/var/storage")

    type: StorageType = StorageType.HOST

    nfs_server: Optional[str] = None
    nfs_export_path: Optional[PurePath] = None

    uri_scheme: str = "storage"

    def __post_init__(self) -> None:
        self._check_nfs_attrs()

    def _check_nfs_attrs(self) -> None:
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
    def create_nfs(
        cls,
        *,
        container_mount_path: PurePath = container_mount_path,
        host_mount_path: Optional[PurePath] = host_mount_path,
        nfs_server: str,
        nfs_export_path: PurePath,
    ) -> "StorageConfig":
        return cls(
            host_mount_path,
            container_mount_path,
            StorageType.NFS,
            nfs_server=nfs_server,
            nfs_export_path=nfs_export_path,
        )

    @classmethod
    def create_host(
        cls,
        *,
        container_mount_path: PurePath = container_mount_path,
        host_mount_path: PurePath,
    ) -> "StorageConfig":
        return cls(host_mount_path, container_mount_path, StorageType.HOST)


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
    jobs_domain_name_template: str
    named_jobs_domain_name_template: str

    ssh_domain_name: str
    ssh_auth_domain_name: str

    resource_pool_types: Sequence[ResourcePoolType]

    is_http_ingress_secure: bool = False


class KubeClientAuthType(str, Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig(OrchestratorConfig):
    jobs_ingress_name: str = ""
    jobs_ingress_auth_name: str = ""

    endpoint_url: str = ""
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token_path: Optional[str] = None

    namespace: str = "default"

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    storage_volume_name: str = "storage"

    node_label_gpu: Optional[str] = None
    node_label_preemptible: Optional[str] = None

    def __post_init__(self) -> None:
        if not all((self.jobs_ingress_name, self.endpoint_url)):
            raise ValueError("Missing required settings")


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
    storage: StorageConfig
    registry: RegistryConfig
    orchestrator: OrchestratorConfig
    logging: LoggingConfig
    ingress: IngressConfig
