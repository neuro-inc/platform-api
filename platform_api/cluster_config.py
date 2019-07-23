from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import Dict, Optional, Sequence

from yarl import URL

from platform_api.resource import GKEGPUModels

from .elasticsearch import ElasticsearchConfig
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

    def __post_init__(self) -> None:
        self._check_nfs_attrs()

    def _check_nfs_attrs(self) -> None:
        nfs_attrs = (self.nfs_server, self.nfs_export_path)
        if self.is_nfs:
            if not all(nfs_attrs):
                raise ValueError("Missing NFS settings")
            if self.host_mount_path != self.nfs_export_path:
                # NOTE (ayushkovskiy 14-May-2019) this is a TEMPORARY PATCH: even for
                # StorageType.NFS, `host_mount_path` must be non-null as it is used
                # in `ContainerVolumeFactory.__init__`, who assumes that it's not None
                raise ValueError("Invalid host mount path")
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
        nfs_server: str,
        nfs_export_path: PurePath,
    ) -> "StorageConfig":
        return cls(
            host_mount_path=nfs_export_path,
            container_mount_path=container_mount_path,
            type=StorageType.NFS,
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
        return cls(
            host_mount_path=host_mount_path,
            container_mount_path=container_mount_path,
            type=StorageType.HOST,
        )


@dataclass(frozen=True)
class RegistryConfig:
    url: URL = URL("https://registry.dev.neuromation.io")
    email: str = "registry@neuromation.io"

    def __post_init__(self) -> None:
        if not self.url.host:
            raise ValueError("Invalid registry config: missing url hostname")

    @property
    def host(self) -> str:
        """Returns registry hostname with port (if specified)
        """
        port = self.url.explicit_port  # type: ignore
        suffix = f":{port}" if port is not None else ""
        return f"{self.url.host}{suffix}"


@dataclass(frozen=True)
class OrchestratorConfig:
    jobs_domain_name_template: str

    ssh_auth_domain_name: str

    resource_pool_types: Sequence[ResourcePoolType]

    is_http_ingress_secure: bool = False

    job_schedule_timeout: float = 3 * 60
    job_schedule_scaleup_timeout: float = 15 * 60


@dataclass(frozen=True)
class LoggingConfig:
    elasticsearch: ElasticsearchConfig


@dataclass(frozen=True)
class IngressConfig:
    storage_url: URL
    users_url: URL
    monitoring_url: URL


@dataclass(frozen=True)
class Preset:
    cpu: Optional[float] = None
    gpu: Optional[int] = None
    memory_mb: Optional[int] = None
    gpu_model: Optional[str] = None


DEFAULT_PRESETS = {
    "gpu-small": Preset(
        cpu=7, memory_mb=30 * 1024, gpu=1, gpu_model=next(iter(GKEGPUModels)).value.id
    ),
    "gpu-large": Preset(
        cpu=7,
        memory_mb=60 * 1024,
        gpu=1,
        gpu_model=next(reversed(GKEGPUModels)).value.id,
    ),
    "cpu-small": Preset(cpu=2, memory_mb=2 * 1024),
    "cpu-large": Preset(cpu=3, memory_mb=14 * 1024),
}


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    storage: StorageConfig
    registry: RegistryConfig
    orchestrator: OrchestratorConfig
    logging: LoggingConfig
    ingress: IngressConfig
    presets: Dict[str, Preset]
