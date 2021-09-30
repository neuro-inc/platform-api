from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePath
from typing import Optional, Sequence

from yarl import URL

from .resource import DEFAULT_PRESETS, Preset, ResourcePoolType, TPUResource


STORAGE_URI_SCHEME = "storage"


class StorageType(str, Enum):
    HOST = "host"
    NFS = "nfs"
    PVC = "pvc"


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: PurePath

    type: StorageType = StorageType.HOST

    nfs_server: Optional[str] = None
    nfs_export_path: Optional[PurePath] = None

    pvc_name: Optional[str] = None

    path: Optional[PurePath] = None

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

    @property
    def is_pvc(self) -> bool:
        return self.type == StorageType.PVC

    @classmethod
    def create_nfs(
        cls,
        *,
        path: Optional[PurePath] = None,
        nfs_server: str,
        nfs_export_path: PurePath,
    ) -> "StorageConfig":
        return cls(
            path=path,
            host_mount_path=nfs_export_path,
            type=StorageType.NFS,
            nfs_server=nfs_server,
            nfs_export_path=nfs_export_path,
        )

    @classmethod
    def create_pvc(
        cls,
        *,
        path: Optional[PurePath] = None,
        pvc_name: str,
    ) -> "StorageConfig":
        return cls(
            path=path,
            type=StorageType.PVC,
            pvc_name=pvc_name,
            # NOTE: `host_mount_path`'s value here does not mean anything
            # really. It is simply used to infer relative paths later.
            host_mount_path=PurePath("/mnt/storage"),
        )

    @classmethod
    def create_host(
        cls,
        *,
        path: Optional[PurePath] = None,
        host_mount_path: PurePath,
    ) -> "StorageConfig":
        return cls(path=path, host_mount_path=host_mount_path, type=StorageType.HOST)


@dataclass(frozen=True)
class RegistryConfig:
    username: str
    password: str = field(repr=False)
    url: URL = URL("https://registry.dev.neuromation.io")
    email: str = "registry@neuromation.io"

    def __post_init__(self) -> None:
        if not self.url.host:
            raise ValueError("Invalid registry config: missing url hostname")

    @property
    def host(self) -> str:
        return self.ger_registry_host(self.url)

    @classmethod
    def ger_registry_host(self, url: URL) -> str:
        """Returns registry hostname with port (if specified)"""
        port = url.explicit_port  # type: ignore
        suffix = f":{port}" if port is not None else ""
        return f"{url.host}{suffix}"


@dataclass(frozen=True)
class OrchestratorConfig:
    jobs_domain_name_template: str
    jobs_internal_domain_name_template: str

    resource_pool_types: Sequence[ResourcePoolType]
    presets: Sequence[Preset] = DEFAULT_PRESETS

    is_http_ingress_secure: bool = False

    job_schedule_timeout: float = 3 * 60
    job_schedule_scaleup_timeout: float = 15 * 60

    allow_privileged_mode: bool = False

    @property
    def tpu_resources(self) -> Sequence[TPUResource]:
        return tuple(
            resource.tpu for resource in self.resource_pool_types if resource.tpu
        )

    @property
    def tpu_ipv4_cidr_block(self) -> Optional[str]:
        tpus = self.tpu_resources
        if not tpus:
            return None
        return tpus[0].ipv4_cidr_block


@dataclass(frozen=True)
class IngressConfig:
    registry_url: URL
    storage_url: URL
    blob_storage_url: URL
    monitoring_url: URL
    secrets_url: URL
    metrics_url: URL
    disks_url: URL
    buckets_url: URL

    @property
    def registry_host(self) -> str:
        return RegistryConfig.ger_registry_host(self.registry_url)


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    orchestrator: OrchestratorConfig
    ingress: IngressConfig
