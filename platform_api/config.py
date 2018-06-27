from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import Optional


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080


class StorageType(str, Enum):
    HOST = 'host'
    NFS = 'nfs'


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: PurePath
    container_mount_path: PurePath = PurePath('/var/storage')

    type: StorageType = StorageType.HOST

    # TODO (A Danshyn 06/27/18): should be set if type is NFS
    nfs_server: Optional[str] = None
    nfs_export_path: Optional[PurePath] = None

    uri_scheme: str = 'storage'

    def __post_init__(self):
        self._check_nfs_attrs()

    def _check_nfs_attrs(self):
        nfs_attrs = (self.nfs_server, self.nfs_export_path)
        if self.is_nfs:
            if not all(nfs_attrs):
                raise ValueError('Missing NFS settings')
        else:
            if any(nfs_attrs):
                raise ValueError('Redundant NFS settings')

    @property
    def is_nfs(self) -> bool:
        return self.type == StorageType.NFS


@dataclass(frozen=True)
class OrchestratorConfig:
    storage: StorageConfig

    jobs_domain_name: str


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    orchestrator: OrchestratorConfig

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = 'NP'  # stands for Neuromation Platform
