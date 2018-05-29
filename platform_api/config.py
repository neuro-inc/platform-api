from dataclasses import dataclass
from pathlib import PurePath

from .orchestrator import KubeConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: PurePath
    container_mount_path: PurePath = PurePath('/var/storage')

    uri_scheme: str = 'storage'


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    orchestrator: KubeConfig

    env_prefix: str = 'NP'  # stands for Neuromation Platform
