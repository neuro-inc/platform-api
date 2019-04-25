from dataclasses import dataclass, field
from pathlib import Path
from typing import Container, Optional, Sequence

from yarl import URL

from .cluster_config import (
    ClusterConfig,
    IngressConfig,
    LoggingConfig,
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from .redis import RedisConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    service_token: str = field(repr=False)
    service_name: str = "compute"


@dataclass(frozen=True)
class OAuthConfig:
    base_url: URL
    client_id: str = field(repr=False)
    audience: str = field(repr=False)

    callback_urls: Sequence[URL] = (
        URL("http://127.0.0.1:54540"),
        URL("http://127.0.0.1:54541"),
        URL("http://127.0.0.1:54542"),
    )

    success_redirect_url: Optional[URL] = None

    @property
    def auth_url(self) -> URL:
        return self.base_url / "authorize"

    @property
    def token_url(self) -> URL:
        return self.base_url / "oauth/token"


@dataclass(frozen=True)
class DatabaseConfig:
    redis: Optional[RedisConfig] = None


@dataclass(frozen=True)
class Config:
    server: ServerConfig

    cluster: ClusterConfig

    database: DatabaseConfig
    auth: AuthConfig
    oauth: Optional[OAuthConfig] = None

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = "NP"  # stands for Neuromation Platform

    @property
    def storage(self) -> StorageConfig:
        return self.cluster.storage

    @property
    def registry(self) -> RegistryConfig:
        return self.cluster.registry

    @property
    def orchestrator(self) -> OrchestratorConfig:
        return self.cluster.orchestrator

    @property
    def logging(self) -> LoggingConfig:
        return self.cluster.logging

    @property
    def ingress(self) -> IngressConfig:
        return self.cluster.ingress


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
    log_fifo: Path
    env_prefix: str = "NP"
