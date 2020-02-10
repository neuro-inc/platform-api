from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional, Sequence

from yarl import URL

from .config_client import ConfigClient
from .redis import RedisConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    public_endpoint_url: URL
    service_token: str = field(repr=False)
    service_name: str = "compute"


@dataclass(frozen=True)
class OAuthConfig:
    base_url: URL
    client_id: str = field(repr=False)
    audience: str = field(repr=False)
    headless_callback_url: URL

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
class JobsConfig:
    default_cluster_name: str = "default"
    deletion_delay_s: int = 0
    orphaned_job_owner: str = ""
    jobs_ingress_class: str = "traefik"
    jobs_ingress_oauth_url: URL = URL("https://neu.ro/oauth/authorize")

    @property
    def deletion_delay(self) -> timedelta:
        return timedelta(seconds=self.deletion_delay_s)


@dataclass(frozen=True)
class NotificationsConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class JobPolicyEnforcerConfig:
    platform_api_url: URL
    token: str
    interval_sec: float = 60
    quota_notification_threshold: float = 0.9


@dataclass(frozen=True)
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class Config:
    config_client: ConfigClient

    server: ServerConfig

    database: DatabaseConfig
    auth: AuthConfig
    notifications: NotificationsConfig
    job_policy_enforcer: JobPolicyEnforcerConfig

    admin_url: URL

    oauth: Optional[OAuthConfig] = None

    jobs: JobsConfig = JobsConfig()
    cors: CORSConfig = CORSConfig()

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = "NP"  # stands for Neuromation Platform

    use_cluster_names_in_uris: bool = False


@dataclass(frozen=True)
class PlatformConfig:
    server_endpoint_url: URL


@dataclass(frozen=True)
class SSHAuthConfig:
    platform: PlatformConfig
    auth: AuthConfig
    log_fifo: Path
    env_prefix: str = "NP"
    jobs_namespace: str = "default"
    use_cluster_names_in_uris: bool = False
