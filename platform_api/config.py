from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional, Sequence

from alembic.config import Config as AlembicConfig
from yarl import URL

from .redis import RedisConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    sample_rate: float


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

    @property
    def logout_url(self) -> URL:
        return self.base_url / "v2/logout"


@dataclass(frozen=True)
class PostgresConfig:
    postgres_dsn: str

    alembic: AlembicConfig

    # based on defaults
    # https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.connect
    pool_min_size: int = 10
    pool_max_size: int = 10

    connect_timeout_s: float = 60.0
    command_timeout_s: Optional[float] = 60.0


@dataclass(frozen=True)
class DatabaseConfig:
    postgres_enabled: bool = False
    redis: Optional[RedisConfig] = None
    postgres: Optional[PostgresConfig] = None


@dataclass(frozen=True)
class JobsConfig:
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
class JobsSchedulerConfig:
    # Minimal time that preepmtible job is guaranteed to run before suspended
    run_quantum_sec: float = 1 * 60 * 60  # 1h
    # Time after which scheduler will try to start oldest SUSPENDED task
    max_suspended_time_sec: float = 2 * 60 * 60  # 2h
    # Time after which materialized job not running job considered as waiting
    # for resources
    is_waiting_min_time_sec: float = 5 * 60  # 5m

    @property
    def run_quantum(self) -> timedelta:
        return timedelta(seconds=self.run_quantum_sec)

    @property
    def max_suspended_time(self) -> timedelta:
        return timedelta(seconds=self.max_suspended_time_sec)

    @property
    def is_waiting_min_time(self) -> timedelta:
        return timedelta(seconds=self.is_waiting_min_time_sec)


@dataclass(frozen=True)
class Config:
    server: ServerConfig

    database: DatabaseConfig
    auth: AuthConfig
    zipkin: ZipkinConfig
    notifications: NotificationsConfig
    job_policy_enforcer: JobPolicyEnforcerConfig

    api_base_url: URL
    config_url: URL
    admin_url: URL

    oauth: Optional[OAuthConfig] = None

    jobs: JobsConfig = JobsConfig()
    cors: CORSConfig = CORSConfig()

    scheduler: JobsSchedulerConfig = JobsSchedulerConfig()

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
    jobs_namespace: str = "default"
