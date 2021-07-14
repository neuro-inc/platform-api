from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from typing import Optional, Sequence

from alembic.config import Config as AlembicConfig
from yarl import URL

from .cluster_config import RegistryConfig, StorageConfig
from .orchestrator.kube_config import KubeConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    app_name: str
    sample_rate: float = 0


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    public_endpoint_url: URL
    service_token: str = field(repr=False)
    service_name: str = "compute"


@dataclass(frozen=True)
class OAuthConfig:
    client_id: str = field(repr=False)
    audience: str = field(repr=False)
    headless_callback_url: URL
    auth_url: URL
    token_url: URL
    logout_url: URL

    callback_urls: Sequence[URL] = (
        URL("http://127.0.0.1:54540"),
        URL("http://127.0.0.1:54541"),
        URL("http://127.0.0.1:54542"),
    )

    success_redirect_url: Optional[URL] = None


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
    postgres: PostgresConfig


@dataclass(frozen=True)
class JobsConfig:
    deletion_delay_s: int = 0
    image_pull_error_delay_s: int = 0
    orphaned_job_owner: str = ""

    @property
    def deletion_delay(self) -> timedelta:
        return timedelta(seconds=self.deletion_delay_s)

    @property
    def image_pull_error_delay(self) -> timedelta:
        return timedelta(seconds=self.image_pull_error_delay_s)


@dataclass(frozen=True)
class NotificationsConfig:
    url: URL
    token: str


@dataclass(frozen=True)
class JobPolicyEnforcerConfig:
    platform_api_url: URL
    token: str
    interval_sec: float = 60
    credit_notification_threshold: Decimal = Decimal("10")
    retention_delay_days: float = 180  # Half of a year

    @property
    def retention_delay(self) -> timedelta:
        return timedelta(days=self.retention_delay_days)


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
class SentryConfig:
    dsn: URL
    cluster_name: str
    app_name: str
    sample_rate: float = 0


@dataclass(frozen=True)
class Config:
    server: ServerConfig

    database: DatabaseConfig
    auth: AuthConfig
    notifications: NotificationsConfig
    job_policy_enforcer: JobPolicyEnforcerConfig

    api_base_url: URL
    config_url: URL
    admin_url: URL

    oauth: Optional[OAuthConfig] = None

    jobs: JobsConfig = JobsConfig()
    cors: CORSConfig = CORSConfig()

    scheduler: JobsSchedulerConfig = JobsSchedulerConfig()

    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None


@dataclass(frozen=True)
class PollerConfig:
    cluster_name: str
    platform_api_url: URL
    server: ServerConfig

    auth: AuthConfig

    config_url: URL

    registry_config: RegistryConfig
    storage_config: StorageConfig
    kube_config: KubeConfig

    jobs: JobsConfig = JobsConfig()

    scheduler: JobsSchedulerConfig = JobsSchedulerConfig()

    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None


@dataclass(frozen=True)
class PlatformConfig:
    server_endpoint_url: URL
