from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal

from alembic.config import Config as AlembicConfig
from apolo_events_client import EventsClientConfig
from yarl import URL

from .orchestrator.kube_config import KubeConfig

STORAGE_URI_SCHEME = "storage"


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
    def ger_registry_host(cls, url: URL) -> str:
        """Returns registry hostname with port (if specified)"""
        port = url.explicit_port
        suffix = f":{port}" if port is not None else ""
        return f"{url.host}{suffix}"


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL | None
    public_endpoint_url: URL | None
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

    success_redirect_url: URL | None = None


@dataclass(frozen=True)
class PostgresConfig:
    postgres_dsn: str

    alembic: AlembicConfig

    # based on defaults
    # https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.connect
    pool_min_size: int = 10
    pool_max_size: int = 50

    connect_timeout_s: float = 60.0
    command_timeout_s: float | None = 60.0

    pool_pre_ping: bool = True
    pool_recycle_s: int = 3_600  # 1 hour


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
    interval_sec: float = 60
    credit_notification_threshold: Decimal = Decimal("10")
    retention_delay_days: float = 180  # Half of a year

    @property
    def retention_delay(self) -> timedelta:
        return timedelta(days=self.retention_delay_days)


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
    notifications: NotificationsConfig
    job_policy_enforcer: JobPolicyEnforcerConfig

    api_base_url: URL
    config_url: URL
    admin_url: URL
    vcluster_public_url: URL
    admin_public_url: URL

    oauth: OAuthConfig | None = None

    jobs: JobsConfig = JobsConfig()

    scheduler: JobsSchedulerConfig = JobsSchedulerConfig()

    events: EventsClientConfig | None = None

    platform_admin_enabled: bool = False


@dataclass(frozen=True)
class PollerConfig:
    cluster_name: str
    platform_api_url: URL
    server: ServerConfig

    auth: AuthConfig

    admin_url: URL | None
    config_url: URL

    registry_config: RegistryConfig
    kube_config: KubeConfig

    jobs: JobsConfig = JobsConfig()

    scheduler: JobsSchedulerConfig = JobsSchedulerConfig()

    events: EventsClientConfig | None = None


@dataclass(frozen=True)
class PlatformConfig:
    server_endpoint_url: URL
