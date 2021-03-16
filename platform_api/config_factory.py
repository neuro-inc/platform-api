import os
import pathlib
from decimal import Decimal
from typing import Dict, Optional, Sequence

from alembic.config import Config as AlembicConfig
from yarl import URL

from .config import (
    AuthConfig,
    Config,
    CORSConfig,
    DatabaseConfig,
    JobPolicyEnforcerConfig,
    JobsConfig,
    JobsSchedulerConfig,
    NotificationsConfig,
    OAuthConfig,
    PlatformConfig,
    PollerConfig,
    PostgresConfig,
    SentryConfig,
    ServerConfig,
    ZipkinConfig,
)


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None):
        self._environ = environ or os.environ

    def _get_bool(self, name: str, default: bool = False) -> bool:
        value = self._environ.get(name)
        if not value:  # None/""
            return default
        return value.lower() in ("true", "1", "yes", "y")

    def create(self) -> Config:
        auth = self.create_auth()
        jobs = self.create_jobs(orphaned_job_owner=auth.service_name)
        api_base_url = URL(self._environ["NP_API_URL"])
        admin_url = URL(self._environ["NP_ADMIN_URL"])
        config_url = URL(self._environ["NP_PLATFORM_CONFIG_URI"])
        return Config(
            server=self.create_server(),
            database=self.create_database(),
            auth=auth,
            zipkin=self.create_zipkin(),
            oauth=self.try_create_oauth(),
            jobs=jobs,
            job_policy_enforcer=self.create_job_policy_enforcer(),
            scheduler=self.create_job_scheduler(),
            notifications=self.create_notifications(),
            cors=self.create_cors(),
            config_url=config_url,
            admin_url=admin_url,
            api_base_url=api_base_url,
            sentry=self.create_sentry(),
        )

    def create_sentry(self) -> Optional[SentryConfig]:
        sentry_url = self._environ.get("NP_SENTRY_URL")
        sentry_cluster = self._environ.get("NP_SENTRY_CLUSTER")
        if sentry_url and sentry_cluster:
            return SentryConfig(sentry_url, sentry_cluster)
        return None

    def create_poller(self) -> PollerConfig:
        auth = self.create_auth()
        jobs = self.create_jobs(orphaned_job_owner=auth.service_name)
        config_url = URL(self._environ["NP_PLATFORM_CONFIG_URI"])
        cluster_name = self._environ["NP_CLUSTER_NAME"]
        return PollerConfig(
            platform_api_url=URL(self._environ["NP_PLATFORM_API_URL"]),
            server=self.create_server(),
            auth=auth,
            jobs=jobs,
            scheduler=self.create_job_scheduler(),
            config_url=config_url,
            cluster_name=cluster_name,
            sentry=self.create_sentry(),
        )

    def create_jobs(self, *, orphaned_job_owner: str) -> JobsConfig:
        return JobsConfig(
            deletion_delay_s=int(
                self._environ.get("NP_K8S_JOB_DELETION_DELAY", 15 * 60)  # 15 minutes
            ),
            image_pull_error_delay_s=int(
                self._environ.get("NP_K8S_JOB_IMAGE_PULL_DELAY", 60)  # 1 minute
            ),
            orphaned_job_owner=orphaned_job_owner,
            jobs_ingress_class=self._environ.get(
                "NP_K8S_JOBS_INGRESS_CLASS", JobsConfig.jobs_ingress_class
            ),
            jobs_ingress_oauth_url=URL(
                self._environ["NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL"]
            ),
        )

    def create_job_policy_enforcer(self) -> JobPolicyEnforcerConfig:
        return JobPolicyEnforcerConfig(
            platform_api_url=URL(self._environ["NP_ENFORCER_PLATFORM_API_URL"]),
            token=self._environ["NP_ENFORCER_TOKEN"],
            interval_sec=float(
                self._environ.get("NP_ENFORCER_INTERVAL_SEC")
                or JobPolicyEnforcerConfig.interval_sec
            ),
            credit_notification_threshold=Decimal(
                self._environ.get("NP_ENFORCER_CREDIT_NOTIFICATION_THRESHOLD")
                or JobPolicyEnforcerConfig.credit_notification_threshold
            ),
        )

    def create_job_scheduler(self) -> JobsSchedulerConfig:
        return JobsSchedulerConfig(
            run_quantum_sec=float(
                self._environ.get("RUN_QUANTUM_SEC")
                or JobsSchedulerConfig.run_quantum_sec
            ),
            max_suspended_time_sec=float(
                self._environ.get("MAX_SUSPENDED_TIME_SEC")
                or JobsSchedulerConfig.max_suspended_time_sec
            ),
            is_waiting_min_time_sec=float(
                self._environ.get("IS_WAITING_MIN_TIME_SEC")
                or JobsSchedulerConfig.is_waiting_min_time_sec
            ),
        )

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get("NP_API_PORT", ServerConfig.port))
        return ServerConfig(port=port)

    def create_platform(self) -> PlatformConfig:
        server_endpoint_url = URL(self._environ["NP_PLATFORM_API_URL"])
        return PlatformConfig(server_endpoint_url=server_endpoint_url)

    def create_database(self) -> DatabaseConfig:
        postgres = self.create_postgres()
        return DatabaseConfig(
            postgres=postgres,
        )

    def create_auth(self) -> AuthConfig:
        url = URL(self._environ["NP_AUTH_URL"])
        token = self._environ["NP_AUTH_TOKEN"]
        name = self._environ.get("NP_AUTH_NAME", AuthConfig.service_name)
        public_endpoint_url = URL(self._environ.get("NP_AUTH_PUBLIC_URL", ""))
        return AuthConfig(
            server_endpoint_url=url,
            service_token=token,
            service_name=name,
            public_endpoint_url=public_endpoint_url,
        )

    def create_zipkin(self) -> ZipkinConfig:
        url = URL(self._environ["NP_API_ZIPKIN_URL"])
        sample_rate = float(self._environ["NP_API_ZIPKIN_SAMPLE_RATE"])
        return ZipkinConfig(url=url, sample_rate=sample_rate)

    def try_create_oauth(self) -> Optional[OAuthConfig]:
        auth_url = self._environ.get("NP_OAUTH_AUTH_URL")
        token_url = self._environ.get("NP_OAUTH_TOKEN_URL")
        logout_url = self._environ.get("NP_OAUTH_LOGOUT_URL")
        client_id = self._environ.get("NP_OAUTH_CLIENT_ID")
        audience = self._environ.get("NP_OAUTH_AUDIENCE")
        success_redirect_url = self._environ.get("NP_OAUTH_SUCCESS_REDIRECT_URL")
        headless_callback_url = self._environ["NP_OAUTH_HEADLESS_CALLBACK_URL"]
        if not (
            auth_url
            and token_url
            and logout_url
            and client_id
            and audience
            and success_redirect_url
            and headless_callback_url
        ):
            return None
        return OAuthConfig(
            auth_url=URL(auth_url),
            token_url=URL(token_url),
            logout_url=URL(logout_url),
            client_id=client_id,
            audience=audience,
            headless_callback_url=URL(headless_callback_url),
            success_redirect_url=URL(success_redirect_url),
        )

    def create_notifications(self) -> NotificationsConfig:
        url = URL(self._environ["NP_NOTIFICATIONS_URL"])
        token = self._environ["NP_NOTIFICATIONS_TOKEN"]
        return NotificationsConfig(url=url, token=token)

    def create_cors(self) -> CORSConfig:
        origins: Sequence[str] = CORSConfig.allowed_origins
        origins_str = self._environ.get("NP_CORS_ORIGINS", "").strip()
        if origins_str:
            origins = origins_str.split(",")
        return CORSConfig(allowed_origins=origins)

    def create_postgres(self) -> PostgresConfig:
        try:
            postgres_dsn = self._environ["NP_DB_POSTGRES_DSN"]
        except KeyError:
            # Temporary fix until postgres deployment is set
            postgres_dsn = ""
        pool_min_size = int(
            self._environ.get("NP_DB_POSTGRES_POOL_MIN", PostgresConfig.pool_min_size)
        )
        pool_max_size = int(
            self._environ.get("NP_DB_POSTGRES_POOL_MAX", PostgresConfig.pool_max_size)
        )
        connect_timeout_s = float(
            self._environ.get(
                "NP_DB_POSTGRES_CONNECT_TIMEOUT", PostgresConfig.connect_timeout_s
            )
        )
        command_timeout_s = PostgresConfig.command_timeout_s
        if self._environ.get("NP_DB_POSTGRES_COMMAND_TIMEOUT"):
            command_timeout_s = float(self._environ["NP_DB_POSTGRES_COMMAND_TIMEOUT"])
        return PostgresConfig(
            postgres_dsn=postgres_dsn,
            alembic=self.create_alembic(postgres_dsn),
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
            connect_timeout_s=connect_timeout_s,
            command_timeout_s=command_timeout_s,
        )

    def create_alembic(self, postgres_dsn: str) -> AlembicConfig:
        parent_path = pathlib.Path(__file__).resolve().parent.parent
        ini_path = str(parent_path / "alembic.ini")
        script_path = str(parent_path / "alembic")
        config = AlembicConfig(ini_path)
        config.set_main_option("script_location", script_path)
        config.set_main_option("sqlalchemy.url", postgres_dsn.replace("%", "%%"))
        return config
