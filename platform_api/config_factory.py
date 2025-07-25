import os
import pathlib
from decimal import Decimal

from yarl import URL

from alembic.config import Config as AlembicConfig

from .config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    JobPolicyEnforcerConfig,
    JobsConfig,
    JobsSchedulerConfig,
    NotificationsConfig,
    OAuthConfig,
    PlatformConfig,
    PollerConfig,
    PostgresConfig,
    RegistryConfig,
    ServerConfig,
)
from .orchestrator.kube_config import KubeClientAuthType, KubeConfig


class EnvironConfigFactory:
    def __init__(self, environ: dict[str, str] | None = None):
        self._environ = environ or os.environ

    def _get_bool(self, name: str, default: bool = False) -> bool:
        value = self._environ.get(name)
        if not value:  # None/""
            return default
        return value.lower() in ("true", "1", "yes", "y")

    def _get_url(self, name: str) -> URL | None:
        value = self._environ[name]
        if value == "-":
            return None
        return URL(value)

    def create(self) -> Config:
        auth = self.create_auth()
        jobs = self.create_jobs(orphaned_job_owner=auth.service_name)
        api_base_url = URL(self._environ["NP_API_URL"])
        admin_url = self._get_url("NP_ADMIN_URL")
        admin_public_url = self._get_url("NP_ADMIN_PUBLIC_URL")
        config_url = URL(self._environ["NP_PLATFORM_CONFIG_URI"])
        return Config(
            server=self.create_server(),
            database=self.create_database(),
            auth=auth,
            oauth=self.try_create_oauth(),
            jobs=jobs,
            job_policy_enforcer=self.create_job_policy_enforcer(),
            scheduler=self.create_job_scheduler(),
            notifications=self.create_notifications(),
            config_url=config_url,
            admin_url=admin_url,
            admin_public_url=admin_public_url,
            api_base_url=api_base_url,
        )

    def create_poller(self) -> PollerConfig:
        auth = self.create_auth()
        jobs = self.create_jobs(orphaned_job_owner=auth.service_name)
        config_url = URL(self._environ["NP_PLATFORM_CONFIG_URI"])
        admin_url = self._get_url("NP_PLATFORM_ADMIN_URI")
        cluster_name = self._environ["NP_CLUSTER_NAME"]
        return PollerConfig(
            platform_api_url=URL(self._environ["NP_PLATFORM_API_URL"]),
            server=self.create_server(),
            auth=auth,
            jobs=jobs,
            scheduler=self.create_job_scheduler(),
            config_url=config_url,
            admin_url=admin_url,
            cluster_name=cluster_name,
            registry_config=self.create_registry(),
            kube_config=self.create_kube(),
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
        )

    def create_job_policy_enforcer(self) -> JobPolicyEnforcerConfig:
        return JobPolicyEnforcerConfig(
            platform_api_url=URL(self._environ["NP_ENFORCER_PLATFORM_API_URL"]),
            interval_sec=float(
                self._environ.get("NP_ENFORCER_INTERVAL_SEC")
                or JobPolicyEnforcerConfig.interval_sec
            ),
            credit_notification_threshold=Decimal(
                self._environ.get("NP_ENFORCER_CREDIT_NOTIFICATION_THRESHOLD")
                or JobPolicyEnforcerConfig.credit_notification_threshold
            ),
            retention_delay_days=float(
                self._environ.get("NP_ENFORCER_RETENTION_DELAY_DAYS")
                or JobPolicyEnforcerConfig.retention_delay_days
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
        url = self._get_url("NP_AUTH_URL")
        token = self._environ.get("NP_AUTH_TOKEN", "")
        name = self._environ.get("NP_AUTH_NAME", AuthConfig.service_name)
        public_endpoint_url = self._get_url("NP_AUTH_PUBLIC_URL")
        return AuthConfig(
            server_endpoint_url=url,
            service_token=token,
            service_name=name,
            public_endpoint_url=public_endpoint_url,
        )

    def try_create_oauth(self) -> OAuthConfig | None:
        auth_url = self._environ.get("NP_OAUTH_AUTH_URL")
        token_url = self._environ.get("NP_OAUTH_TOKEN_URL")
        logout_url = self._environ.get("NP_OAUTH_LOGOUT_URL")
        client_id = self._environ.get("NP_OAUTH_CLIENT_ID")
        audience = self._environ.get("NP_OAUTH_AUDIENCE")
        success_redirect_url = self._environ.get("NP_OAUTH_SUCCESS_REDIRECT_URL")
        headless_callback_url = self._environ.get("NP_OAUTH_HEADLESS_CALLBACK_URL")
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
        url = URL(self._environ.get("NP_NOTIFICATIONS_URL", ""))
        token = self._environ.get("NP_NOTIFICATIONS_TOKEN", "")
        return NotificationsConfig(url=url, token=token)

    def create_postgres(self) -> PostgresConfig:
        try:
            postgres_dsn = to_async_postgres_dsn(self._environ["NP_DB_POSTGRES_DSN"])
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
        postgres_dsn = to_sync_postgres_dsn(postgres_dsn)
        config.set_main_option("sqlalchemy.url", postgres_dsn.replace("%", "%%"))
        return config

    def create_kube(self) -> KubeConfig:
        return KubeConfig(
            endpoint_url=self._environ["NP_KUBE_URL"],
            auth_type=KubeClientAuthType(
                self._environ.get("NP_KUBE_AUTH_TYPE", "none")
            ),
            cert_authority_data_pem=self._environ.get("NP_KUBE_CA_DATA"),
            cert_authority_path=self._environ.get("NP_KUBE_CA_DATA_PATH"),
            auth_cert_path=self._environ.get("NP_KUBE_CERT_PATH"),
            auth_cert_key_path=self._environ.get("NP_KUBE_CERT_KEY_PATH"),
            token=self._environ.get("NP_KUBE_TOKEN"),
            token_path=self._environ.get("NP_KUBE_TOKEN_PATH"),
            namespace=self._environ.get("NP_KUBE_NAMESPACE", KubeConfig.namespace),
            client_conn_timeout_s=int(
                self._environ.get(
                    "NP_KUBE_CONN_TIMEOUT", KubeConfig.client_conn_timeout_s
                )
            ),
            client_read_timeout_s=int(
                self._environ.get(
                    "NP_KUBE_READ_TIMEOUT", KubeConfig.client_read_timeout_s
                )
            ),
            client_conn_pool_size=int(
                self._environ.get(
                    "NP_KUBE_CONN_POOL_SIZE", KubeConfig.client_conn_pool_size
                )
            ),
            jobs_ingress_class=self._environ.get(
                "NP_KUBE_INGRESS_CLASS", KubeConfig.jobs_ingress_class
            ),
            jobs_ingress_auth_middleware=self._environ.get(
                "NP_KUBE_INGRESS_AUTH_MIDDLEWARE",
                KubeConfig.jobs_ingress_auth_middleware,
            ),
            jobs_ingress_error_page_middleware=self._environ.get(
                "NP_KUBE_INGRESS_ERROR_PAGE_MIDDLEWARE",
                KubeConfig.jobs_ingress_error_page_middleware,
            ),
            jobs_pod_job_toleration_key=self._environ.get(
                "NP_KUBE_POD_JOB_TOLERATION_KEY", KubeConfig.jobs_pod_job_toleration_key
            ),
            jobs_pod_preemptible_toleration_key=self._environ.get(
                "NP_KUBE_POD_PREEMPTIBLE_TOLERATION_KEY"
            ),
            jobs_pod_priority_class_name=self._environ.get(
                "NP_KUBE_POD_PRIORITY_CLASS_NAME"
            ),
            node_label_preemptible=self._environ.get("NP_KUBE_NODE_LABEL_PREEMPTIBLE"),
            node_label_job=self._environ.get("NP_KUBE_NODE_LABEL_JOB"),
            node_label_node_pool=self._environ.get("NP_KUBE_NODE_LABEL_NODE_POOL"),
            image_pull_secret_name=self._environ.get("NP_KUBE_IMAGE_PULL_SECRET"),
            external_job_runner_image=self._environ.get(
                "NP_KUBE_EXTERNAL_JOB_RUNNER_IMAGE",
                KubeConfig.external_job_runner_image,
            ),
        )

    def create_registry(self) -> RegistryConfig:
        return RegistryConfig(
            url=URL(self._environ["NP_REGISTRY_URL"]),
            username=self._environ.get("NP_AUTH_NAME", AuthConfig.service_name),
            password=self._environ["NP_AUTH_TOKEN"],
            email=self._environ.get("NP_REGISTRY_EMAIL", RegistryConfig.email),
        )


syncpg_schema = "postgresql"
asyncpg_schema = "postgresql+asyncpg"


def to_async_postgres_dsn(dsn: str) -> str:
    if dsn.startswith(syncpg_schema + "://"):
        dsn = asyncpg_schema + dsn[len(syncpg_schema) :]
    return dsn


def to_sync_postgres_dsn(dsn: str) -> str:
    if dsn.startswith(asyncpg_schema + "://"):
        dsn = syncpg_schema + dsn[len(asyncpg_schema) :]
    return dsn
