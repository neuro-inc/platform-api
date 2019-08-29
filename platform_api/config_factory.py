import os
from pathlib import Path, PurePath
from typing import Any, Dict, List, Optional

from yarl import URL

import platform_api

from .cluster_config import (
    ClusterConfig,
    IngressConfig,
    RegistryConfig,
    StorageConfig,
    StorageType,
)
from .config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    JobsConfig,
    NotificationsConfig,
    OAuthConfig,
    PlatformConfig,
    ServerConfig,
    SSHAuthConfig,
    SSHConfig,
    SSHServerConfig,
)
from .orchestrator.kube_client import KubeClientAuthType
from .orchestrator.kube_orchestrator import KubeConfig
from .redis import RedisConfig
from .resource import ResourcePoolType


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None):
        self._environ = environ or os.environ

    def _get_bool(self, name: str, default: bool = False) -> bool:
        value = self._environ.get(name)
        if not value:  # None/""
            return default
        return value.lower() in ("true", "1", "yes", "y")

    def create(self) -> Config:
        env_prefix = self._environ.get("NP_ENV_PREFIX", Config.env_prefix)
        auth = self.create_auth()
        jobs = self.create_jobs(orphaned_job_owner=auth.service_name)
        return Config(
            server=self.create_server(),
            database=self.create_database(),
            auth=auth,
            oauth=self.try_create_oauth(),
            env_prefix=env_prefix,
            jobs=jobs,
            config_client=self.create_config_client(),
            notifications=self.create_notifications(),
        )

    def create_cluster(self) -> ClusterConfig:
        return ClusterConfig(
            name=JobsConfig.default_cluster_name,
            storage=self.create_storage(),
            registry=self.create_registry(),
            orchestrator=self.create_orchestrator(),
            ingress=self.create_ingress(),
        )

    def create_jobs(self, *, orphaned_job_owner: str) -> JobsConfig:
        return JobsConfig(
            deletion_delay_s=int(
                self._environ.get("NP_K8S_JOB_DELETION_DELAY", 60 * 60 * 24)  # one day
            ),
            orphaned_job_owner=orphaned_job_owner,
            jobs_ingress_class=self._environ.get(
                "NP_K8S_JOBS_INGRESS_CLASS", JobsConfig.jobs_ingress_class
            ),
            jobs_ingress_oauth_url=URL(
                self._environ["NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL"]
            ),
        )

    def create_ssh(self) -> SSHConfig:
        env_prefix = self._environ.get("NP_ENV_PREFIX", SSHConfig.env_prefix)
        storage = self.create_storage()
        database = self.create_database()
        auth = self.create_auth()
        registry = self.create_registry()
        return SSHConfig(
            server=self.create_ssh_server(),
            storage=storage,
            orchestrator=self.create_orchestrator(),
            database=database,
            auth=auth,
            registry=registry,
            env_prefix=env_prefix,
        )

    def create_ssh_auth(self) -> SSHAuthConfig:
        platform = self.create_platform()
        auth = self.create_auth()
        log_fifo = Path(self._environ["NP_LOG_FIFO"])
        jobs_namespace = self._environ.get("NP_K8S_NS", SSHAuthConfig.jobs_namespace)
        return SSHAuthConfig(
            platform=platform,
            auth=auth,
            log_fifo=log_fifo,
            jobs_namespace=jobs_namespace,
        )

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get("NP_API_PORT", ServerConfig.port))
        return ServerConfig(port=port)

    def create_ssh_server(self) -> SSHServerConfig:
        port = int(self._environ.get("NP_SSH_PORT", SSHServerConfig.port))
        # NP_SSH_HOST_KEYS is a comma separated list of paths to SSH server keys
        ssh_host_keys = [
            s.strip()
            for s in self._environ.get("NP_SSH_HOST_KEYS", "").split(",")
            if s.strip()
        ]
        return SSHServerConfig(port=port, ssh_host_keys=ssh_host_keys)

    def create_platform(self) -> PlatformConfig:
        server_endpoint_url = URL(self._environ["NP_PLATFORM_API_URL"])
        return PlatformConfig(server_endpoint_url=server_endpoint_url)

    @property
    def _storage_host_mount_path(self) -> PurePath:
        return PurePath(self._environ["NP_STORAGE_HOST_MOUNT_PATH"])

    def create_storage(self) -> StorageConfig:
        host_mount_path = self._storage_host_mount_path
        container_mount_path = PurePath(
            self._environ.get(
                "NP_STORAGE_CONTAINER_MOUNT_PATH",
                str(StorageConfig.container_mount_path),
            )
        )
        storage_type = StorageType(
            self._environ.get("NP_STORAGE_TYPE", StorageConfig.type)
        )
        kwargs: Dict[str, Any] = {}
        if storage_type == StorageType.NFS:
            kwargs.update(
                nfs_server=self._environ["NP_STORAGE_NFS_SERVER"],
                nfs_export_path=PurePath(self._environ["NP_STORAGE_NFS_PATH"]),
            )
        return StorageConfig(
            host_mount_path=host_mount_path,
            container_mount_path=container_mount_path,
            type=storage_type,
            **kwargs,
        )

    def create_orchestrator(self) -> KubeConfig:
        endpoint_url = self._environ["NP_K8S_API_URL"]
        auth_type = KubeClientAuthType(
            self._environ.get("NP_K8S_AUTH_TYPE", KubeConfig.auth_type.value)
        )

        pool_types = self.create_resource_pool_types()

        ca_path = self._environ.get("NP_K8S_CA_PATH")
        ca_data = Path(ca_path).read_text() if ca_path else None

        token_path = self._environ.get("NP_K8S_TOKEN_PATH")
        token = Path(token_path).read_text() if token_path else None

        return KubeConfig(
            endpoint_url=endpoint_url,
            cert_authority_data_pem=ca_data,
            cert_authority_path=None,  # disable it so that only `ca_data` works here
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get("NP_K8S_AUTH_CERT_KEY_PATH"),
            token=token,
            token_path=None,
            namespace=self._environ.get("NP_K8S_NS", KubeConfig.namespace),
            client_conn_timeout_s=int(
                self._environ.get(
                    "NP_K8S_CLIENT_CONN_TIMEOUT", KubeConfig.client_conn_timeout_s
                )
            ),
            client_read_timeout_s=int(
                self._environ.get(
                    "NP_K8S_CLIENT_READ_TIMEOUT", KubeConfig.client_read_timeout_s
                )
            ),
            client_conn_pool_size=int(
                self._environ.get(
                    "NP_K8S_CLIENT_CONN_POOL_SIZE", KubeConfig.client_conn_pool_size
                )
            ),
            jobs_ingress_class=self._environ.get(
                "NP_K8S_JOBS_INGRESS_CLASS", KubeConfig.jobs_ingress_class
            ),
            jobs_ingress_oauth_url=URL(
                self._environ["NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL"]
            ),
            is_http_ingress_secure=self._get_bool("NP_K8S_JOBS_INGRESS_HTTPS"),
            jobs_domain_name_template=self._environ[
                "NP_K8S_JOBS_INGRESS_DOMAIN_NAME_TEMPLATE"
            ],
            ssh_auth_domain_name=self._environ["NP_K8S_SSH_AUTH_INGRESS_DOMAIN_NAME"],
            resource_pool_types=pool_types,
            node_label_gpu=self._environ.get("NP_K8S_NODE_LABEL_GPU"),
            node_label_preemptible=self._environ.get("NP_K8S_NODE_LABEL_PREEMPTIBLE"),
        )

    def create_resource_pool_types(self) -> List[ResourcePoolType]:
        models = self._environ.get("NP_GKE_GPU_MODELS", "")
        # the default pool that represents a non-GPU instance type
        types = [ResourcePoolType()]
        # skipping blanks
        model_ids = [model_id for model_id in models.split(",") if model_id]
        # removing duplicates, but preserving the order
        model_ids = list(dict.fromkeys(model_ids))
        for model_id in model_ids:
            # TODO (A Danshyn 10/23/18): drop the hardcoded number of GPUs
            types.append(ResourcePoolType(gpu=1, gpu_model=model_id))
        return types

    def create_database(self) -> DatabaseConfig:
        redis = self.create_redis()
        return DatabaseConfig(redis=redis)

    def create_redis(self) -> Optional[RedisConfig]:
        uri = self._environ.get("NP_DB_REDIS_URI")
        if not uri:
            return None
        conn_pool_size = int(
            self._environ.get("NP_DB_REDIS_CONN_POOL_SIZE", RedisConfig.conn_pool_size)
        )
        conn_timeout_s = float(
            self._environ.get("NP_DB_REDIS_CONN_TIMEOUT", RedisConfig.conn_timeout_s)
        )
        return RedisConfig(
            uri=uri, conn_pool_size=conn_pool_size, conn_timeout_s=conn_timeout_s
        )

    def create_auth(self) -> AuthConfig:
        url = URL(self._environ["NP_AUTH_URL"])
        token = self._environ["NP_AUTH_TOKEN"]
        name = self._environ.get("NP_AUTH_NAME", AuthConfig.service_name)
        public_endpoint_url = URL()
        if "NP_AUTH_PUBLIC_URL" in self._environ:
            public_endpoint_url = URL(self._environ["NP_AUTH_PUBLIC_URL"])
        return AuthConfig(
            server_endpoint_url=url,
            service_token=token,
            service_name=name,
            public_endpoint_url=public_endpoint_url,
        )

    def try_create_oauth(self) -> Optional[OAuthConfig]:
        base_url = self._environ.get("NP_OAUTH_BASE_URL")
        client_id = self._environ.get("NP_OAUTH_CLIENT_ID")
        audience = self._environ.get("NP_OAUTH_AUDIENCE")
        success_redirect_url = self._environ.get("NP_OAUTH_SUCCESS_REDIRECT_URL")
        headless_callback_url = self._environ["NP_OAUTH_HEADLESS_CALLBACK_URL"]
        if not (
            base_url
            and client_id
            and audience
            and success_redirect_url
            and headless_callback_url
        ):
            return None
        return OAuthConfig(
            base_url=URL(base_url),
            client_id=client_id,
            audience=audience,
            headless_callback_url=URL(headless_callback_url),
            success_redirect_url=URL(success_redirect_url),
        )

    def create_registry(self) -> RegistryConfig:
        host = self._environ.get("NP_REGISTRY_HOST")
        if host:
            is_https = self._get_bool("NP_REGISTRY_HTTPS", True)
            scheme = "https" if is_https else "http"
            url = URL(f"{scheme}://{host}")
        else:
            url = RegistryConfig.url
        return RegistryConfig(url=url)

    def create_ingress(self) -> IngressConfig:
        base_url = URL(self._environ["NP_API_URL"])
        return IngressConfig(
            storage_url=base_url / "storage", monitoring_url=base_url / "jobs"
        )

    def create_config_client(self) -> platform_api.config_client.ConfigClient:
        platform_config_url = URL(self._environ["NP_PLATFORM_CONFIG_URI"])
        return platform_api.config_client.ConfigClient(
            base_url=platform_config_url, service_token=self._environ["NP_AUTH_TOKEN"]
        )

    def create_notifications(self) -> NotificationsConfig:
        url = URL(self._environ["NP_NOTIFICATIONS_URL"])
        token = self._environ["NP_NOTIFICATIONS_TOKEN"]
        return NotificationsConfig(url=url, token=token)
