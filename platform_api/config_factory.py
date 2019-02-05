import os
from pathlib import Path, PurePath
from typing import List, Optional

from yarl import URL

from .config import (
    AuthConfig,
    Config,
    DatabaseConfig,
    ElasticsearchConfig,
    LoggingConfig,
    OAuthConfig,
    PlatformConfig,
    RegistryConfig,
    ServerConfig,
    SSHAuthConfig,
    SSHConfig,
    SSHServerConfig,
    StorageConfig,
    StorageType,
)
from .orchestrator import KubeConfig
from .orchestrator.kube_orchestrator import KubeClientAuthType
from .redis import RedisConfig
from .resource import GKEGPUModels, ResourcePoolType


class EnvironConfigFactory:
    def __init__(self, environ=None):
        self._environ = environ or os.environ

    def _get_bool(self, name, default: bool = False) -> bool:
        value = self._environ.get(name)
        if not value:  # None/""
            return default
        return value.lower() in ("true", "1", "yes", "y")

    def create(self):
        env_prefix = self._environ.get("NP_ENV_PREFIX", Config.env_prefix)
        storage = self.create_storage()
        database = self.create_database()
        auth = self.create_auth()
        oauth = self.create_oauth()
        registry = self.create_registry()
        return Config(
            server=self.create_server(),
            storage=storage,
            orchestrator=self.create_orchestrator(storage, registry, auth),
            database=database,
            auth=auth,
            oauth=oauth,
            logging=self.create_logging(),
            registry=registry,
            env_prefix=env_prefix,
        )

    def create_ssh(self):
        env_prefix = self._environ.get("NP_ENV_PREFIX", SSHConfig.env_prefix)
        storage = self.create_storage()
        database = self.create_database()
        auth = self.create_auth()
        registry = self.create_registry()
        return SSHConfig(
            server=self.create_ssh_server(),
            storage=storage,
            orchestrator=self.create_orchestrator(storage, registry, auth),
            database=database,
            auth=auth,
            registry=registry,
            env_prefix=env_prefix,
        )

    def create_ssh_auth(self) -> SSHAuthConfig:
        platform = self.create_platform()
        auth = self.create_auth()
        log_fifo = Path(self._environ["NP_LOG_FIFO"])
        return SSHAuthConfig(platform=platform, auth=auth, log_fifo=log_fifo)

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get("NP_API_PORT", ServerConfig.port))
        return ServerConfig(port=port)  # type: ignore

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
        uri_scheme = self._environ.get(
            "NP_STORAGE_URI_SCHEME", StorageConfig.uri_scheme
        )
        kwargs = {}
        if storage_type == StorageType.NFS:
            kwargs.update(
                dict(
                    nfs_server=self._environ["NP_STORAGE_NFS_SERVER"],
                    nfs_export_path=PurePath(self._environ["NP_STORAGE_NFS_PATH"]),
                )
            )
        return StorageConfig(  # type: ignore
            host_mount_path=host_mount_path,
            container_mount_path=container_mount_path,
            type=storage_type,
            uri_scheme=uri_scheme,
            **kwargs
        )

    def create_orchestrator(
        self, storage: StorageConfig, registry: RegistryConfig, auth: AuthConfig
    ) -> KubeConfig:
        endpoint_url = self._environ["NP_K8S_API_URL"]
        auth_type = KubeClientAuthType(
            self._environ.get("NP_K8S_AUTH_TYPE", KubeConfig.auth_type.value)
        )

        pool_types = self.create_resource_pool_types()

        return KubeConfig(  # type: ignore
            storage=storage,
            registry=registry,
            endpoint_url=endpoint_url,
            cert_authority_path=self._environ.get("NP_K8S_CA_PATH"),
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get("NP_K8S_AUTH_CERT_KEY_PATH"),
            token_path=self._environ.get("NP_K8S_TOKEN_PATH"),
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
            jobs_ingress_name=self._environ["NP_K8S_JOBS_INGRESS_NAME"],
            is_http_ingress_secure=self._get_bool("NP_K8S_JOBS_INGRESS_HTTPS"),
            jobs_domain_name=(self._environ["NP_K8S_JOBS_INGRESS_DOMAIN_NAME"]),
            ssh_domain_name=self._environ["NP_K8S_SSH_INGRESS_DOMAIN_NAME"],
            ssh_auth_domain_name=self._environ["NP_K8S_SSH_AUTH_INGRESS_DOMAIN_NAME"],
            job_deletion_delay_s=int(
                self._environ.get(
                    "NP_K8S_JOB_DELETION_DELAY", KubeConfig.job_deletion_delay_s
                )
            ),
            resource_pool_types=pool_types,
            node_label_gpu=self._environ.get("NP_K8S_NODE_LABEL_GPU"),
            node_label_preemptible=self._environ.get("NP_K8S_NODE_LABEL_PREEMPTIBLE"),
            orphaned_job_owner=auth.service_name,
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
            model = GKEGPUModels.find_model_by_id(model_id)
            if model:
                # TODO (A Danshyn 10/23/18): drop the hardcoded number of GPUs
                types.append(ResourcePoolType(gpu=1, gpu_model=model))
        return types

    def create_database(self) -> DatabaseConfig:
        redis = self.create_redis()
        return DatabaseConfig(redis=redis)  # type: ignore

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
        return RedisConfig(  # type: ignore
            uri=uri, conn_pool_size=conn_pool_size, conn_timeout_s=conn_timeout_s
        )

    def create_logging(self) -> LoggingConfig:
        es = self.create_elasticsearch()
        return LoggingConfig(elasticsearch=es)

    def create_elasticsearch(self) -> ElasticsearchConfig:
        hosts = self._environ["NP_ES_HOSTS"].split(",")
        return ElasticsearchConfig(hosts=hosts)

    def create_auth(self) -> AuthConfig:
        url = URL(self._environ["NP_AUTH_URL"])
        token = self._environ["NP_AUTH_TOKEN"]
        name = self._environ.get("NP_AUTH_NAME", AuthConfig.service_name)
        return AuthConfig(
            server_endpoint_url=url, service_token=token, service_name=name
        )  # type: ignore

    def create_oauth(self) -> OAuthConfig:
        base_url = URL(self._environ["NP_OAUTH_BASE_URL"])
        client_id = self._environ["NP_OAUTH_CLIENT_ID"]
        audience = self._environ["NP_OAUTH_CLIENT_AUDIENCE"]
        success_redirect_url = URL(
            self._environ.get("NP_OAUTH_CLIENT_SUCCESS_REDIRECT_URL")
        )
        return OAuthConfig(
            base_url=base_url,
            client_id=client_id,
            audience=audience,
            success_redirect_url=success_redirect_url,
        )

    def create_registry(self) -> RegistryConfig:
        host = self._environ.get("NP_REGISTRY_HOST", RegistryConfig.host)
        return RegistryConfig(host=host)
