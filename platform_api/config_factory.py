from pathlib import PurePath
import os

from .config import Config, ServerConfig, StorageConfig, StorageType
from .orchestrator import KubeConfig
from .orchestrator.kube_orchestrator import KubeClientAuthType


class EnvironConfigFactory:
    def __init__(self, environ=None):
        self._environ = environ or os.environ

    def create(self):
        env_prefix = self._environ.get('NP_ENV_PREFIX', Config.env_prefix)
        storage = self.create_storage()
        return Config(
            server=self.create_server(),
            storage=storage,
            orchestrator=self.create_orchestrator(storage),
            env_prefix=env_prefix,
        )

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get('NP_API_PORT', ServerConfig.port))
        return ServerConfig(port=port)  # type: ignore

    @property
    def _storage_host_mount_path(self) -> PurePath:
        return PurePath(self._environ['NP_STORAGE_HOST_MOUNT_PATH'])

    def create_storage(self) -> StorageConfig:
        host_mount_path = self._storage_host_mount_path
        container_mount_path = PurePath(self._environ.get(
            'NP_STORAGE_CONTAINER_MOUNT_PATH',
            str(StorageConfig.container_mount_path)))
        storage_type = StorageType(
            self._environ.get('NP_STORAGE_TYPE', StorageConfig.type))
        uri_scheme = self._environ.get(
            'NP_STORAGE_URI_SCHEME', StorageConfig.uri_scheme)
        kwargs = {}
        if storage_type == StorageType.NFS:
            kwargs.update(dict(
                nfs_server=self._environ['NP_STORAGE_NFS_SERVER'],
                nfs_export_path=PurePath(
                    self._environ['NP_STORAGE_NFS_PATH']),
            ))
        return StorageConfig(  # type: ignore
            host_mount_path=host_mount_path,
            container_mount_path=container_mount_path,
            type=storage_type,
            uri_scheme=uri_scheme,
            **kwargs
        )

    def create_orchestrator(self, storage: StorageConfig) -> KubeConfig:
        endpoint_url = self._environ['NP_K8S_API_URL']
        auth_type = KubeClientAuthType(self._environ.get(
            'NP_K8S_AUTH_TYPE', KubeConfig.auth_type.value))

        return KubeConfig(  # type: ignore
            storage=storage,
            endpoint_url=endpoint_url,
            cert_authority_path=self._environ.get('NP_K8S_CA_PATH'),

            auth_type=auth_type,
            auth_cert_path=self._environ.get('NP_K8S_AUTH_CERT_PATH'),
            auth_cert_key_path=self._environ.get('NP_K8S_AUTH_CERT_KEY_PATH'),

            namespace=self._environ.get('NP_K8S_NS', KubeConfig.namespace),

            client_conn_timeout_s=int(self._environ.get(
                'NP_K8S_CLIENT_CONN_TIMEOUT',
                KubeConfig.client_conn_timeout_s)),
            client_read_timeout_s=int(self._environ.get(
                'NP_K8S_CLIENT_READ_TIMEOUT',
                KubeConfig.client_read_timeout_s)),
            client_conn_pool_size=int(self._environ.get(
                'NP_K8S_CLIENT_CONN_POOL_SIZE',
                KubeConfig.client_conn_pool_size)),

            jobs_ingress_name=self._environ['NP_K8S_JOBS_INGRESS_NAME'],
            jobs_domain_name=(
                self._environ['NP_K8S_JOBS_INGRESS_DOMAIN_NAME']),
        )
