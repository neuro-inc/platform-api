from dataclasses import dataclass
from pathlib import PurePath
import os
from typing import Any, Dict

from .orchestrator import KubeConfig
from .orchestrator.kube_orchestrator import KubeClientAuthType, VolumeType


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080


@dataclass(frozen=True)
class StorageConfig:
    host_mount_path: PurePath
    container_mount_path: PurePath = PurePath('/var/storage')

    uri_scheme: str = 'storage'


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    # TODO (A Danshyn 05/30/18): come up with a more generic solution here
    orchestrator: KubeConfig

    # used for generating environment variable names and
    # sourcing them inside containers.
    env_prefix: str = 'NP'  # stands for Neuromation Platform


class EnvironConfigFactory:
    def __init__(self, environ=None):
        self._environ = environ or os.environ

    def create(self):
        env_prefix = self._environ.get('NP_ENV_PREFIX', Config.env_prefix)
        return Config(
            server=self.create_server(),
            storage=self.create_storage(),
            orchestrator=self.create_orchestrator(),
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
        uri_scheme = self._environ.get(
            'NP_STORAGE_URI_SCHEME', StorageConfig.uri_scheme)
        return StorageConfig(  # type: ignore
            host_mount_path=host_mount_path,
            container_mount_path=container_mount_path,
            uri_scheme=uri_scheme,
        )

    def _create_storage_kwargs(self) -> Dict[str, Any]:
        storage_type = VolumeType(
            self._environ.get('NP_STORAGE_TYPE', KubeConfig.storage_type))
        host_mount_path = self._storage_host_mount_path

        kwargs = dict(
            storage_mount_path=host_mount_path,
            storage_type=storage_type,
        )
        if storage_type == VolumeType.NFS:
            kwargs.update(dict(
                nfs_volume_server=self._environ['NP_STORAGE_NFS_SERVER'],
                nfs_volume_export_path=PurePath(
                    self._environ['NP_STORAGE_NFS_PATH']),
            ))
        return kwargs

    def create_orchestrator(self) -> KubeConfig:
        endpoint_url = self._environ['NP_K8S_API_URL']
        auth_type = KubeClientAuthType(self._environ.get(
            'NP_K8S_AUTH_TYPE', KubeConfig.auth_type.value))

        kwargs = {}
        kwargs.update(self._create_storage_kwargs())

        return KubeConfig(  # type: ignore
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
            jobs_ingress_domain_name=(
                self._environ['NP_K8S_JOBS_INGRESS_DOMAIN_NAME']),

            **kwargs
        )
