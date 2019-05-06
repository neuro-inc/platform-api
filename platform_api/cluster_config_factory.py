from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePath
from typing import Any, Dict, Optional, Sequence

import trafaret as t
from yarl import URL

from .elasticsearch import ElasticsearchConfig
from .resource import GKEGPUModels, GPUModel, ResourcePoolType


class StorageType(str, Enum):
    HOST = "host"
    NFS = "nfs"


@dataclass(frozen=True)
class StorageConfig:
    type: StorageType
    container_mount_path: PurePath

    host_mount_path: Optional[PurePath]

    nfs_server: Optional[str]
    nfs_export_path: Optional[PurePath]

    uri_scheme: str = field(init=False, default="storage")

    @property
    def is_nfs(self) -> bool:
        return self.type == StorageType.NFS

    def __post_init__(self) -> None:
        self._check_storage_attrs()

    def _check_storage_attrs(self) -> None:
        nfs_attrs = (self.nfs_server, self.nfs_export_path)
        if self.is_nfs:
            if not all(nfs_attrs):
                raise ValueError("Missing NFS settings")
            if self.host_mount_path:
                raise ValueError("Redundant host settings")
        else:
            if not self.host_mount_path:
                raise ValueError("Missing host settings")
            if any(nfs_attrs):
                raise ValueError("Redundant NFS settings")


_storage_config_validator = t.Dict(
    {
        "type": t.Enum(*[s.value for s in StorageType]),
        "container_mount_path": t.String,
        "host_mount_path": t.String | t.Null,
        "nfs_server": t.String | t.Null,
        "nfs_export_path": t.String | t.Null,
    }
)


@dataclass(frozen=True)
class RegistryConfig:
    host: str
    email: str
    is_secure: bool

    @property
    def url(self) -> URL:
        scheme = "https" if self.is_secure else "http"
        return URL(f"{scheme}://{self.host}")


_registry_config_validator = t.Dict(
    {"host": t.String, "email": t.Email, "is_secure": t.Bool}
)


@dataclass(frozen=True)
class DockerRegistryConfig:
    url: URL
    user_name: str
    password: str
    email: str


_docker_registry_config_validator = t.Dict(
    {"url": t.String, "user_name": t.String, "password": t.String, "email": t.Email}
)


@dataclass(frozen=True)
class HelmRepositoryConfig:
    url: URL
    user_name: str
    token: str


_helm_repository_config_validator = t.Dict(
    {"url": t.String, "user_name": t.String, "token": t.String}
)


@dataclass(frozen=True)
class OrchestratorConfig:
    jobs_domain_name_template: str
    named_jobs_domain_name_template: str
    ssh_domain_name: str
    ssh_auth_domain_name: str
    resource_pool_types: Sequence[ResourcePoolType]
    is_http_ingress_secure: bool


_orchestrator_config_validator = t.Dict(
    {
        "jobs_domain_name_template": t.String,
        "named_jobs_domain_name_template": t.String,
        "ssh_domain_name": t.String,
        "ssh_auth_domain_name": t.String,
        "resource_pool_types": t.List(
            t.Dict(
                {
                    "gpu": t.Int | t.Null,
                    "gpu_model": t.Enum(*[m.value.id for m in GKEGPUModels]) | t.Null,
                }
            )
        ),
        "is_http_ingress_secure": t.Bool,
    }
)


@dataclass(frozen=True)
class LoggingConfig:
    elasticsearch: ElasticsearchConfig


_logging_config_validator = t.Dict(
    {
        "elasticsearch": t.Dict(
            {
                "hosts": t.List(t.String),
                "user": t.String | t.Null,
                "password": t.String | t.Null,
            }
        )
    }
)


@dataclass(frozen=True)
class IngressConfig:
    storage_url: URL
    users_url: URL
    monitoring_url: URL


_ingress_config_validator = t.Dict(
    {"storage_url": t.String, "users_url": t.String, "monitoring_url": t.String}
)


@dataclass(frozen=True)
class AuthConfig:
    url: URL
    storage_token: str
    registry_token: str
    cluster_token: str


_auth_config_validator = t.Dict(
    {
        "url": t.String,
        "storage_token": t.String,
        "registry_token": t.String,
        "cluster_token": t.String,
    }
)


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    type: str
    docker_registry: DockerRegistryConfig
    helm_repository: HelmRepositoryConfig
    auth: AuthConfig
    storage: StorageConfig
    registry: RegistryConfig
    orchestrator: OrchestratorConfig
    logging: LoggingConfig
    ingress: IngressConfig


_cluster_config_validator = t.Dict(
    {
        "type": t.String,
        "name": t.String,
        "docker_registry": _docker_registry_config_validator,
        "helm_repository": _helm_repository_config_validator,
        "auth": _auth_config_validator,
        "storage": _storage_config_validator,
        "registry": _registry_config_validator,
        "orchestrator": _orchestrator_config_validator,
        "logging": _logging_config_validator,
        "ingress": _ingress_config_validator,
    }
)


class ClusterConfigFactory:
    def cluster_configs(
        self, data: Sequence[Dict[str, Any]]
    ) -> Sequence[ClusterConfig]:
        return [self.cluster_config(d) for d in data]

    def cluster_config(self, data: Dict[str, Any]) -> ClusterConfig:
        _cluster_config_validator.check(data)
        return ClusterConfig(
            name=data["name"],
            type=data["type"],
            docker_registry=self.docker_registry_config(data["docker_registry"]),
            helm_repository=self.helm_repository_config(data["helm_repository"]),
            auth=self.auth_config(data["auth"]),
            storage=self.storage_config(data["storage"]),
            registry=self.registry_config(data["registry"]),
            orchestrator=self.orchestrator_config(data["orchestrator"]),
            logging=self.logging_config(data["logging"]),
            ingress=self.ingress_config(data["ingress"]),
        )

    def docker_registry_config(self, data: Dict[str, str]) -> DockerRegistryConfig:
        _docker_registry_config_validator.check(data)
        return DockerRegistryConfig(
            url=URL(data["url"]),
            user_name=data["user_name"],
            password=data["password"],
            email=data["email"],
        )

    def helm_repository_config(self, data: Dict[str, str]) -> HelmRepositoryConfig:
        _helm_repository_config_validator.check(data)
        return HelmRepositoryConfig(
            url=URL(data["url"]), user_name=data["user_name"], token=data["token"]
        )

    def auth_config(self, data: Dict[str, str]) -> AuthConfig:
        _auth_config_validator.check(data)
        return AuthConfig(
            url=URL(data["url"]),
            storage_token=data["storage_token"],
            registry_token=data["registry_token"],
            cluster_token=data["cluster_token"],
        )

    def ingress_config(self, data: Dict[str, str]) -> IngressConfig:
        _ingress_config_validator.check(data)
        return IngressConfig(
            storage_url=URL(data["storage_url"]),
            users_url=URL(data["users_url"]),
            monitoring_url=URL(data["monitoring_url"]),
        )

    def logging_config(self, data: Dict[str, Any]) -> LoggingConfig:
        _logging_config_validator.check(data)
        return LoggingConfig(
            elasticsearch=self._elasticsearch_config(data["elasticsearch"])
        )

    def _elasticsearch_config(self, data: Dict[str, Any]) -> ElasticsearchConfig:
        return ElasticsearchConfig(
            hosts=data["hosts"], user=data.get("user"), password=data.get("password")
        )

    def orchestrator_config(self, data: Dict[str, Any]) -> OrchestratorConfig:
        _orchestrator_config_validator.check(data)
        return OrchestratorConfig(
            jobs_domain_name_template=data["jobs_domain_name_template"],
            named_jobs_domain_name_template=data["named_jobs_domain_name_template"],
            ssh_domain_name=data["ssh_domain_name"],
            ssh_auth_domain_name=data["ssh_auth_domain_name"],
            resource_pool_types=[
                self._resource_pool_type(d) for d in data["resource_pool_types"]
            ],
            is_http_ingress_secure=data["is_http_ingress_secure"],
        )

    def _resource_pool_type(self, data: Dict[str, Any]) -> ResourcePoolType:
        return ResourcePoolType(
            gpu=data.get("gpu"), gpu_model=self._gpu_model(data.get("gpu_model"))
        )

    def registry_config(self, data: Dict[str, Any]) -> RegistryConfig:
        _registry_config_validator.check(data)
        return RegistryConfig(
            host=data["host"], email=data["email"], is_secure=data["is_secure"]
        )

    def storage_config(self, data: Dict[str, str]) -> StorageConfig:
        _storage_config_validator.check(data)
        return StorageConfig(
            type=StorageType(data["type"]),
            container_mount_path=PurePath(data["container_mount_path"]),
            host_mount_path=self._optional_pure_path(data.get("host_mount_path")),
            nfs_server=data.get("nfs_server"),
            nfs_export_path=self._optional_pure_path(data.get("nfs_export_path")),
        )

    def _optional_pure_path(self, path: Optional[str]) -> Optional[PurePath]:
        return None if path is None else PurePath(path)

    def _gpu_model(self, gpu_model_id: Optional[str]) -> Optional[GPUModel]:
        return (
            None
            if gpu_model_id is None
            else GKEGPUModels.find_model_by_id(gpu_model_id)
        )
