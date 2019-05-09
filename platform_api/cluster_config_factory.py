from pathlib import PurePath
from typing import Any, Dict, Optional, Sequence

import trafaret as t
from yarl import URL

from .cluster_config import (
    ClusterConfig,
    IngressConfig,
    LoggingConfig,
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from .elasticsearch import ElasticsearchConfig
from .orchestrator.kube_client import KubeClientAuthType
from .orchestrator.kube_orchestrator import KubeConfig
from .resource import GKEGPUModels, GPUModel, ResourcePoolType


_OptionalString = t.String | t.Null

_storage_config_validator = t.Dict(
    {
        "url": t.String,
        "host": t.Null | t.Dict({"mount_path": t.String}),
        "nfs": t.Null | t.Dict({"server": t.String, "export_path": t.String}),
    }
)

_registry_config_validator = t.Dict({"url": t.String, "email": t.Email})

_orchestrator_config_validator = t.Dict(
    {
        "kubernetes": t.Dict(
            {
                "url": t.String,
                "ca_data": t.String,
                "auth_type": t.Enum(*[s.value for s in KubeClientAuthType]),
                "token": _OptionalString,
                "namespace": t.String,
                "jobs_ingress_name": t.String,
                "jobs_ingress_auth_name": t.String,
                "node_label_gpu": t.String,
                "node_label_preemptible": t.String,
            }
        ),
        "is_http_ingress_secure": t.Bool,
        "job_domain_name_template": t.String,
        "named_job_domain_name_template": t.String,
        "resource_pool_types": t.List(
            t.Dict(
                {
                    "gpu": t.Int,
                    "gpu_model": t.Null | t.Enum(*[m.value.id for m in GKEGPUModels]),
                }
            )
        ),
    }
)

_monitoring_config_validator = t.Dict(
    {
        "url": t.String,
        "elasticsearch": t.Dict(
            {
                "hosts": t.List(t.String),
                "user": _OptionalString,
                "password": _OptionalString,
            }
        ),
    }
)

_ssh_config_validator = t.Dict({"server": t.String})

_cluster_config_validator = t.Dict(
    {
        "name": t.String,
        "storage": _storage_config_validator,
        "registry": _registry_config_validator,
        "orchestrator": _orchestrator_config_validator,
        "ssh": _ssh_config_validator,
        "monitoring": _monitoring_config_validator,
    }
)


class ClusterConfigFactory:
    def cluster_configs(
        self, payload: Sequence[Dict[str, Any]]
    ) -> Sequence[ClusterConfig]:
        return [self._cluster_config(p) for p in payload]

    def _cluster_config(self, payload: Dict[str, Any]) -> ClusterConfig:
        _cluster_config_validator.check(payload)
        return ClusterConfig(
            name=payload["name"],
            storage=self._storage_config(payload),
            registry=self._registry_config(payload),
            orchestrator=self._orchestrator_config(payload),
            logging=self._logging_config(payload),
            ingress=self._ingress_config(payload),
        )

    def _ingress_config(self, payload: Dict[str, Any]) -> IngressConfig:
        return IngressConfig(
            storage_url=URL(payload["storage"]["url"]),
            monitoring_url=URL(payload["monitoring"]["url"]),
            users_url=URL(),
        )

    def _logging_config(self, payload: Dict[str, Any]) -> LoggingConfig:
        monitoring = payload["monitoring"]
        return LoggingConfig(
            elasticsearch=self._elasticsearch_config(monitoring["elasticsearch"])
        )

    def _elasticsearch_config(self, payload: Dict[str, Any]) -> ElasticsearchConfig:
        return ElasticsearchConfig(
            hosts=payload["hosts"],
            user=payload.get("user"),
            password=payload.get("password"),
        )

    def _orchestrator_config(self, payload: Dict[str, Any]) -> OrchestratorConfig:
        orchestrator = payload["orchestrator"]
        kube = orchestrator["kubernetes"]
        ssh = payload["ssh"]
        return KubeConfig(
            ssh_domain_name=ssh["server"],
            ssh_auth_domain_name=ssh["server"],
            is_http_ingress_secure=orchestrator["is_http_ingress_secure"],
            jobs_domain_name_template=orchestrator["job_domain_name_template"],
            named_jobs_domain_name_template=orchestrator[
                "named_job_domain_name_template"
            ],
            resource_pool_types=[
                self._resource_pool_type(r) for r in orchestrator["resource_pool_types"]
            ],
            endpoint_url=kube["url"],
            cert_authority_path=None,
            auth_type=KubeClientAuthType(kube["auth_type"]),
            auth_cert_path=None,
            auth_cert_key_path=None,
            token_path=None,
            namespace=kube["namespace"],
            jobs_ingress_name=kube["jobs_ingress_name"],
            jobs_ingress_auth_name=kube["jobs_ingress_auth_name"],
            node_label_gpu=kube["node_label_gpu"],
            node_label_preemptible=kube["node_label_preemptible"],
        )

    def _resource_pool_type(self, payload: Dict[str, Any]) -> ResourcePoolType:
        return ResourcePoolType(
            gpu=payload["gpu"], gpu_model=self._gpu_model(payload.get("gpu_model"))
        )

    def _gpu_model(self, gpu_model_id: Optional[str]) -> Optional[GPUModel]:
        return (
            None
            if gpu_model_id is None
            else GKEGPUModels.find_model_by_id(gpu_model_id)
        )

    def _registry_config(self, payload: Dict[str, Any]) -> RegistryConfig:
        registry = payload["registry"]
        return RegistryConfig(host=registry["url"], email=registry["email"])

    def _storage_config(self, payload: Dict[str, Any]) -> StorageConfig:
        storage = payload["storage"]
        if storage["nfs"]:
            host = storage["host"] or {}
            return StorageConfig.create_nfs(
                host_mount_path=self._optional_path(host.get("mount_path")),
                nfs_server=storage["nfs"]["server"],
                nfs_export_path=PurePath(storage["nfs"]["export_path"]),
            )
        return StorageConfig.create_host(
            host_mount_path=PurePath(storage["host"]["mount_path"])
        )

    def _optional_path(self, path: Optional[str]) -> Optional[PurePath]:
        return PurePath(path) if path else None
