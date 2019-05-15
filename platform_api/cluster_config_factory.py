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


_nfs_storage_cfg_validator = t.Dict(
    {"url": t.String, "host": t.Dict({"mount_path": t.String})}
)

_host_storage_cfg_validator = t.Dict(
    {"url": t.String, "nfs": t.Dict({"server": t.String, "export_path": t.String})}
)

_storage_config_validator = _nfs_storage_cfg_validator | _host_storage_cfg_validator

_registry_config_validator = t.Dict({"url": t.String, "email": t.Email})

_orchestrator_config_validator = t.Dict(
    {
        "kubernetes": t.Dict(
            {
                "url": t.String,
                "ca_data": t.String,
                "auth_type": t.Enum(*[s.value for s in KubeClientAuthType]),
                "token": t.String | t.Null,
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
                {"gpu": t.Int, "gpu_model": t.Enum(*[m.value.id for m in GKEGPUModels])}
            )
            | t.Dict({"gpu": t.Int})
            | t.Dict({})
        ),
    }
)

_monitoring_config_validator = t.Dict(
    {
        "url": t.String,
        "elasticsearch": t.Dict({"hosts": t.List(t.String)})
        | t.Dict({"hosts": t.List(t.String), "user": t.String, "password": t.String}),
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
    def create_cluster_configs(
        self, payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> Sequence[ClusterConfig]:
        return [self._create_cluster_config(p, users_url) for p in payload]

    def _create_cluster_config(
        self, payload: Dict[str, Any], users_url: URL
    ) -> ClusterConfig:
        _cluster_config_validator.check(payload)
        return ClusterConfig(
            name=payload["name"],
            storage=self._create_storage_config(payload),
            registry=self._create_registry_config(payload),
            orchestrator=self._create_orchestrator_config(payload),
            logging=self._create_logging_config(payload),
            ingress=self._create_ingress_config(payload, users_url),
        )

    def _create_ingress_config(
        self, payload: Dict[str, Any], users_url: URL
    ) -> IngressConfig:
        return IngressConfig(
            storage_url=URL(payload["storage"]["url"]),
            monitoring_url=URL(payload["monitoring"]["url"]),
            users_url=users_url,
        )

    def _create_logging_config(self, payload: Dict[str, Any]) -> LoggingConfig:
        monitoring = payload["monitoring"]
        return LoggingConfig(
            elasticsearch=self._create_elasticsearch_config(monitoring["elasticsearch"])
        )

    def _create_elasticsearch_config(
        self, payload: Dict[str, Any]
    ) -> ElasticsearchConfig:
        return ElasticsearchConfig(
            hosts=payload["hosts"],
            user=payload.get("user"),
            password=payload.get("password"),
        )

    def _create_orchestrator_config(
        self, payload: Dict[str, Any]
    ) -> OrchestratorConfig:
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
                self._create_resource_pool_type(r)
                for r in orchestrator["resource_pool_types"]
            ],
            endpoint_url=kube["url"],
            cert_authority_data_pem=kube["ca_data"],
            cert_authority_path=None,  # not initialized, see `cert_authority_data_pem`
            auth_type=KubeClientAuthType(kube["auth_type"]),
            auth_cert_path=None,
            auth_cert_key_path=None,
            token=kube["token"],
            token_path=None,  # not initialized, see field `token`
            namespace=kube["namespace"],
            jobs_ingress_name=kube["jobs_ingress_name"],
            jobs_ingress_auth_name=kube["jobs_ingress_auth_name"],
            node_label_gpu=kube["node_label_gpu"],
            node_label_preemptible=kube["node_label_preemptible"],
        )

    def _create_resource_pool_type(self, payload: Dict[str, Any]) -> ResourcePoolType:
        return ResourcePoolType(
            gpu=payload.get("gpu"),
            gpu_model=self._create_gpu_model(payload.get("gpu_model")),
        )

    def _create_gpu_model(self, gpu_model_id: Optional[str]) -> Optional[GPUModel]:
        return (
            None
            if gpu_model_id is None
            else GKEGPUModels.find_model_by_id(gpu_model_id)
        )

    def _create_registry_config(self, payload: Dict[str, Any]) -> RegistryConfig:
        registry = payload["registry"]
        return RegistryConfig(host=registry["url"], email=registry["email"])

    def _create_storage_config(self, payload: Dict[str, Any]) -> StorageConfig:
        storage = payload["storage"]
        if storage.get("nfs"):
            return StorageConfig.create_nfs(
                nfs_server=storage["nfs"]["server"],
                nfs_export_path=PurePath(storage["nfs"]["export_path"]),
            )
        return StorageConfig.create_host(
            host_mount_path=PurePath(storage["host"]["mount_path"])
        )

    def _create_optional_path(self, path: Optional[str]) -> Optional[PurePath]:
        return PurePath(path) if path else None
