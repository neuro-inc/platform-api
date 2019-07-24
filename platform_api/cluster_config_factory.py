import logging
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence

import trafaret as t
from yarl import URL

from platform_api.resource import DEFAULT_PRESETS, Preset

from .cluster_config import (
    ClusterConfig,
    IngressConfig,
    LoggingConfig,
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from .elasticsearch import ElasticsearchConfig
from .orchestrator.kube_config import KubeClientAuthType, KubeConfig
from .resource import GKEGPUModels, GPUModel, ResourcePoolType


_cluster_config_validator = t.Dict({"name": t.String}).allow_extra("*")


class ClusterConfigFactory:
    def create_cluster_configs(
        self, payload: Sequence[Dict[str, Any]], *, users_url: URL
    ) -> Sequence[ClusterConfig]:
        configs = (self._create_cluster_config(p, users_url=users_url) for p in payload)
        return [c for c in configs if c]

    def _create_cluster_config(
        self, payload: Dict[str, Any], *, users_url: URL
    ) -> Optional[ClusterConfig]:
        try:
            _cluster_config_validator.check(payload)
            return ClusterConfig(
                name=payload["name"],
                storage=self._create_storage_config(payload),
                registry=self._create_registry_config(payload),
                orchestrator=self._create_orchestrator_config(payload),
                logging=self._create_logging_config(payload),
                ingress=self._create_ingress_config(payload, users_url),
            )
        except t.DataError as err:
            logging.warning(f"failed to parse cluster config: {err}")
            return None

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

    def _create_presets(self, payload: Dict[str, Any]) -> List[Preset]:
        result = []
        for preset in payload.get("presets", []):
            result.append(
                Preset(
                    name=preset["name"],
                    cpu=preset.get("cpu") or payload["cpu"],
                    memory_mb=preset.get("memory_mb") or payload["memory_mb"],
                    gpu=preset.get("gpu") or payload.get("gpu"),
                    gpu_model=self._create_gpu_model(preset.get("gpu_model"))
                    or self._create_gpu_model(payload.get("gpu_model")),
                )
            )
        # default fallback
        if len(result) == 0:
            return DEFAULT_PRESETS
        return result

    def _create_elasticsearch_config(
        self, payload: Dict[str, Any]
    ) -> ElasticsearchConfig:
        return ElasticsearchConfig(
            hosts=payload["hosts"],
            user=payload.get("username"),
            password=payload.get("password"),
        )

    def _create_orchestrator_config(
        self, payload: Dict[str, Any]
    ) -> OrchestratorConfig:
        orchestrator = payload["orchestrator"]
        kube = orchestrator["kubernetes"]
        ssh = payload["ssh"]
        return KubeConfig(
            ssh_auth_domain_name=ssh["server"],
            is_http_ingress_secure=orchestrator["is_http_ingress_secure"],
            jobs_domain_name_template=orchestrator["job_hostname_template"],
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
            is_preemptible=payload.get("is_preemptible"),
            cpu=payload.get("cpu"),
            memory_mb=payload.get("memory_mb"),
            disk_gb=payload.get("disk_gb"),
            min_size=payload.get("min_size"),
            max_size=payload.get("max_size"),
            presets=self._create_presets(payload),
        )

    def _create_gpu_model(self, gpu_model_id: Optional[str]) -> Optional[GPUModel]:
        return (
            None
            if gpu_model_id is None
            else GKEGPUModels.find_model_by_id(gpu_model_id)
        )

    def _create_registry_config(self, payload: Dict[str, Any]) -> RegistryConfig:
        registry = payload["registry"]
        return RegistryConfig(url=URL(registry["url"]), email=registry["email"])

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
