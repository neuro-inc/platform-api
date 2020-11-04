import logging
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence

import trafaret as t
from yarl import URL

from .cluster_config import (
    ClusterConfig,
    IngressConfig,
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from .orchestrator.kube_config import KubeClientAuthType, KubeConfig
from .resource import Preset, ResourcePoolType, TPUPreset, TPUResource


_cluster_config_validator = t.Dict({"name": t.String}).allow_extra("*")


class ClusterConfigFactory:
    def create_cluster_configs(
        self,
        payload: Sequence[Dict[str, Any]],
        *,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
        registry_username: str,
        registry_password: str,
    ) -> Sequence[ClusterConfig]:
        configs = (
            self._create_cluster_config(
                p,
                jobs_ingress_class=jobs_ingress_class,
                jobs_ingress_oauth_url=jobs_ingress_oauth_url,
                registry_username=registry_username,
                registry_password=registry_password,
            )
            for p in payload
        )
        return [c for c in configs if c]

    def _create_cluster_config(
        self,
        payload: Dict[str, Any],
        *,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
        registry_username: str,
        registry_password: str,
    ) -> Optional[ClusterConfig]:
        try:
            _cluster_config_validator.check(payload)
            return ClusterConfig(
                name=payload["name"],
                storage=self._create_storage_config(payload),
                registry=self._create_registry_config(
                    payload,
                    registry_username=registry_username,
                    registry_password=registry_password,
                ),
                orchestrator=self._create_orchestrator_config(
                    payload,
                    jobs_ingress_class=jobs_ingress_class,
                    jobs_ingress_oauth_url=jobs_ingress_oauth_url,
                ),
                ingress=self._create_ingress_config(payload),
            )
        except t.DataError as err:
            logging.warning(f"failed to parse cluster config: {err}")
            return None

    def _create_ingress_config(self, payload: Dict[str, Any]) -> IngressConfig:
        return IngressConfig(
            storage_url=URL(payload["storage"]["url"]),
            monitoring_url=URL(payload["monitoring"]["url"]),
            secrets_url=URL(payload["secrets"]["url"]),
            metrics_url=URL(payload["metrics"]["url"]),
        )

    def _create_presets(self, payload: Dict[str, Any]) -> List[Preset]:
        result = []
        for preset in payload.get("presets", []):
            result.append(
                Preset(
                    name=preset["name"],
                    cpu=preset.get("cpu") or payload["cpu"],
                    memory_mb=preset.get("memory_mb") or payload["memory_mb"],
                    is_preemptible=payload.get("is_preemptible", False),
                    gpu=preset.get("gpu") or payload.get("gpu"),
                    gpu_model=preset.get("gpu_model") or payload.get("gpu_model"),
                    # TPU presets do not inherit their pool type resources,
                    # because CPU pool types may or may not be used to run TPU
                    # workloads.
                    tpu=self._create_tpu_preset(preset.get("tpu")),
                )
            )
        return result

    def _create_orchestrator_config(
        self,
        payload: Dict[str, Any],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> OrchestratorConfig:
        orchestrator = payload["orchestrator"]
        kube = orchestrator["kubernetes"]
        return KubeConfig(
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
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            node_label_gpu=kube["node_label_gpu"],
            node_label_preemptible=kube["node_label_preemptible"],
            node_label_job=kube.get("node_label_job"),
            jobs_pod_priority_class_name=kube.get("job_pod_priority_class_name"),
        )

    def _create_tpu_preset(
        self, payload: Optional[Dict[str, Any]]
    ) -> Optional[TPUPreset]:
        if not payload:
            return None

        return TPUPreset(
            type=payload["type"], software_version=payload["software_version"]
        )

    def _create_resource_pool_type(self, payload: Dict[str, Any]) -> ResourcePoolType:
        return ResourcePoolType(
            gpu=payload.get("gpu"),
            gpu_model=payload.get("gpu_model"),
            is_preemptible=payload.get("is_preemptible"),
            cpu=payload.get("cpu"),
            memory_mb=payload.get("memory_mb"),
            disk_gb=payload.get("disk_gb"),
            min_size=payload.get("min_size"),
            max_size=payload.get("max_size"),
            presets=self._create_presets(payload),
            tpu=self._create_tpu_resource(payload.get("tpu")),
        )

    def _create_tpu_resource(
        self, payload: Optional[Dict[str, Any]]
    ) -> Optional[TPUResource]:
        if not payload:
            return None

        return TPUResource(
            ipv4_cidr_block=payload.get("ipv4_cidr_block", TPUResource.ipv4_cidr_block),
            types=tuple(payload["types"]),
            software_versions=tuple(payload["software_versions"]),
        )

    def _create_registry_config(
        self, payload: Dict[str, Any], registry_username: str, registry_password: str
    ) -> RegistryConfig:
        registry = payload["registry"]
        return RegistryConfig(
            url=URL(registry["url"]),
            email=registry["email"],
            username=registry_username,
            password=registry_password,
        )

    def _create_storage_config(self, payload: Dict[str, Any]) -> StorageConfig:
        storage = payload["storage"]
        if storage.get("nfs"):
            return StorageConfig.create_nfs(
                nfs_server=storage["nfs"]["server"],
                nfs_export_path=PurePath(storage["nfs"]["export_path"]),
            )
        if storage.get("pvc"):
            return StorageConfig.create_pvc(pvc_name=storage["pvc"]["name"])
        return StorageConfig.create_host(
            host_mount_path=PurePath(storage["host"]["mount_path"])
        )

    def _create_optional_path(self, path: Optional[str]) -> Optional[PurePath]:
        return PurePath(path) if path else None
