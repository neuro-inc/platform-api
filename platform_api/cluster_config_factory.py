import logging
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Optional

import trafaret as t
from yarl import URL

from .cluster_config import ClusterConfig, IngressConfig, OrchestratorConfig
from .resource import DEFAULT_PRESETS, Preset, ResourcePoolType, TPUPreset, TPUResource


_cluster_config_validator = t.Dict({"name": t.String}).allow_extra("*")


class ClusterConfigFactory:
    def create_cluster_configs(
        self, payload: Sequence[dict[str, Any]]
    ) -> Sequence[ClusterConfig]:
        configs = (self.create_cluster_config(p) for p in payload)
        return [c for c in configs if c]

    def create_cluster_config(self, payload: dict[str, Any]) -> Optional[ClusterConfig]:
        try:
            _cluster_config_validator.check(payload)
            return ClusterConfig(
                name=payload["name"],
                orchestrator=self._create_orchestrator_config(payload),
                ingress=self._create_ingress_config(payload),
            )
        except t.DataError as err:
            logging.warning(f"failed to parse cluster config: {err}")
            return None

    def _create_ingress_config(self, payload: dict[str, Any]) -> IngressConfig:
        return IngressConfig(
            registry_url=URL(payload["registry"]["url"]),
            storage_url=URL(payload["storage"]["url"]),
            blob_storage_url=URL(payload["blob_storage"]["url"]),
            monitoring_url=URL(payload["monitoring"]["url"]),
            secrets_url=URL(payload["secrets"]["url"]),
            metrics_url=URL(payload["metrics"]["url"]),
            disks_url=URL(payload["disks"]["url"]),
            buckets_url=URL(payload["buckets"]["url"]),
        )

    def _create_presets(self, payload: dict[str, Any]) -> list[Preset]:
        result = []
        for preset in payload.get("resource_presets", []):
            result.append(
                Preset(
                    name=preset["name"],
                    credits_per_hour=Decimal(preset["credits_per_hour"]),
                    cpu=preset.get("cpu") or payload["cpu"],
                    memory_mb=preset.get("memory_mb"),
                    scheduler_enabled=preset.get("scheduler_enabled")
                    or preset.get("is_preemptible", False),
                    preemptible_node=preset.get("preemptible_node")
                    or preset.get("is_preemptible_node_required", False),
                    gpu=preset.get("gpu"),
                    gpu_model=preset.get("gpu_model"),
                    tpu=self._create_tpu_preset(preset.get("tpu")),
                )
            )
        return result

    def _create_orchestrator_config(
        self, payload: dict[str, Any]
    ) -> OrchestratorConfig:
        orchestrator = payload["orchestrator"]
        presets = self._create_presets(orchestrator)
        return OrchestratorConfig(
            is_http_ingress_secure=orchestrator["is_http_ingress_secure"],
            jobs_domain_name_template=orchestrator["job_hostname_template"],
            jobs_internal_domain_name_template=orchestrator.get(
                "job_internal_hostname_template", ""
            ),
            resource_pool_types=[
                self._create_resource_pool_type(r)
                for r in orchestrator["resource_pool_types"]
            ],
            presets=presets or DEFAULT_PRESETS,
            job_schedule_timeout=orchestrator.get(
                "job_schedule_timeout_s", OrchestratorConfig.job_schedule_timeout
            ),
            job_schedule_scaleup_timeout=orchestrator.get(
                "job_schedule_scale_up_timeout_s",
                OrchestratorConfig.job_schedule_scaleup_timeout,
            ),
            allow_privileged_mode=orchestrator.get(
                "allow_privileged_mode",
                OrchestratorConfig.allow_privileged_mode,
            ),
        )

    def _create_tpu_preset(
        self, payload: Optional[dict[str, Any]]
    ) -> Optional[TPUPreset]:
        if not payload:
            return None

        return TPUPreset(
            type=payload["type"], software_version=payload["software_version"]
        )

    def _create_resource_pool_type(self, payload: dict[str, Any]) -> ResourcePoolType:
        cpu = payload.get("cpu")
        memory_mb = payload.get("memory_mb")
        return ResourcePoolType(
            name=payload["name"],
            gpu=payload.get("gpu"),
            gpu_model=payload.get("gpu_model"),
            is_preemptible=payload.get("is_preemptible"),
            cpu=cpu,
            available_cpu=payload.get("available_cpu") or cpu,
            memory_mb=payload.get("memory_mb"),
            available_memory_mb=payload.get("available_memory_mb") or memory_mb,
            disk_gb=payload.get("disk_size_gb"),
            min_size=payload.get("min_size"),
            max_size=payload.get("max_size"),
            tpu=self._create_tpu_resource(payload.get("tpu")),
        )

    def _create_tpu_resource(
        self, payload: Optional[dict[str, Any]]
    ) -> Optional[TPUResource]:
        if not payload:
            return None

        return TPUResource(
            ipv4_cidr_block=payload.get("ipv4_cidr_block", TPUResource.ipv4_cidr_block),
            types=tuple(payload["types"]),
            software_versions=tuple(payload["software_versions"]),
        )
