import logging
from collections.abc import Sequence
from datetime import time, tzinfo
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

import trafaret as t
from yarl import URL

from .cluster_config import (
    DEFAULT_ENERGY_SCHEDULE_NAME,
    ClusterConfig,
    EnergyConfig,
    EnergySchedule,
    EnergySchedulePeriod,
    IngressConfig,
    OrchestratorConfig,
)
from .resource import Preset, ResourcePoolType, TPUPreset, TPUResource

_cluster_config_validator = t.Dict({"name": t.String}).allow_extra("*")


def _get_memory_with_deprecated_mb(data: Any, key: str) -> Optional[int]:
    if key in data:
        return data[key]
    mb_key = key + "_mb"
    if mb_key in data:
        return data[mb_key] * 2**20
    return None


class ClusterConfigFactory:
    def create_cluster_configs(
        self, payload: Sequence[dict[str, Any]]
    ) -> Sequence[ClusterConfig]:
        configs = (self.create_cluster_config(p) for p in payload)
        return [c for c in configs if c]

    def create_cluster_config(self, payload: dict[str, Any]) -> Optional[ClusterConfig]:
        try:
            _cluster_config_validator.check(payload)
            timezone = self._create_timezone(payload.get("timezone"))
            return ClusterConfig(
                name=payload["name"],
                orchestrator=self._create_orchestrator_config(payload),
                ingress=self._create_ingress_config(payload),
                timezone=timezone,
                energy=self._create_energy_config(payload, timezone=timezone),
            )
        except t.DataError as err:
            logging.warning(f"failed to parse cluster config: {err}")
            return None

    def _create_ingress_config(self, payload: dict[str, Any]) -> IngressConfig:
        return IngressConfig(
            registry_url=URL(payload["registry"]["url"]),
            storage_url=URL(payload["storage"]["url"]),
            monitoring_url=URL(payload["monitoring"]["url"]),
            secrets_url=URL(payload["secrets"]["url"]),
            metrics_url=URL(payload["metrics"]["url"]),
            disks_url=URL(payload["disks"]["url"]),
            buckets_url=URL(payload["buckets"]["url"]),
        )

    def _create_presets(self, payload: dict[str, Any]) -> list[Preset]:
        result = []
        for preset in payload.get("resource_presets", []):
            memory = _get_memory_with_deprecated_mb(preset, "memory")
            if memory is None:
                raise ValueError("memory is not set for resource preset")
            result.append(
                Preset(
                    name=preset["name"],
                    credits_per_hour=Decimal(preset["credits_per_hour"]),
                    cpu=preset.get("cpu") or payload["cpu"],
                    memory=memory,
                    scheduler_enabled=preset.get("scheduler_enabled")
                    or preset.get("is_preemptible", False),
                    preemptible_node=preset.get("preemptible_node")
                    or preset.get("is_preemptible_node_required", False),
                    nvidia_gpu=preset.get("nvidia_gpu"),
                    amd_gpu=preset.get("amd_gpu"),
                    gpu_model=preset.get("gpu_model"),
                    tpu=self._create_tpu_preset(preset.get("tpu")),
                    is_external_job=preset.get("is_external_job", False),
                )
            )
        return result

    def _create_orchestrator_config(
        self, payload: dict[str, Any]
    ) -> OrchestratorConfig:
        orchestrator = payload["orchestrator"]
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
            presets=self._create_presets(orchestrator),
            job_schedule_timeout=orchestrator.get(
                "job_schedule_timeout_s", OrchestratorConfig.job_schedule_timeout
            ),
            job_schedule_scaleup_timeout=orchestrator.get(
                "job_schedule_scale_up_timeout_s",
                OrchestratorConfig.job_schedule_scaleup_timeout,
            ),
            allow_privileged_mode=orchestrator.get(
                "allow_privileged_mode", OrchestratorConfig.allow_privileged_mode
            ),
            allow_job_priority=orchestrator.get(
                "allow_job_priority", OrchestratorConfig.allow_job_priority
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

        memory = _get_memory_with_deprecated_mb(payload, "memory")
        available_memory = _get_memory_with_deprecated_mb(payload, "available_memory")
        return ResourcePoolType(
            name=payload["name"],
            nvidia_gpu=payload.get("nvidia_gpu"),
            amd_gpu=payload.get("amd_gpu"),
            is_preemptible=payload.get("is_preemptible"),
            cpu=cpu,
            available_cpu=payload.get("available_cpu") or cpu,
            memory=memory,
            available_memory=available_memory or memory,
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

    def _create_timezone(self, name: Optional[str]) -> tzinfo:
        if not name:
            return ClusterConfig.timezone
        try:
            return ZoneInfo(name)
        except Exception:
            raise ValueError(f"invalid timezone: {name}")

    def _create_energy_schedule_period(
        self, payload: dict[str, Any], *, timezone: tzinfo
    ) -> EnergySchedulePeriod:
        start_time = time.fromisoformat(payload["start_time"]).replace(tzinfo=timezone)
        end_time = time.fromisoformat(payload["end_time"]).replace(tzinfo=timezone)
        return EnergySchedulePeriod(
            weekday=payload["weekday"],
            start_time=start_time,
            end_time=end_time,
        )

    def _create_energy_schedule(
        self, payload: dict[str, Any], timezone: tzinfo
    ) -> EnergySchedule:
        return EnergySchedule(
            name=payload["name"],
            periods=[
                self._create_energy_schedule_period(p, timezone=timezone)
                for p in payload["periods"]
            ],
        )

    def _create_energy_config(
        self, payload: dict[str, Any], *, timezone: tzinfo
    ) -> EnergyConfig:
        schedules = {
            schedule.name: schedule
            for s in payload.get("energy", {}).get("schedules", [])
            if (schedule := self._create_energy_schedule(s, timezone=timezone))
        }
        schedules[DEFAULT_ENERGY_SCHEDULE_NAME] = EnergySchedule.create_default(
            timezone=timezone
        )
        return EnergyConfig(schedules=list(schedules.values()))
