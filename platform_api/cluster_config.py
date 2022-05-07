from collections.abc import Sequence
from dataclasses import dataclass
from typing import Optional

from yarl import URL

from .resource import Preset, ResourcePoolType, TPUResource


@dataclass(frozen=True)
class OrchestratorConfig:
    jobs_domain_name_template: str
    jobs_internal_domain_name_template: str

    resource_pool_types: Sequence[ResourcePoolType]
    presets: Sequence[Preset]

    is_http_ingress_secure: bool = False

    job_schedule_timeout: float = 3 * 60
    job_schedule_scaleup_timeout: float = 15 * 60

    allow_privileged_mode: bool = False
    allow_job_priority: bool = False

    @property
    def tpu_resources(self) -> Sequence[TPUResource]:
        return tuple(
            resource.tpu for resource in self.resource_pool_types if resource.tpu
        )

    @property
    def tpu_ipv4_cidr_block(self) -> Optional[str]:
        tpus = self.tpu_resources
        if not tpus:
            return None
        return tpus[0].ipv4_cidr_block

    @property
    def has_scheduler_enabled_presets(self) -> bool:
        for preset in self.presets:
            if preset.scheduler_enabled:
                return True
        return False


@dataclass(frozen=True)
class IngressConfig:
    registry_url: URL
    storage_url: URL
    monitoring_url: URL
    secrets_url: URL
    metrics_url: URL
    disks_url: URL
    buckets_url: URL

    @property
    def registry_host(self) -> str:
        """Returns registry hostname with port (if specified)"""
        port = self.registry_url.explicit_port  # type: ignore
        suffix = f":{port}" if port is not None else ""
        return f"{self.registry_url.host}{suffix}"


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    orchestrator: OrchestratorConfig
    ingress: IngressConfig
