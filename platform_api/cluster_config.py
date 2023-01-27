from collections.abc import Sequence
from dataclasses import dataclass
from datetime import time, tzinfo
from typing import Optional
from zoneinfo import ZoneInfo

from yarl import URL

from .resource import Preset, ResourcePoolType, TPUResource

UTC = ZoneInfo("UTC")


@dataclass(frozen=True)
class EnergySchedulePeriod:
    # ISO 8601 weekday number (1-7)
    weekday: int
    start_time: time
    end_time: time

    @classmethod
    def create_full_day(
        cls, *, weekday: int, timezone: tzinfo
    ) -> "EnergySchedulePeriod":
        return cls(
            weekday=weekday,
            start_time=time.min.replace(tzinfo=timezone),
            end_time=time.max.replace(tzinfo=timezone),
        )


@dataclass(frozen=True)
class EnergySchedule:
    name: str
    periods: Sequence[EnergySchedulePeriod] = ()

    @classmethod
    def create_default(cls, *, timezone: tzinfo) -> "EnergySchedule":
        return cls(
            name="default",
            periods=[
                EnergySchedulePeriod.create_full_day(weekday=weekday, timezone=timezone)
                for weekday in range(1, 8)
            ],
        )


@dataclass(frozen=True)
class EnergyConfig:
    schedules: Sequence[EnergySchedule] = (EnergySchedule.create_default(timezone=UTC),)


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
    def allow_scheduler_enabled_job(self) -> bool:
        for preset in self.presets:
            if preset.scheduler_enabled:
                return True
        return False

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
    timezone: tzinfo = UTC
    energy: EnergyConfig = EnergyConfig()
