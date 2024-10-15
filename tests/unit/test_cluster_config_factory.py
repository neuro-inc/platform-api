from collections.abc import Sequence
from datetime import datetime, time
from decimal import Decimal
from typing import Any
from unittest import mock
from zoneinfo import ZoneInfo

import pytest
from yarl import URL

from platform_api.cluster_config import (
    UTC,
    EnergyConfig,
    EnergySchedule,
    EnergySchedulePeriod,
    VolumeConfig,
)
from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.resource import GKEGPUModels, Preset, TPUPreset, TPUResource


@pytest.fixture
def storage_payload() -> dict[str, Any]:
    return {
        "storage": {
            "url": "https://dev.neu.ro/api/v1/storage",
            "volumes": [
                {
                    "name": "default",
                },
                {
                    "name": "org",
                    "path": "/org",
                    "credits_per_hour_per_gb": "100",
                },
            ],
        }
    }


@pytest.fixture
def clusters_payload(storage_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
            "location": "eu-west-4",
            "logo_url": "https://logo.url",
            "registry": {
                "url": "https://registry-dev.neu.ro",
                "email": "registry@neuromation.io",
            },
            "orchestrator": {
                "job_hostname_template": "{job_id}.jobs.neu.ro",
                "job_internal_hostname_template": "{job_id}.default",
                "job_schedule_timeout_s": 60,
                "job_schedule_scale_up_timeout_s": 120,
                "is_http_ingress_secure": True,
                "allow_job_priority": True,
                "resource_presets": [
                    {
                        "name": "cpu-small",
                        "credits_per_hour": "10",
                        "cpu": 1,
                        "memory": 2048 * 10**6,
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "cpu-large",
                        "credits_per_hour": "10",
                        "cpu": 7,
                        "memory": 49152 * 10**6,
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "tpu",
                        "credits_per_hour": "10",
                        "cpu": 7,
                        "memory": 49152 * 10**6,
                        "tpu": {"type": "v2-8", "software_version": "1.14"},
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "gpu-small-p",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "nvidia_gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "gpu-small",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "nvidia_gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "gpu-large-p",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "nvidia_gpu": 1,
                        "amd_gpu": 1,
                        "intel_gpu": 1,
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                    {
                        "name": "gpu-large",
                        "credits_per_hour": "10",
                        "cpu": 0.1,
                        "memory": 52224 * 10**6,
                        "nvidia_gpu": 1,
                        "amd_gpu": 1,
                        "intel_gpu": 1,
                        "available_resource_pool_names": ["n1-highmem-8"],
                    },
                ],
                "resource_pool_types": [
                    {
                        "name": "n1-highmem-8",
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory": 53248 * 10**6,
                        "available_memory": 49152 * 10**6,
                        "disk_size": 150 * 10**9,
                        "tpu": {
                            "ipv4_cidr_block": "1.1.1.1/32",
                            "types": ["v2-8", "v3-8"],
                            "software_versions": ["1.13", "1.14"],
                        },
                    },
                    {
                        "name": "n1-highmem-32-preemptible",
                        "is_preemptible": True,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 31.0,
                        "memory": 204800 * 10**6,
                        "disk_size_gb": 150 * 10**9,
                        "nvidia_gpu": 4,
                        "amd_gpu": 4,
                        "intel_gpu": 4,
                    },
                    {
                        "name": "n1-highmem-32",
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 8,
                        "cpu": 32.0,
                        "available_cpu": 31.0,
                        "memory": 212992 * 10**6,
                        "available_memory": 204800 * 10**6,
                        "disk_size_gb": 150 * 10**9,
                        "nvidia_gpu": 4,
                        "amd_gpu": 4,
                        "intel_gpu": 4,
                    },
                    {
                        "name": "n1-highmem-8-preemptible",
                        "is_preemptible": True,
                        "min_size": 0,
                        "max_size": 5,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory": 53248 * 10**6,
                        "available_memory": 49152 * 10**6,
                        "disk_size_gb": 150 * 10**9,
                        "nvidia_gpu": 1,
                        "amd_gpu": 1,
                        "intel_gpu": 1,
                    },
                    {
                        "name": "n1-highmem-8",
                        "is_preemptible": False,
                        "min_size": 0,
                        "max_size": 2,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory": 53248 * 10**6,
                        "available_memory": 49152 * 10**6,
                        "disk_size_gb": 150 * 10**9,
                        "nvidia_gpu": 1,
                        "amd_gpu": 1,
                        "intel_gpu": 1,
                    },
                ],
            },
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
            "secrets": {"url": "https://dev.neu.ro/api/v1/secrets"},
            "metrics": {"url": "https://metrics.dev.neu.ro"},
            "disks": {"url": "https://dev.neu.ro/api/v1/disk"},
            "buckets": {"url": "https://dev.neu.ro/api/v1/buckets"},
            "blob_storage": {"url": "https://dev.neu.ro/api/v1/blob"},
            **storage_payload,
        }
    ]


@pytest.fixture
def users_url() -> URL:
    return URL("https://dev.neu.ro/api/v1/users")


class TestClusterConfigFactory:
    def test_valid_cluster_config(
        self, clusters_payload: Sequence[dict[str, Any]]
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]
        registry_payload = clusters_payload[0]["registry"]
        orchestrator_payload = clusters_payload[0]["orchestrator"]
        monitoring_payload = clusters_payload[0]["monitoring"]
        secrets_payload = clusters_payload[0]["secrets"]
        metrics_payload = clusters_payload[0]["metrics"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload)

        assert len(clusters) == 1

        cluster = clusters[0]

        assert cluster.name == clusters_payload[0]["name"]
        assert cluster.location == clusters_payload[0]["location"]
        assert cluster.logo_url == URL(clusters_payload[0]["logo_url"])

        ingress = cluster.ingress
        assert ingress.registry_url == URL(registry_payload["url"])
        assert ingress.storage_url == URL(storage_payload["url"])
        assert ingress.monitoring_url == URL(monitoring_payload["url"])
        assert ingress.secrets_url == URL(secrets_payload["url"])
        assert ingress.metrics_url == URL(metrics_payload["url"])

        assert ingress.registry_host == "registry-dev.neu.ro"

        orchestrator = cluster.orchestrator

        assert (
            orchestrator.is_http_ingress_secure
            == orchestrator_payload["is_http_ingress_secure"]
        )
        assert (
            orchestrator.jobs_domain_name_template
            == orchestrator_payload["job_hostname_template"]
        )
        assert (
            orchestrator.jobs_internal_domain_name_template
            == orchestrator_payload["job_internal_hostname_template"]
        )
        assert orchestrator.job_schedule_timeout == 60
        assert orchestrator.job_schedule_scaleup_timeout == 120
        assert orchestrator.allow_job_priority

        assert len(orchestrator.resource_pool_types) == 5
        assert orchestrator.resource_pool_types[0].name == "n1-highmem-8"
        assert orchestrator.resource_pool_types[0].cpu == 7.0
        assert orchestrator.resource_pool_types[0].memory == 49152 * 10**6
        assert orchestrator.resource_pool_types[0].disk_size == 150 * 10**9
        assert orchestrator.resource_pool_types[0].nvidia_gpu is None
        assert orchestrator.resource_pool_types[0].tpu == TPUResource(
            ipv4_cidr_block="1.1.1.1/32",
            types=("v2-8", "v3-8"),
            software_versions=("1.13", "1.14"),
        )

        assert orchestrator.resource_pool_types[1].cpu == 31.0
        assert orchestrator.resource_pool_types[1].memory == 204800 * 10**6
        assert orchestrator.resource_pool_types[1].nvidia_gpu == 4
        assert orchestrator.resource_pool_types[1].amd_gpu == 4
        assert orchestrator.resource_pool_types[1].intel_gpu == 4

        assert orchestrator.resource_pool_types[3].nvidia_gpu == 1
        assert orchestrator.resource_pool_types[3].amd_gpu == 1
        assert orchestrator.resource_pool_types[3].intel_gpu == 1

        assert orchestrator.presets is not None
        assert orchestrator.presets[1].cpu == 7.0
        assert orchestrator.presets[1].memory == 49152 * 10**6
        assert orchestrator.presets[1].gpu_model is None
        assert orchestrator.presets[1].available_resource_pool_names == ["n1-highmem-8"]
        assert orchestrator.presets[2] == Preset(
            name="tpu",
            credits_per_hour=Decimal("10"),
            cpu=7.0,
            memory=49152 * 10**6,
            tpu=TPUPreset(type="v2-8", software_version="1.14"),
            available_resource_pool_names=["n1-highmem-8"],
        )

        assert orchestrator.presets[3].nvidia_gpu_model == GKEGPUModels.K80.value.id

        assert orchestrator.presets[4].cpu == 7.0
        assert orchestrator.presets[4].nvidia_gpu == 1
        assert orchestrator.presets[4].nvidia_gpu_model == GKEGPUModels.K80.value.id
        assert orchestrator.presets[4].memory == 52224 * 10**6

        assert orchestrator.presets[6].cpu == 0.1
        assert orchestrator.presets[6].nvidia_gpu == 1
        assert orchestrator.presets[6].amd_gpu == 1
        assert orchestrator.presets[6].intel_gpu == 1

        assert orchestrator.tpu_resources == (
            TPUResource(
                ipv4_cidr_block="1.1.1.1/32",
                types=("v2-8", "v3-8"),
                software_versions=("1.13", "1.14"),
            ),
        )
        assert orchestrator.tpu_ipv4_cidr_block == "1.1.1.1/32"

        assert cluster.timezone == UTC
        assert cluster.energy == EnergyConfig(
            schedules=[EnergySchedule.create_default(timezone=UTC)]
        )

        assert cluster.storage.volumes == [
            VolumeConfig(name="default", path=None, credits_per_hour_per_gb=Decimal(0)),
            VolumeConfig(name="org", path="/org", credits_per_hour_per_gb=Decimal(100)),
        ]

    def test_orchestrator_resource_presets_default(
        self, clusters_payload: Sequence[dict[str, Any]]
    ) -> None:
        factory = ClusterConfigFactory()
        clusters_payload[0]["orchestrator"]["resource_presets"] = [
            {
                "name": "cpu",
                "credits_per_hour": "10",
                "cpu": 1,
                "memory": 2048 * 10**6,
            }
        ]
        clusters = factory.create_cluster_configs(clusters_payload)

        assert clusters[0].orchestrator.presets == [
            Preset(
                name="cpu", credits_per_hour=Decimal("10"), cpu=1, memory=2048 * 10**6
            )
        ]
        assert clusters[0].orchestrator.allow_scheduler_enabled_job is False

    def test_orchestrator_resource_presets_memory_mb(
        self, clusters_payload: Sequence[dict[str, Any]]
    ) -> None:
        factory = ClusterConfigFactory()
        clusters_payload[0]["orchestrator"]["resource_presets"] = [
            {
                "name": "cpu",
                "credits_per_hour": "10",
                "cpu": 1,
                "memory_mb": 2048,
            }
        ]
        clusters = factory.create_cluster_configs(clusters_payload)

        assert clusters[0].orchestrator.presets == [
            Preset(
                name="cpu", credits_per_hour=Decimal("10"), cpu=1, memory=2048 * 2**20
            )
        ]
        assert clusters[0].orchestrator.allow_scheduler_enabled_job is False

    def test_orchestrator_resource_presets_custom(
        self, clusters_payload: Sequence[dict[str, Any]]
    ) -> None:
        factory = ClusterConfigFactory()
        clusters_payload[0]["orchestrator"]["resource_presets"] = [
            {
                "name": "cpu-p",
                "credits_per_hour": "10",
                "cpu": 7,
                "memory": 49152 * 10**6,
                "scheduler_enabled": True,
                "preemptible_node": True,
                "is_external_job": True,
            },
        ]
        clusters = factory.create_cluster_configs(clusters_payload)

        assert clusters[0].orchestrator.presets == [
            Preset(
                name="cpu-p",
                credits_per_hour=Decimal("10"),
                cpu=7,
                memory=49152 * 10**6,
                scheduler_enabled=True,
                preemptible_node=True,
                is_external_job=True,
            ),
        ]
        assert clusters[0].orchestrator.allow_scheduler_enabled_job is True

    def test_orchestrator_job_schedule_settings_default(
        self, clusters_payload: Sequence[dict[str, Any]]
    ) -> None:
        orchestrator = clusters_payload[0]["orchestrator"]
        del orchestrator["job_schedule_timeout_s"]
        del orchestrator["job_schedule_scale_up_timeout_s"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload)

        assert clusters[0].orchestrator.job_schedule_timeout == 180
        assert clusters[0].orchestrator.job_schedule_scaleup_timeout == 900

    def test_factory_skips_invalid_cluster_configs(
        self, clusters_payload: list[dict[str, Any]]
    ) -> None:
        clusters_payload.append({})
        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload)

        assert len(clusters) == 1

    def test_energy(self, clusters_payload: Sequence[dict[str, Any]]) -> None:
        clusters_payload[0]["timezone"] = "Europe/Kyiv"
        clusters_payload[0]["energy"] = {
            "schedules": [
                {
                    "name": "default",
                    "periods": [
                        {
                            "weekday": 1,
                            "start_time": "00:00",
                            "end_time": "06:00",
                        },
                    ],
                },
                {
                    "name": "green",
                    "periods": [
                        {
                            "weekday": 1,
                            "start_time": "00:00",
                            "end_time": "06:00",
                        },
                    ],
                },
            ]
        }
        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload)

        assert clusters[0].timezone == ZoneInfo("Europe/Kyiv")
        assert clusters[0].energy == EnergyConfig(schedules=mock.ANY)
        assert clusters[0].energy.schedules == [
            EnergySchedule.create_default(timezone=ZoneInfo("Europe/Kyiv")),
            EnergySchedule(
                name="green",
                periods=[
                    EnergySchedulePeriod(
                        weekday=1,
                        start_time=time(0, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                        end_time=time(6, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                    )
                ],
            ),
        ]


class TestEnergySchedulePeriod:
    def test__post_init__missing_start_time_tzinfo(self) -> None:
        with pytest.raises(
            ValueError, match="start_time and end_time must have tzinfo"
        ):
            EnergySchedulePeriod(weekday=1, start_time=time(0, 0), end_time=time(6, 0))

    def test__post_init__missing_end_time_tzinfo(self) -> None:
        with pytest.raises(
            ValueError, match="start_time and end_time must have tzinfo"
        ):
            EnergySchedulePeriod(
                weekday=1, start_time=time(0, 0, tzinfo=UTC), end_time=time(6, 0)
            )

    def test__post_init__end_time_before_start_time(self) -> None:
        with pytest.raises(ValueError, match="start_time must be less than end_time"):
            EnergySchedulePeriod(
                weekday=1,
                start_time=time(6, 0, tzinfo=UTC),
                end_time=time(6, 0, tzinfo=UTC),
            )

    def test__post_init__invalid_weekday(self) -> None:
        with pytest.raises(ValueError, match="weekday must be in range 1-7"):
            EnergySchedulePeriod(
                weekday=0,
                start_time=time(0, 0, tzinfo=UTC),
                end_time=time(6, 0, tzinfo=UTC),
            )

    def test__post_init__end_time_00_00_turns_into_time_max(self) -> None:
        period = EnergySchedulePeriod(
            weekday=1,
            start_time=time(0, 0, tzinfo=UTC),
            end_time=time(0, 0, tzinfo=UTC),
        )
        assert period.end_time == time.max.replace(tzinfo=UTC)


class TestEnergyConfig:
    def test_get_schedule(self) -> None:
        config = EnergyConfig(
            schedules=[
                EnergySchedule.create_default(timezone=UTC),
                EnergySchedule(name="green"),
            ]
        )
        assert config.get_schedule("default") == EnergySchedule.create_default(
            timezone=UTC
        )
        assert config.get_schedule("green") == EnergySchedule(name="green")
        assert config.get_schedule("unknown") == EnergySchedule.create_default(
            timezone=UTC
        )


class TestEnergySchedule:
    @pytest.mark.parametrize(
        "current_time, result",
        [
            # Monday
            (
                datetime(2023, 1, 2, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                False,
            ),
            # Monday, right before the start of a period
            (
                datetime(2023, 1, 2, 23, 59, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                False,
            ),
            # Tuesday, right at the start of a period
            (
                datetime(2023, 1, 3, 0, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                True,
            ),
            # Tuesday, in the middle of a period
            (
                datetime(2023, 1, 3, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                True,
            ),
            # Tuesday, right at at the end of a period
            (
                datetime(2023, 1, 3, 5, 59, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                True,
            ),
            # Tuesday, right after the end of a period
            (
                datetime(2023, 1, 3, 6, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                False,
            ),
            # Wednesday, in the middle of a period
            (
                datetime(2023, 1, 4, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                True,
            ),
            # Thursday
            (
                datetime(2023, 1, 5, 2, 0, tzinfo=ZoneInfo("Europe/Kyiv")).astimezone(
                    UTC
                ),
                False,
            ),
        ],
    )
    def test_check_period(self, current_time: datetime, result: bool) -> None:
        schedule = EnergySchedule(
            name="green",
            periods=[
                EnergySchedulePeriod(
                    weekday=2,
                    start_time=time(0, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                    end_time=time(6, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                ),
                EnergySchedulePeriod(
                    weekday=3,
                    start_time=time(0, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                    end_time=time(6, 0, tzinfo=ZoneInfo("Europe/Kyiv")),
                ),
            ],
        )
        assert schedule.check_time(current_time) is result
