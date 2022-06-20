from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import pytest
from yarl import URL

from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.resource import GKEGPUModels, Preset, TPUPreset, TPUResource


@pytest.fixture
def host_storage_payload() -> dict[str, Any]:
    return {
        "storage": {
            "host": {"mount_path": "/host/mount/path"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def nfs_storage_payload() -> dict[str, Any]:
    return {
        "storage": {
            "nfs": {"server": "127.0.0.1", "export_path": "/nfs/export/path"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def pvc_storage_payload() -> dict[str, Any]:
    return {
        "storage": {
            "pvc": {"name": "platform-storage"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def clusters_payload(nfs_storage_payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
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
                    },
                    {
                        "name": "cpu-large",
                        "credits_per_hour": "10",
                        "cpu": 7,
                        "memory": 49152 * 10**6,
                    },
                    {
                        "name": "tpu",
                        "credits_per_hour": "10",
                        "cpu": 7,
                        "memory": 49152 * 10**6,
                        "tpu": {"type": "v2-8", "software_version": "1.14"},
                    },
                    {
                        "name": "gpu-small-p",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "gpu-small",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "gpu-large-p",
                        "credits_per_hour": "10",
                        "cpu": 7.0,
                        "memory": 52224 * 10**6,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                    },
                    {
                        "name": "gpu-large",
                        "credits_per_hour": "10",
                        "cpu": 0.1,
                        "memory": 52224 * 10**6,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
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
                        "disk_size_gb": 150,
                        "tpu": {
                            "ipv4_cidr_block": "1.1.1.1/32",
                            "types": ["v2-8", "v3-8"],
                            "software_versions": ["1.13", "1.14"],
                        },
                    },
                    {
                        "name": "n1-highmem-32-1xk80-preemptible",
                        "is_preemptible": True,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 31.0,
                        "memory": 204800 * 10**6,
                        "disk_size_gb": 150,
                        "gpu": 4,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "n1-highmem-32-1xk80",
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 8,
                        "cpu": 32.0,
                        "available_cpu": 31.0,
                        "memory": 212992 * 10**6,
                        "available_memory": 204800 * 10**6,
                        "disk_size_gb": 150,
                        "gpu": 4,
                        "gpu_model": "nvidia-tesla-k80",
                    },
                    {
                        "name": "n1-highmem-8-1xv100-preemptible",
                        "is_preemptible": True,
                        "min_size": 0,
                        "max_size": 5,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory": 53248 * 10**6,
                        "available_memory": 49152 * 10**6,
                        "disk_size_gb": 150,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                    },
                    {
                        "name": "n1-highmem-8-1xv100",
                        "is_preemptible": False,
                        "min_size": 0,
                        "max_size": 2,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory": 53248 * 10**6,
                        "available_memory": 49152 * 10**6,
                        "disk_size_gb": 150,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                    },
                ],
            },
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
            "secrets": {"url": "https://dev.neu.ro/api/v1/secrets"},
            "metrics": {"url": "https://metrics.dev.neu.ro"},
            "disks": {"url": "https://dev.neu.ro/api/v1/disk"},
            "buckets": {"url": "https://dev.neu.ro/api/v1/buckets"},
            "blob_storage": {"url": "https://dev.neu.ro/api/v1/blob"},
            **nfs_storage_payload,
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
        assert orchestrator.resource_pool_types[0].cpu == 8.0
        assert orchestrator.resource_pool_types[0].available_cpu == 7.0
        assert orchestrator.resource_pool_types[0].memory == 53248 * 10**6
        assert orchestrator.resource_pool_types[0].available_memory == 49152 * 10**6
        assert orchestrator.resource_pool_types[0].disk_gb == 150
        assert orchestrator.resource_pool_types[0].gpu is None
        assert orchestrator.resource_pool_types[0].gpu_model is None
        assert orchestrator.resource_pool_types[0].tpu == TPUResource(
            ipv4_cidr_block="1.1.1.1/32",
            types=("v2-8", "v3-8"),
            software_versions=("1.13", "1.14"),
        )

        assert orchestrator.resource_pool_types[1].cpu == 31.0
        assert orchestrator.resource_pool_types[1].available_cpu == 31.0
        assert orchestrator.resource_pool_types[1].memory == 204800 * 10**6
        assert orchestrator.resource_pool_types[1].available_memory == 204800 * 10**6
        assert orchestrator.resource_pool_types[1].gpu == 4
        assert (
            orchestrator.resource_pool_types[1].gpu_model == GKEGPUModels.K80.value.id
        )

        assert orchestrator.resource_pool_types[3].gpu == 1
        assert (
            orchestrator.resource_pool_types[3].gpu_model == GKEGPUModels.V100.value.id
        )

        assert orchestrator.presets is not None
        assert orchestrator.presets[1].cpu == 7.0
        assert orchestrator.presets[1].memory == 49152 * 10**6
        assert orchestrator.presets[1].gpu_model is None
        assert orchestrator.presets[2] == Preset(
            name="tpu",
            credits_per_hour=Decimal("10"),
            cpu=7.0,
            memory=49152 * 10**6,
            tpu=TPUPreset(type="v2-8", software_version="1.14"),
        )

        assert orchestrator.presets[3].gpu_model == GKEGPUModels.K80.value.id

        assert orchestrator.presets[4].cpu == 7.0
        assert orchestrator.presets[4].gpu == 1
        assert orchestrator.presets[4].gpu_model == GKEGPUModels.K80.value.id
        assert orchestrator.presets[4].memory == 52224 * 10**6

        assert orchestrator.presets[6].cpu == 0.1

        assert orchestrator.tpu_resources == (
            TPUResource(
                ipv4_cidr_block="1.1.1.1/32",
                types=("v2-8", "v3-8"),
                software_versions=("1.13", "1.14"),
            ),
        )
        assert orchestrator.tpu_ipv4_cidr_block == "1.1.1.1/32"

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
