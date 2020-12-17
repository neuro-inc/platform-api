from pathlib import PurePath
from typing import Any, Dict, List, Sequence

import pytest
from yarl import URL

from platform_api.cluster_config import StorageType
from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.orchestrator.kube_client import KubeClientAuthType
from platform_api.orchestrator.kube_orchestrator import KubeConfig
from platform_api.resource import GKEGPUModels, Preset, TPUPreset, TPUResource


@pytest.fixture
def host_storage_payload() -> Dict[str, Any]:
    return {
        "storage": {
            "host": {"mount_path": "/host/mount/path"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def nfs_storage_payload() -> Dict[str, Any]:
    return {
        "storage": {
            "nfs": {"server": "127.0.0.1", "export_path": "/nfs/export/path"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def pvc_storage_payload() -> Dict[str, Any]:
    return {
        "storage": {
            "pvc": {"name": "platform-storage"},
            "url": "https://dev.neu.ro/api/v1/storage",
        }
    }


@pytest.fixture
def clusters_payload(nfs_storage_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
            "registry": {
                "url": "https://registry-dev.neu.ro",
                "email": "registry@neuromation.io",
            },
            "orchestrator": {
                "kubernetes": {
                    "url": "https://1.2.3.4:8443",
                    "ca_data": "certificate",
                    "auth_type": "token",
                    "token": "auth_token",
                    "namespace": "default",
                    "node_label_gpu": "cloud.google.com/gke-accelerator",
                    "node_label_preemptible": "cloud.google.com/gke-preemptible",
                    "node_label_job": "platform.neuromation.io/job",
                    "node_label_node_pool": "platform.neuromation.io/nodepool",
                    "job_pod_preemptible_toleration_key": (
                        "platform.neuromation.io/preemptible"
                    ),
                    "job_pod_priority_class_name": "testpriority",
                },
                "job_hostname_template": "{job_id}.jobs.neu.ro",
                "job_schedule_timeout_s": 60,
                "job_schedule_scale_up_timeout_s": 120,
                "resource_pool_types": [
                    {
                        "name": "n1-highmem-8",
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory_mb": 53248,
                        "available_memory_mb": 49152,
                        "disk_size_gb": 150,
                        "tpu": {
                            "ipv4_cidr_block": "1.1.1.1/32",
                            "types": ["v2-8", "v3-8"],
                            "software_versions": ["1.13", "1.14"],
                        },
                        "presets": [
                            {"name": "cpu-small", "cpu": 1, "memory_mb": 2048},
                            {"name": "cpu-large", "cpu": 7, "memory_mb": 49152},
                            {
                                "name": "tpu",
                                "cpu": 7,
                                "memory_mb": 49152,
                                "tpu": {"type": "v2-8", "software_version": "1.14"},
                            },
                        ],
                    },
                    {
                        "name": "n1-highmem-32-1xk80-preemptible",
                        "is_preemptible": True,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 31.0,
                        "memory_mb": 204800,
                        "disk_size_gb": 150,
                        "gpu": 4,
                        "gpu_model": "nvidia-tesla-k80",
                        "presets": [
                            {
                                "name": "gpu-small-p",
                                "cpu": 7.0,
                                "memory_mb": 52224,
                                "gpu": 1,
                            }
                        ],
                    },
                    {
                        "name": "n1-highmem-32-1xk80",
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 8,
                        "cpu": 32.0,
                        "available_cpu": 31.0,
                        "memory_mb": 212992,
                        "available_memory_mb": 204800,
                        "disk_size_gb": 150,
                        "gpu": 4,
                        "gpu_model": "nvidia-tesla-k80",
                        "presets": [
                            {
                                "name": "gpu-small",
                                "cpu": 7.0,
                                "memory_mb": 52224,
                                "gpu": 1,
                            }
                        ],
                    },
                    {
                        "name": "n1-highmem-8-1xv100-preemptible",
                        "is_preemptible": True,
                        "min_size": 0,
                        "max_size": 5,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory_mb": 53248,
                        "available_memory_mb": 49152,
                        "disk_size_gb": 150,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "presets": [
                            {
                                "name": "gpu-large-p",
                                "cpu": 7.0,
                                "memory_mb": 52224,
                                "gpu": 1,
                            }
                        ],
                    },
                    {
                        "name": "n1-highmem-8-1xv100",
                        "is_preemptible": False,
                        "min_size": 0,
                        "max_size": 2,
                        "cpu": 8.0,
                        "available_cpu": 7.0,
                        "memory_mb": 53248,
                        "available_memory_mb": 49152,
                        "disk_size_gb": 150,
                        "gpu": 1,
                        "gpu_model": "nvidia-tesla-v100",
                        "presets": [
                            {
                                "name": "gpu-large",
                                "cpu": 0.1,
                                "memory_mb": 52224,
                                "gpu": 1,
                            }
                        ],
                    },
                ],
                "is_http_ingress_secure": True,
            },
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
            "secrets": {"url": "https://dev.neu.ro/api/v1/secrets"},
            "metrics": {"url": "https://metrics.dev.neu.ro"},
            "disks": {"url": "https://dev.neu.ro/api/v1/disk"},
            "blob_storage": {"url": "https://dev.neu.ro/api/v1/blob"},
            **nfs_storage_payload,
        }
    ]


@pytest.fixture
def users_url() -> URL:
    return URL("https://dev.neu.ro/api/v1/users")


@pytest.fixture
def jobs_ingress_class() -> str:
    return "nginx"


@pytest.fixture
def jobs_ingress_oauth_url() -> URL:
    return URL("https://neu.ro/oauth/authorize")


class TestClusterConfigFactory:
    def test_valid_cluster_config(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]
        registry_payload = clusters_payload[0]["registry"]
        orchestrator_payload = clusters_payload[0]["orchestrator"]
        kube_payload = orchestrator_payload["kubernetes"]
        monitoring_payload = clusters_payload[0]["monitoring"]
        secrets_payload = clusters_payload[0]["secrets"]
        metrics_payload = clusters_payload[0]["metrics"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )

        assert len(clusters) == 1

        cluster = clusters[0]

        assert cluster.name == clusters_payload[0]["name"]

        ingress = cluster.ingress
        assert ingress.storage_url == URL(storage_payload["url"])
        assert ingress.monitoring_url == URL(monitoring_payload["url"])
        assert ingress.secrets_url == URL(secrets_payload["url"])
        assert ingress.metrics_url == URL(metrics_payload["url"])

        registry = cluster.registry
        assert registry.url == URL(registry_payload["url"])
        assert registry.email == registry_payload["email"]
        assert registry.username == "registry_user"
        assert registry.password == "registry_token"

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        nfs_mount_point = PurePath(storage_payload["nfs"]["export_path"])
        assert storage.host_mount_path == nfs_mount_point
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == nfs_mount_point

        orchestrator = cluster.orchestrator
        assert isinstance(orchestrator, KubeConfig)

        assert (
            orchestrator.is_http_ingress_secure
            == orchestrator_payload["is_http_ingress_secure"]
        )
        assert (
            orchestrator.jobs_domain_name_template
            == orchestrator_payload["job_hostname_template"]
        )
        assert orchestrator.job_schedule_timeout == 60
        assert orchestrator.job_schedule_scaleup_timeout == 120

        assert len(orchestrator.resource_pool_types) == 5
        assert orchestrator.resource_pool_types[0].name == "n1-highmem-8"
        assert orchestrator.resource_pool_types[0].cpu == 8.0
        assert orchestrator.resource_pool_types[0].available_cpu == 7.0
        assert orchestrator.resource_pool_types[0].memory_mb == 53248
        assert orchestrator.resource_pool_types[0].available_memory_mb == 49152
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
        assert orchestrator.resource_pool_types[1].memory_mb == 204800
        assert orchestrator.resource_pool_types[1].available_memory_mb == 204800
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
        assert orchestrator.presets[1].memory_mb == 49152
        assert orchestrator.presets[1].gpu_model is None
        assert orchestrator.presets[2] == Preset(
            name="tpu",
            cpu=7.0,
            memory_mb=49152,
            tpu=TPUPreset(type="v2-8", software_version="1.14"),
        )

        assert orchestrator.presets[3].gpu_model == GKEGPUModels.K80.value.id

        assert orchestrator.presets[4].cpu == 7.0
        assert orchestrator.presets[4].gpu == 1
        assert orchestrator.presets[4].gpu_model == GKEGPUModels.K80.value.id
        assert orchestrator.presets[4].memory_mb == 52224

        assert orchestrator.presets[6].cpu == 0.1

        assert orchestrator.endpoint_url == kube_payload["url"]
        assert orchestrator.cert_authority_data_pem == kube_payload["ca_data"]
        assert orchestrator.cert_authority_path is None
        assert orchestrator.auth_type == KubeClientAuthType.TOKEN
        assert orchestrator.namespace == kube_payload["namespace"]
        assert orchestrator.jobs_ingress_class == "nginx"
        assert orchestrator.jobs_ingress_oauth_url == URL(
            "https://neu.ro/oauth/authorize"
        )
        assert (
            orchestrator.jobs_pod_preemptible_toleration_key
            == "platform.neuromation.io/preemptible"
        )
        assert orchestrator.node_label_gpu == kube_payload["node_label_gpu"]
        assert (
            orchestrator.node_label_preemptible
            == kube_payload["node_label_preemptible"]
        )
        assert orchestrator.node_label_job == kube_payload["node_label_job"]
        assert orchestrator.node_label_node_pool == kube_payload["node_label_node_pool"]

        assert orchestrator.tpu_resources == (
            TPUResource(
                ipv4_cidr_block="1.1.1.1/32",
                types=("v2-8", "v3-8"),
                software_versions=("1.13", "1.14"),
            ),
        )
        assert orchestrator.tpu_ipv4_cidr_block == "1.1.1.1/32"
        assert orchestrator.jobs_pod_priority_class_name == "testpriority"

    def test_orchestrator_resource_presets(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        factory = ClusterConfigFactory()
        clusters_payload[0]["orchestrator"]["resource_presets"] = [
            {"name": "cpu-small", "cpu": 1, "memory_mb": 2048},
            {"name": "cpu-large", "cpu": 7, "memory_mb": 49152},
            {
                "name": "cpu-large-p",
                "cpu": 7,
                "memory_mb": 49152,
                "is_preemptible": True,
                "is_preemptible_node_required": True,
            },
        ]
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )

        assert clusters[0].orchestrator.presets == [
            Preset(name="cpu-small", cpu=1, memory_mb=2048),
            Preset(name="cpu-large", cpu=7, memory_mb=49152),
            Preset(
                name="cpu-large-p",
                cpu=7,
                memory_mb=49152,
                is_preemptible=True,
                is_preemptible_node_required=True,
            ),
        ]

    def test_orchestrator_job_schedule_settings_default(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        orchestrator = clusters_payload[0]["orchestrator"]
        del orchestrator["job_schedule_timeout_s"]
        del orchestrator["job_schedule_scale_up_timeout_s"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )

        assert clusters[0].orchestrator.job_schedule_timeout == 180
        assert clusters[0].orchestrator.job_schedule_scaleup_timeout == 900

    def test_storage_config_nfs(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        assert storage.is_nfs
        nfs_mount_point = PurePath(storage_payload["nfs"]["export_path"])
        assert storage.host_mount_path == nfs_mount_point
        assert storage.container_mount_path == PurePath("/var/storage")
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == nfs_mount_point
        assert storage.uri_scheme == "storage"

    def test_storage_config_pvc(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
        pvc_storage_payload: Dict[str, Any],
    ) -> None:
        storage_payload = pvc_storage_payload
        clusters_payload[0].update(storage_payload)

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.type == StorageType.PVC
        assert storage.is_pvc
        assert storage.host_mount_path == PurePath("/mnt/storage")
        assert storage.container_mount_path == PurePath("/var/storage")
        assert storage.pvc_name == "platform-storage"
        assert storage.uri_scheme == "storage"

    def test_storage_config_host(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
        host_storage_payload: Dict[str, Any],
    ) -> None:
        storage_payload = host_storage_payload
        clusters_payload[0].update(storage_payload)

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.host_mount_path == PurePath("/host/mount/path")
        assert storage.type == StorageType.HOST
        assert not storage.is_pvc
        assert not storage.is_nfs
        assert storage.container_mount_path == PurePath("/var/storage")
        assert storage.nfs_server is None
        assert storage.nfs_export_path is None
        assert storage.pvc_name is None
        assert storage.uri_scheme == "storage"

    def test_factory_skips_invalid_cluster_configs(
        self,
        clusters_payload: List[Dict[str, Any]],
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        clusters_payload.append({})
        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
            registry_username="registry_user",
            registry_password="registry_token",
        )

        assert len(clusters) == 1
