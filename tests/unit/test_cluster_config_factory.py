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
                },
                "job_hostname_template": "{job_id}.jobs.neu.ro",
                "resource_pool_types": [
                    {
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 8.0,
                        "memory_mb": 53248,
                        "disk_gb": 150,
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
                        "is_preemptible": True,
                        "min_size": 1,
                        "max_size": 16,
                        "cpu": 32.0,
                        "memory_mb": 212992,
                        "disk_gb": 150,
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
                        "is_preemptible": False,
                        "min_size": 1,
                        "max_size": 8,
                        "cpu": 32.0,
                        "memory_mb": 212992,
                        "disk_gb": 150,
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
                        "is_preemptible": True,
                        "min_size": 0,
                        "max_size": 5,
                        "cpu": 8.0,
                        "memory_mb": 53248,
                        "disk_gb": 150,
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
                        "is_preemptible": False,
                        "min_size": 0,
                        "max_size": 2,
                        "cpu": 8.0,
                        "memory_mb": 53248,
                        "disk_gb": 150,
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
            "ssh": {"server": "ssh-auth-dev.neu.ro"},
            "monitoring": {"url": "https://dev.neu.ro/api/v1/jobs"},
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
        users_url: URL,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]
        registry_payload = clusters_payload[0]["registry"]
        orchestrator_payload = clusters_payload[0]["orchestrator"]
        kube_payload = orchestrator_payload["kubernetes"]
        monitoring_payload = clusters_payload[0]["monitoring"]
        ssh_payload = clusters_payload[0]["ssh"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            users_url=users_url,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
        )

        assert len(clusters) == 1

        cluster = clusters[0]

        assert cluster.name == clusters_payload[0]["name"]

        ingress = cluster.ingress
        assert ingress.storage_url == URL(storage_payload["url"])
        assert ingress.monitoring_url == URL(monitoring_payload["url"])
        assert ingress.users_url == users_url

        registry = cluster.registry
        assert registry.url == URL(registry_payload["url"])
        assert registry.email == registry_payload["email"]

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        nfs_mount_point = PurePath(storage_payload["nfs"]["export_path"])
        assert storage.host_mount_path == nfs_mount_point
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == nfs_mount_point

        orchestrator = cluster.orchestrator
        assert isinstance(orchestrator, KubeConfig)

        assert orchestrator.ssh_auth_domain_name == ssh_payload["server"]
        assert (
            orchestrator.is_http_ingress_secure
            == orchestrator_payload["is_http_ingress_secure"]
        )
        assert (
            orchestrator.jobs_domain_name_template
            == orchestrator_payload["job_hostname_template"]
        )

        assert len(orchestrator.resource_pool_types) == 5
        assert orchestrator.resource_pool_types[0].gpu is None
        assert orchestrator.resource_pool_types[0].gpu_model is None
        assert orchestrator.resource_pool_types[0].tpu == TPUResource(
            ipv4_cidr_block="1.1.1.1/32",
            types=("v2-8", "v3-8"),
            software_versions=("1.13", "1.14"),
        )

        assert orchestrator.resource_pool_types[1].gpu == 4
        assert (
            orchestrator.resource_pool_types[1].gpu_model == GKEGPUModels.K80.value.id
        )

        assert orchestrator.resource_pool_types[3].gpu == 1
        assert (
            orchestrator.resource_pool_types[3].gpu_model == GKEGPUModels.V100.value.id
        )

        assert orchestrator.resource_pool_types[0].presets is not None
        assert orchestrator.resource_pool_types[0].presets[1].cpu == 7.0
        assert orchestrator.resource_pool_types[0].presets[1].memory_mb == 49152
        assert orchestrator.resource_pool_types[0].presets[1].gpu_model is None
        assert orchestrator.resource_pool_types[0].presets[2] == Preset(
            name="tpu",
            cpu=7.0,
            memory_mb=49152,
            tpu=TPUPreset(type="v2-8", software_version="1.14"),
        )

        assert orchestrator.resource_pool_types[1].presets is not None
        assert (
            orchestrator.resource_pool_types[1].presets[0].gpu_model
            == GKEGPUModels.K80.value.id
        )

        assert orchestrator.resource_pool_types[2].presets is not None
        assert orchestrator.resource_pool_types[2].presets[0].cpu == 7.0
        assert orchestrator.resource_pool_types[2].presets[0].gpu == 1
        assert (
            orchestrator.resource_pool_types[2].presets[0].gpu_model
            == GKEGPUModels.K80.value.id
        )
        assert orchestrator.resource_pool_types[2].presets[0].memory_mb == 52224

        assert orchestrator.resource_pool_types[4].presets is not None
        assert orchestrator.resource_pool_types[4].presets[0].cpu == 0.1

        assert orchestrator.endpoint_url == kube_payload["url"]
        assert orchestrator.cert_authority_data_pem == kube_payload["ca_data"]
        assert orchestrator.cert_authority_path is None
        assert orchestrator.auth_type == KubeClientAuthType.TOKEN
        assert orchestrator.namespace == kube_payload["namespace"]
        assert orchestrator.jobs_ingress_class == "nginx"
        assert orchestrator.jobs_ingress_oauth_url == URL(
            "https://neu.ro/oauth/authorize"
        )
        assert orchestrator.node_label_gpu == kube_payload["node_label_gpu"]
        assert (
            orchestrator.node_label_preemptible
            == kube_payload["node_label_preemptible"]
        )

        assert orchestrator.tpu_resources == (
            TPUResource(
                ipv4_cidr_block="1.1.1.1/32",
                types=("v2-8", "v3-8"),
                software_versions=("1.13", "1.14"),
            ),
        )
        assert orchestrator.tpu_ipv4_cidr_block == "1.1.1.1/32"

    def test_valid_storage_config_nfs(
        self,
        clusters_payload: Sequence[Dict[str, Any]],
        users_url: URL,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            users_url=users_url,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
        )
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        nfs_mount_point = PurePath(storage_payload["nfs"]["export_path"])
        assert storage.host_mount_path == nfs_mount_point
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == nfs_mount_point

    def test_create_storage_config_host(
        self, host_storage_payload: Dict[str, Any]
    ) -> None:
        factory = ClusterConfigFactory()
        config = factory._create_storage_config(payload=host_storage_payload)
        # initialized fields:
        assert config.host_mount_path == PurePath("/host/mount/path")
        assert config.type == StorageType.HOST
        # default fields:
        assert config.container_mount_path == PurePath("/var/storage")
        assert config.nfs_server is None
        assert config.nfs_export_path is None
        assert config.uri_scheme == "storage"

    def test_create_storage_config_nfs(
        self, nfs_storage_payload: Dict[str, Any]
    ) -> None:
        factory = ClusterConfigFactory()
        config = factory._create_storage_config(payload=nfs_storage_payload)
        # initialized fields:
        assert config.nfs_server == "127.0.0.1"
        assert config.nfs_export_path == PurePath("/nfs/export/path")
        assert config.host_mount_path == PurePath("/nfs/export/path")
        assert config.type == StorageType.NFS
        # default fields:
        assert config.container_mount_path == PurePath("/var/storage")
        assert config.uri_scheme == "storage"

    def test_factory_skips_invalid_cluster_configs(
        self,
        clusters_payload: List[Dict[str, Any]],
        users_url: URL,
        jobs_ingress_class: str,
        jobs_ingress_oauth_url: URL,
    ) -> None:
        clusters_payload.append({})
        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(
            clusters_payload,
            users_url=users_url,
            jobs_ingress_class=jobs_ingress_class,
            jobs_ingress_oauth_url=jobs_ingress_oauth_url,
        )

        assert len(clusters) == 1
