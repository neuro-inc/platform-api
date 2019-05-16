from pathlib import PurePath
from typing import Any, Dict, List, Sequence

import pytest
from yarl import URL

from platform_api.cluster_config import StorageType
from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.orchestrator.kube_client import KubeClientAuthType
from platform_api.orchestrator.kube_orchestrator import KubeConfig
from platform_api.resource import GKEGPUModels


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
                    "jobs_ingress_name": "platformjobsingress",
                    "jobs_ingress_auth_name": "platformjobsingressauth",
                    "node_label_gpu": "cloud.google.com/gke-accelerator",
                    "node_label_preemptible": "cloud.google.com/gke-preemptible",
                },
                "job_domain_name_template": "{job_id}.jobs.neu.ro",
                "named_job_domain_name_template": "{job_name}-{job_owner}.jobs.neu.ro",
                "resource_pool_types": [
                    {},
                    {"gpu": 0},
                    {"gpu": 1, "gpu_model": "nvidia-tesla-v100"},
                ],
                "is_http_ingress_secure": True,
            },
            "ssh": {"server": "ssh-auth-dev.neu.ro"},
            "monitoring": {
                "url": "https://dev.neu.ro/api/v1/jobs",
                "elasticsearch": {
                    "hosts": ["http://logging-elasticsearch:9200"],
                    "user": "es_user_name",
                    "password": "es_assword",
                },
            },
            **nfs_storage_payload,
        }
    ]


@pytest.fixture
def users_url() -> URL:
    return URL("https://dev.neu.ro/api/v1/users")


class TestClusterConfigFactory:
    def test_valid_cluster_config(
        self, clusters_payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]
        registry_payload = clusters_payload[0]["registry"]
        orchestrator_payload = clusters_payload[0]["orchestrator"]
        kube_payload = orchestrator_payload["kubernetes"]
        ssh_payload = clusters_payload[0]["ssh"]
        monitoring_payload = clusters_payload[0]["monitoring"]
        elasticsearch_payload = monitoring_payload["elasticsearch"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)

        assert len(clusters) == 1

        cluster = clusters[0]

        assert cluster.name == clusters_payload[0]["name"]

        logging = cluster.logging
        assert logging.elasticsearch.hosts == elasticsearch_payload["hosts"]
        assert logging.elasticsearch.user == elasticsearch_payload["user"]
        assert logging.elasticsearch.password == elasticsearch_payload["password"]

        ingress = cluster.ingress
        assert ingress.storage_url == URL(storage_payload["url"])
        assert ingress.monitoring_url == URL(monitoring_payload["url"])

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

        assert orchestrator.ssh_domain_name == ssh_payload["server"]
        assert orchestrator.ssh_auth_domain_name == ssh_payload["server"]
        assert (
            orchestrator.is_http_ingress_secure
            == orchestrator_payload["is_http_ingress_secure"]
        )
        assert (
            orchestrator.jobs_domain_name_template
            == orchestrator_payload["job_domain_name_template"]
        )
        assert (
            orchestrator.named_jobs_domain_name_template
            == orchestrator_payload["named_job_domain_name_template"]
        )

        assert len(orchestrator.resource_pool_types) == 3
        assert orchestrator.resource_pool_types[0].gpu is None
        assert orchestrator.resource_pool_types[0].gpu_model is None
        assert orchestrator.resource_pool_types[1].gpu == 0
        assert orchestrator.resource_pool_types[1].gpu_model is None
        assert orchestrator.resource_pool_types[2].gpu == 1
        assert orchestrator.resource_pool_types[2].gpu_model == GKEGPUModels.V100.value

        assert orchestrator.endpoint_url == kube_payload["url"]
        assert orchestrator.cert_authority_data_pem == kube_payload["ca_data"]
        assert orchestrator.cert_authority_path is None
        assert orchestrator.auth_type == KubeClientAuthType.TOKEN
        assert orchestrator.namespace == kube_payload["namespace"]
        assert orchestrator.jobs_ingress_name == kube_payload["jobs_ingress_name"]
        assert (
            orchestrator.jobs_ingress_auth_name
            == kube_payload["jobs_ingress_auth_name"]
        )
        assert orchestrator.node_label_gpu == kube_payload["node_label_gpu"]
        assert (
            orchestrator.node_label_preemptible
            == kube_payload["node_label_preemptible"]
        )

    def test_valid_elasticsearch_config_without_user(
        self, clusters_payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> None:
        elasticsearch_payload = clusters_payload[0]["monitoring"]["elasticsearch"]

        del elasticsearch_payload["user"]
        del elasticsearch_payload["password"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)
        cluster = clusters[0]

        logging = cluster.logging
        assert logging.elasticsearch.hosts == elasticsearch_payload["hosts"]
        assert logging.elasticsearch.user is None
        assert logging.elasticsearch.password is None

    def test_valid_storage_config_nfs(
        self, clusters_payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)
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
        self, clusters_payload: List[Dict[str, Any]], users_url: URL
    ) -> None:
        clusters_payload.append({})
        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)

        assert len(clusters) == 1
