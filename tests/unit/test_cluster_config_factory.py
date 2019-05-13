from pathlib import PurePath
from typing import Any, Dict, Sequence

import pytest
from yarl import URL

from platform_api.cluster_config import StorageType
from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.orchestrator.kube_client import KubeClientAuthType
from platform_api.orchestrator.kube_orchestrator import KubeConfig
from platform_api.resource import GKEGPUModels


@pytest.fixture
def clusters_payload() -> Sequence[Dict[str, Any]]:
    return [
        {
            "name": "cluster_name",
            "storage": {
                "host": {"mount_path": "/host/mount/path"},
                "nfs": {"server": "127.0.0.1", "export_path": "/nfs/export/path"},
                "url": "https://dev.neu.ro/api/v1/storage",
            },
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
        assert registry.host is registry_payload["url"]
        assert registry.email is registry_payload["email"]

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        assert storage.host_mount_path == PurePath(
            storage_payload["host"]["mount_path"]
        )
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == PurePath(
            storage_payload["nfs"]["export_path"]
        )

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

        del storage_payload["host"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.type == StorageType.NFS
        assert storage.host_mount_path is None
        assert storage.nfs_server == storage_payload["nfs"]["server"]
        assert storage.nfs_export_path == PurePath(
            storage_payload["nfs"]["export_path"]
        )

    def test_valid_storage_config_host(
        self, clusters_payload: Sequence[Dict[str, Any]], users_url: URL
    ) -> None:
        storage_payload = clusters_payload[0]["storage"]

        del storage_payload["nfs"]

        factory = ClusterConfigFactory()
        clusters = factory.create_cluster_configs(clusters_payload, users_url)
        cluster = clusters[0]

        storage = cluster.storage
        assert storage.type == StorageType.HOST
        assert storage.host_mount_path == PurePath(
            storage_payload["host"]["mount_path"]
        )
        assert storage.nfs_server is None
        assert storage.nfs_export_path is None
