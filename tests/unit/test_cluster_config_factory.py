from pathlib import PurePath
from typing import Any, Dict

import pytest
from yarl import URL

from platform_api.cluster_config import KubeClientAuthType, KubeConfig, StorageType
from platform_api.cluster_config_factory import ClusterConfigFactory
from platform_api.resource import GKEGPUModels


@pytest.fixture
def monitoring_payload() -> Dict[str, Any]:
    return {
        "url": "http://platform-api/monitoring",
        "elasticsearch": {
            "hosts": ["http://elasticsearch:9200"],
            "user": "es_user_name",
            "password": "es_assword",
        },
    }


@pytest.fixture
def ssh_payload() -> Dict[str, Any]:
    return {"server": "ssh-server"}


@pytest.fixture
def registry_payload() -> Dict[str, Any]:
    return {"url": "http://registry-host", "email": "test@email.com"}


@pytest.fixture
def storage_payload() -> Dict[str, Any]:
    return {
        "host": {"mount_path": "/host/mount/path"},
        "nfs": {"server": "127.0.0.1", "export_path": "/nfs/export/path"},
        "url": "http://platform-api/storage",
    }


@pytest.fixture
def kube_payload() -> Dict[str, Any]:
    return {
        "url": "https://1.2.3.4:8443",
        "ca_data": "base64.encode(pem)",
        "auth_type": "token",
        "token": "auth_token",
        "namespace": "default",
        "jobs_ingress_name": "platformjobsingress",
        "jobs_ingress_auth_name": "platformjobsingressauth",
        "node_label_gpu": "cloud.google.com/gke-accelerator",
        "node_label_preemptible": "cloud.google.com/gke-preemptible",
    }


@pytest.fixture
def orchestrator_payload(kube_payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "kubernetes": kube_payload,
        "job_domain_name_template": "{job_id}.jobs.neu.ro",
        "named_job_domain_name_template": "{job_name}-{job_owner}.jobs.neu.ro",
        "resource_pool_types": [
            {"gpu": 0, "gpu_model": None},
            {"gpu": 1, "gpu_model": "nvidia-tesla-v100"},
        ],
        "is_http_ingress_secure": True,
    }


@pytest.fixture
def cluster_payload(
    monitoring_payload: Dict[str, Any],
    ssh_payload: Dict[str, str],
    registry_payload: Dict[str, str],
    storage_payload: Dict[str, Any],
    orchestrator_payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "name": "cluster_name",
        "monitoring": monitoring_payload,
        "ssh": ssh_payload,
        "registry": registry_payload,
        "storage": storage_payload,
        "orchestrator": orchestrator_payload,
    }


class TestClusterConfigFactory:
    def test_valid_cluster_config(
        self,
        monitoring_payload: Dict[str, Any],
        ssh_payload: Dict[str, str],
        registry_payload: Dict[str, str],
        storage_payload: Dict[str, Any],
        orchestrator_payload: Dict[str, Any],
        kube_payload: Dict[str, Any],
        cluster_payload: Dict[str, Any],
    ) -> None:
        factory = ClusterConfigFactory()
        results = factory.cluster_configs([cluster_payload])

        assert len(results) == 1

        result = results[0]

        assert result.name == cluster_payload["name"]

        logging = result.logging
        assert (
            logging.elasticsearch.hosts
            == cluster_payload["monitoring"]["elasticsearch"]["hosts"]
        )
        assert (
            logging.elasticsearch.user
            == cluster_payload["monitoring"]["elasticsearch"]["user"]
        )
        assert (
            logging.elasticsearch.password
            == cluster_payload["monitoring"]["elasticsearch"]["password"]
        )

        ingress = result.ingress
        assert ingress.storage_url == URL(cluster_payload["storage"]["url"])
        assert ingress.monitoring_url == URL(cluster_payload["monitoring"]["url"])

        registry = result.registry
        assert registry.host is cluster_payload["registry"]["url"]
        assert registry.email is cluster_payload["registry"]["email"]

        storage = result.storage
        assert storage.type == StorageType.NFS
        assert storage.host_mount_path == PurePath(
            cluster_payload["storage"]["host"]["mount_path"]
        )
        assert storage.nfs_server == cluster_payload["storage"]["nfs"]["server"]
        assert storage.nfs_export_path == PurePath(
            cluster_payload["storage"]["nfs"]["export_path"]
        )

        orchestrator = result.orchestrator
        assert isinstance(orchestrator, KubeConfig)

        assert orchestrator.ssh_domain_name == cluster_payload["ssh"]["server"]
        assert orchestrator.ssh_auth_domain_name == cluster_payload["ssh"]["server"]
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

        assert len(orchestrator.resource_pool_types) == 2
        assert orchestrator.resource_pool_types[0].gpu == 0
        assert orchestrator.resource_pool_types[0].gpu_model is None
        assert orchestrator.resource_pool_types[1].gpu == 1
        assert orchestrator.resource_pool_types[1].gpu_model == GKEGPUModels.V100.value

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
