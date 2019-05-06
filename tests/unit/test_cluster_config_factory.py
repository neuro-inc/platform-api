from pathlib import PurePath
from typing import Any, Dict

import pytest
from yarl import URL

from platform_api.cluster_config_factory import ClusterConfigFactory, StorageType


@pytest.fixture
def docker_registry_config() -> Dict[str, str]:
    return {
        "url": "http://docker-resgistry",
        "user_name": "user_name",
        "password": "password",
        "email": "test@email.com",
    }


@pytest.fixture
def helm_repository_config() -> Dict[str, str]:
    return {"url": "http://helm-repository", "user_name": "user_name", "token": "token"}


@pytest.fixture
def auth_config() -> Dict[str, str]:
    return {
        "url": "http://auth-url:8080",
        "storage_token": "storage_token",
        "registry_token": "registry_token",
        "cluster_token": "cluster_token",
    }


@pytest.fixture
def logging_config() -> Dict[str, Any]:
    return {
        "elasticsearch": {
            "hosts": ["http://elasticsearch:9200"],
            "user": "es_user_name",
            "password": "es_assword",
        }
    }


@pytest.fixture
def ingress_config() -> Dict[str, Any]:
    return {
        "storage_url": "http://platform-api:8080/storage",
        "users_url": "http://platform-api:8080/users",
        "monitoring_url": "http://platform-api:8080/monitoring",
    }


@pytest.fixture
def registry_config() -> Dict[str, Any]:
    return {
        "host": "http://registry_host",
        "email": "test@email.com",
        "is_secure": True,
    }


@pytest.fixture
def nfs_storage_config() -> Dict[str, Any]:
    return {
        "type": "nfs",
        "container_mount_path": "/container/path",
        "nfs_server": "127.0.0.1",
        "nfs_export_path": "/nfs/export/path",
    }


@pytest.fixture
def host_storage_config() -> Dict[str, Any]:
    return {
        "type": "host",
        "container_mount_path": "/container/path",
        "host_mount_path": "/host/mount/path",
    }


@pytest.fixture
def orchestrator_config() -> Dict[str, Any]:
    return {
        "jobs_domain_name_template": "{job_id}.jobs.neu.ro",
        "named_jobs_domain_name_template": "{job_name}-{job_owner}.jobs.neu.ro",
        "ssh_domain_name": "ssh.neu.ro",
        "ssh_auth_domain_name": "ssh-auth.neu.ro",
        "resource_pool_types": [
            {"gpu": 1, "gpu_model": "nvidia-tesla-p100"},
            {"gpu": 1, "gpu_model": "nvidia-tesla-v100"},
        ],
        "is_http_ingress_secure": True,
    }


@pytest.fixture
def cluster_config(
    docker_registry_config: Dict[str, str],
    helm_repository_config: Dict[str, str],
    auth_config: Dict[str, str],
    logging_config: Dict[str, Any],
    ingress_config: Dict[str, str],
    registry_config: Dict[str, str],
    nfs_storage_config: Dict[str, Any],
    orchestrator_config: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "name": "cluster_name",
        "type": "cluster_type",
        "docker_registry": docker_registry_config,
        "helm_repository": helm_repository_config,
        "auth": auth_config,
        "logging": logging_config,
        "ingress": ingress_config,
        "registry": registry_config,
        "storage": nfs_storage_config,
        "orchestrator": orchestrator_config,
    }


class TestClusterConfigFactory:
    def test_valid_cluster_config(self, cluster_config: Dict[str, Any]) -> None:
        factory = ClusterConfigFactory()
        results = factory.from_primitive([cluster_config])

        assert len(results) == 1

        result = results[0]

        assert result.name == cluster_config["name"]
        assert result.type == cluster_config["type"]
        assert result.docker_registry is not None
        assert result.helm_repository is not None
        assert result.auth is not None
        assert result.logging is not None
        assert result.ingress is not None
        assert result.registry is not None
        assert result.storage is not None
        assert result.orchestrator is not None

    def test_valid_docker_registry_config(
        self, docker_registry_config: Dict[str, str]
    ) -> None:
        factory = ClusterConfigFactory()
        result = factory.docker_registry_from_primitive(docker_registry_config)

        assert result.url == URL(docker_registry_config["url"])
        assert result.user_name == docker_registry_config["user_name"]
        assert result.password == docker_registry_config["password"]
        assert result.email == docker_registry_config["email"]

    def test_valid_auth_config(self, helm_repository_config: Dict[str, str]) -> None:
        factory = ClusterConfigFactory()
        result = factory.helm_repository_config_from_primitive(helm_repository_config)

        assert result.url == URL(helm_repository_config["url"])
        assert result.user_name == helm_repository_config["user_name"]
        assert result.token == helm_repository_config["token"]

    def test_valid_auth_config(self, auth_config: Dict[str, str]) -> None:
        factory = ClusterConfigFactory()
        result = factory.auth_config_from_primitive(auth_config)

        assert result.url == URL(auth_config["url"])
        assert result.storage_token == auth_config["storage_token"]
        assert result.registry_token == auth_config["registry_token"]
        assert result.cluster_token == auth_config["cluster_token"]

    def test_valid_logging_config(self, logging_config: Dict[str, Any]) -> None:
        factory = ClusterConfigFactory()
        result = factory.logging_config_from_primitive(logging_config)

        assert result.elasticsearch.hosts == logging_config["elasticsearch"]["hosts"]
        assert result.elasticsearch.user == logging_config["elasticsearch"]["user"]
        assert (
            result.elasticsearch.password == logging_config["elasticsearch"]["password"]
        )

    def test_valid_ingress_config(self, ingress_config: Dict[str, str]) -> None:
        factory = ClusterConfigFactory()
        result = factory.ingress_config_from_primitive(ingress_config)

        assert result.storage_url == URL(ingress_config["storage_url"])
        assert result.users_url == URL(ingress_config["users_url"])
        assert result.monitoring_url == URL(ingress_config["monitoring_url"])

    def test_valid_registry_config(self, registry_config: Dict[str, Any]) -> None:
        factory = ClusterConfigFactory()
        result = factory.registry_config_from_primitive(registry_config)

        assert result.host == registry_config["host"]
        assert result.email == registry_config["email"]
        assert result.is_secure == registry_config["is_secure"]

    def test_valid_nfs_storage_config(self, nfs_storage_config: Dict[str, Any]) -> None:
        factory = ClusterConfigFactory()
        result = factory.storage_config_from_primitive(nfs_storage_config)

        assert result.type == StorageType.NFS
        assert result.uri_scheme == "storage"
        assert result.container_mount_path == PurePath(
            nfs_storage_config["container_mount_path"]
        )
        assert result.nfs_server == nfs_storage_config["nfs_server"]
        assert result.nfs_export_path == PurePath(nfs_storage_config["nfs_export_path"])

    def test_nfs_storage_config_without_nfs_settings__fails(
        self, nfs_storage_config: Dict[str, Any]
    ) -> None:
        del nfs_storage_config["nfs_server"]
        del nfs_storage_config["nfs_export_path"]

        factory = ClusterConfigFactory()

        with pytest.raises(ValueError, match="Missing NFS settings"):
            factory.storage_config_from_primitive(nfs_storage_config)

    def test_nfs_storage_config_with_redundant_host_settings__fails(
        self, nfs_storage_config: Dict[str, Any]
    ) -> None:
        nfs_storage_config["host_mount_path"] = "/host/mount/path"

        factory = ClusterConfigFactory()

        with pytest.raises(ValueError, match="Redundant host settings"):
            factory.storage_config_from_primitive(nfs_storage_config)

    def test_valid_host_storage_config(
        self, host_storage_config: Dict[str, Any]
    ) -> None:
        factory = ClusterConfigFactory()
        result = factory.storage_config_from_primitive(host_storage_config)

        assert result.type == StorageType.HOST
        assert result.uri_scheme == "storage"
        assert result.container_mount_path == PurePath(
            host_storage_config["container_mount_path"]
        )
        assert result.host_mount_path == PurePath(
            host_storage_config["host_mount_path"]
        )

    def test_host_storage_config_without_host_settings__fails(
        self, host_storage_config: Dict[str, Any]
    ) -> None:
        del host_storage_config["host_mount_path"]

        factory = ClusterConfigFactory()

        with pytest.raises(ValueError, match="Missing host settings"):
            factory.storage_config_from_primitive(host_storage_config)

    def test_host_storage_config_with_redundant_nfs_settings__fails(
        self, host_storage_config: Dict[str, Any]
    ) -> None:
        host_storage_config["nfs_server"] = "127.0.0.1"
        host_storage_config["nfs_export_path"] = "/nfs/export/path"

        factory = ClusterConfigFactory()

        with pytest.raises(ValueError, match="Redundant NFS settings"):
            factory.storage_config_from_primitive(host_storage_config)
