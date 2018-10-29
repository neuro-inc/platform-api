from datetime import timedelta
from pathlib import PurePath

import pytest
from yarl import URL

from platform_api.config import RegistryConfig, StorageConfig, StorageType
from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.kube_orchestrator import (
    HostVolume,
    KubeConfig,
    NfsVolume,
)
from platform_api.resource import GKEGPUModels, ResourcePoolType


class TestStorageConfig:
    def test_missing_nfs_settings(self):
        with pytest.raises(ValueError, match="Missing NFS settings"):
            StorageConfig(host_mount_path=PurePath("/tmp"), type=StorageType.NFS)

    def test_redundant_nfs_settings(self):
        with pytest.raises(ValueError, match="Redundant NFS settings"):
            StorageConfig(
                host_mount_path=PurePath("/tmp"),
                type=StorageType.HOST,
                nfs_server="1.2.3.4",
            )

    def test_is_nfs(self):
        config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.NFS,
            nfs_server="1.2.3.4",
            nfs_export_path=PurePath("/tmp"),
        )
        assert config.is_nfs


class TestKubeConfig:
    def test_create_storage_volume_nfs(self):
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.NFS,
            nfs_server="4.3.2.1",
            nfs_export_path=PurePath("/tmp"),
        )
        registry_config = RegistryConfig()
        kube_config = KubeConfig(
            storage=storage_config,
            registry=registry_config,
            jobs_domain_name="testdomain",
            jobs_ingress_name="testingress",
            ssh_domain_name="ssh.domain",
            endpoint_url="http://1.2.3.4",
            resource_pool_types=[ResourcePoolType()],
        )
        volume = kube_config.create_storage_volume()
        assert volume == NfsVolume(
            name="storage", path=PurePath("/tmp"), server="4.3.2.1"
        )

    def test_create_storage_volume_host(self):
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"), type=StorageType.HOST
        )
        registry_config = RegistryConfig()
        kube_config = KubeConfig(
            storage=storage_config,
            registry=registry_config,
            jobs_domain_name="testdomain",
            ssh_domain_name="ssh.domain",
            jobs_ingress_name="testingress",
            endpoint_url="http://1.2.3.4",
            resource_pool_types=[ResourcePoolType()],
        )
        volume = kube_config.create_storage_volume()
        assert volume == HostVolume(name="storage", path=PurePath("/tmp"))


class TestEnvironConfigFactory:
    def test_create_key_error(self):
        environ = {}
        with pytest.raises(KeyError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_defaults(self):
        environ = {
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_K8S_JOBS_INGRESS_NAME": "testingress",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME": "jobs.domain",
            "NP_K8S_SSH_INGRESS_DOMAIN_NAME": "ssh.domain",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 8080

        assert config.storage.host_mount_path == PurePath("/tmp")
        assert config.storage.container_mount_path == PurePath("/var/storage")
        assert config.storage.uri_scheme == "storage"

        assert config.orchestrator.storage_mount_path == PurePath("/tmp")
        assert config.orchestrator.endpoint_url == "https://localhost:8443"
        assert not config.orchestrator.cert_authority_path
        assert not config.orchestrator.auth_cert_path
        assert not config.orchestrator.auth_cert_key_path
        assert config.orchestrator.namespace == "default"
        assert config.orchestrator.client_conn_timeout_s == 300
        assert config.orchestrator.client_read_timeout_s == 300
        assert config.orchestrator.client_conn_pool_size == 100
        assert config.orchestrator.jobs_ingress_name == "testingress"
        assert config.orchestrator.jobs_ingress_domain_name == "jobs.domain"
        assert config.orchestrator.ssh_ingress_domain_name == "ssh.domain"

        assert config.orchestrator.job_deletion_delay_s == 86400
        assert config.orchestrator.job_deletion_delay == timedelta(days=1)

        assert config.orchestrator.resource_pool_types == [ResourcePoolType()]
        assert config.orchestrator.node_label_gpu is None

        assert config.orchestrator.orphaned_job_owner == "compute"

        assert config.database.redis is None

        assert config.env_prefix == "NP"

        assert config.auth.server_endpoint_url == URL("https://auth")
        assert config.auth.service_token == "token"
        assert config.auth.service_name == "compute"

        assert config.registry.host == "registry.dev.neuromation.io"

    def test_create_value_error_invalid_port(self):
        environ = {
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_API_PORT": "port",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
        }
        with pytest.raises(ValueError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_custom(self):
        environ = {
            "NP_ENV_PREFIX": "TEST",
            "NP_API_PORT": "1111",
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_STORAGE_CONTAINER_MOUNT_PATH": "/opt/storage",
            "NP_STORAGE_URI_SCHEME": "something",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_K8S_CA_PATH": "/ca_path",
            "NP_K8S_AUTH_CERT_PATH": "/cert_path",
            "NP_K8S_AUTH_CERT_KEY_PATH": "/cert_key_path",
            "NP_K8S_NS": "other",
            "NP_K8S_CLIENT_CONN_TIMEOUT": "111",
            "NP_K8S_CLIENT_READ_TIMEOUT": "222",
            "NP_K8S_CLIENT_CONN_POOL_SIZE": "333",
            "NP_K8S_JOBS_INGRESS_NAME": "testingress",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME": "jobs.domain",
            "NP_K8S_SSH_INGRESS_DOMAIN_NAME": "ssh.domain",
            "NP_K8S_JOB_DELETION_DELAY": "3600",
            "NP_DB_REDIS_URI": "redis://localhost:6379/0",
            "NP_DB_REDIS_CONN_POOL_SIZE": "444",
            "NP_DB_REDIS_CONN_TIMEOUT": "555",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_AUTH_NAME": "servicename",
            "NP_REGISTRY_HOST": "testregistry:5000",
            "NP_K8S_NODE_LABEL_GPU": "testlabel",
            "NP_GKE_GPU_MODELS": ",".join(
                [
                    "",
                    "nvidia-tesla-k80",
                    "unknown",
                    "nvidia-tesla-k80",
                    "nvidia-tesla-v100",
                ]
            ),
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 1111

        assert config.storage.host_mount_path == PurePath("/tmp")
        assert config.storage.container_mount_path == PurePath("/opt/storage")
        assert config.storage.uri_scheme == "something"

        assert config.orchestrator.storage_mount_path == PurePath("/tmp")
        assert config.orchestrator.endpoint_url == "https://localhost:8443"
        assert config.orchestrator.cert_authority_path == "/ca_path"
        assert config.orchestrator.auth_cert_path == "/cert_path"
        assert config.orchestrator.auth_cert_key_path == "/cert_key_path"
        assert config.orchestrator.namespace == "other"
        assert config.orchestrator.client_conn_timeout_s == 111
        assert config.orchestrator.client_read_timeout_s == 222
        assert config.orchestrator.client_conn_pool_size == 333
        assert config.orchestrator.jobs_ingress_name == "testingress"
        assert config.orchestrator.jobs_ingress_domain_name == "jobs.domain"
        assert config.orchestrator.ssh_ingress_domain_name == "ssh.domain"

        assert config.orchestrator.job_deletion_delay_s == 3600
        assert config.orchestrator.job_deletion_delay == timedelta(seconds=3600)

        assert config.orchestrator.resource_pool_types == [
            ResourcePoolType(),
            ResourcePoolType(gpu=1, gpu_model=GKEGPUModels.K80.value),
            ResourcePoolType(gpu=1, gpu_model=GKEGPUModels.V100.value),
        ]
        assert config.orchestrator.node_label_gpu == "testlabel"

        assert config.orchestrator.orphaned_job_owner == "servicename"

        assert config.database.redis.uri == "redis://localhost:6379/0"
        assert config.database.redis.conn_pool_size == 444
        assert config.database.redis.conn_timeout_s == 555.0

        assert config.env_prefix == "TEST"

        assert config.auth.server_endpoint_url == URL("https://auth")
        assert config.auth.service_token == "token"
        assert config.auth.service_name == "servicename"

        assert config.registry.host == "testregistry:5000"

    def test_create_nfs(self):
        environ = {
            "NP_STORAGE_TYPE": "nfs",
            "NP_STORAGE_NFS_SERVER": "1.2.3.4",
            "NP_STORAGE_NFS_PATH": "/tmp",
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_K8S_JOBS_INGRESS_NAME": "testingress",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME": "jobs.domain",
            "NP_K8S_SSH_INGRESS_DOMAIN_NAME": "ssh.domain",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
        }
        config = EnvironConfigFactory(environ=environ).create()
        assert config.storage.nfs_server == "1.2.3.4"
        assert config.storage.nfs_export_path == PurePath("/tmp")
