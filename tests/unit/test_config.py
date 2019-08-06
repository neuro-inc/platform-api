from datetime import timedelta
from pathlib import PurePath
from typing import Dict

import pytest
from yarl import URL

from platform_api.cluster_config import (
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
    StorageType,
)
from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.kube_orchestrator import (
    HostVolume,
    KubeConfig,
    KubeOrchestrator,
    NfsVolume,
)
from platform_api.resource import (
    DEFAULT_PRESETS,
    GKEGPUModels,
    Preset,
    ResourcePoolType,
)
from tests.unit.conftest import CA_DATA_PEM


class TestStorageConfig:
    def test_missing_nfs_settings(self) -> None:
        with pytest.raises(ValueError, match="Missing NFS settings"):
            StorageConfig(host_mount_path=PurePath("/tmp"), type=StorageType.NFS)

    def test_nfs_invalid_host_mount_path(self) -> None:
        with pytest.raises(ValueError, match="Invalid host mount path"):
            StorageConfig(
                host_mount_path=PurePath("/path1"),
                nfs_export_path=PurePath("/path2"),
                nfs_server="http://1.2.3.4",
                type=StorageType.NFS,
            )

    def test_redundant_nfs_settings(self) -> None:
        with pytest.raises(ValueError, match="Redundant NFS settings"):
            StorageConfig(
                host_mount_path=PurePath("/tmp"),
                type=StorageType.HOST,
                nfs_server="1.2.3.4",
            )

    def test_is_nfs(self) -> None:
        config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.NFS,
            nfs_server="1.2.3.4",
            nfs_export_path=PurePath("/tmp"),
        )
        assert config.is_nfs


class TestStorageVolume:
    def test_create_storage_volume_nfs(self) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.NFS,
            nfs_server="4.3.2.1",
            nfs_export_path=PurePath("/tmp"),
        )
        registry_config = RegistryConfig()
        kube_config = KubeConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            ssh_auth_domain_name="ssh-auth.domain",
            endpoint_url="http://1.2.3.4",
            resource_pool_types=[ResourcePoolType()],
        )
        kube_orchestrator = KubeOrchestrator(
            storage_config=storage_config,
            registry_config=registry_config,
            kube_config=kube_config,
        )
        volume = kube_orchestrator.create_storage_volume()
        assert volume == NfsVolume(
            name="storage", path=PurePath("/tmp"), server="4.3.2.1"
        )

    def test_create_storage_volume_host(self) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"), type=StorageType.HOST
        )
        registry_config = RegistryConfig()
        kube_config = KubeConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            ssh_auth_domain_name="ssh-auth.domain",
            endpoint_url="http://1.2.3.4",
            resource_pool_types=[ResourcePoolType()],
        )
        kube_orchestrator = KubeOrchestrator(
            storage_config=storage_config,
            registry_config=registry_config,
            kube_config=kube_config,
        )
        volume = kube_orchestrator.create_storage_volume()
        assert volume == HostVolume(name="storage", path=PurePath("/tmp"))


class TestEnvironConfigFactory:
    def test_create_key_error(self) -> None:
        environ: Dict[str, str] = {}
        with pytest.raises(KeyError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_defaults(self) -> None:
        environ = {
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL": "http://neu.ro/oauth/authorize",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME_TEMPLATE": "{job_id}.jobs.domain",
            "NP_K8S_SSH_AUTH_INGRESS_DOMAIN_NAME": "ssh-auth.domain",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_OAUTH_BASE_URL": "https://oauth",
            "NP_OAUTH_CLIENT_ID": "oauth_client_id",
            "NP_OAUTH_AUDIENCE": "https://platform-url",
            "NP_OAUTH_SUCCESS_REDIRECT_URL": "https://platform-default-url",
            "NP_OAUTH_HEADLESS_CALLBACK_URL": "https://dev.neu.ro/oauth/show-code",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_NOTIFICATIONS_URL": "http://notifications:8080",
            "NP_NOTIFICATIONS_TOKEN": "token",
        }
        config = EnvironConfigFactory(environ=environ).create()
        cluster = EnvironConfigFactory(environ=environ).create_cluster()

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 8080

        assert cluster.storage.host_mount_path == PurePath("/tmp")
        assert cluster.storage.container_mount_path == PurePath("/var/storage")
        assert cluster.storage.uri_scheme == "storage"

        assert config.jobs.deletion_delay_s == 86400
        assert config.jobs.deletion_delay == timedelta(days=1)
        assert config.jobs.orphaned_job_owner == "compute"

        assert config.notifications.url == URL("http://notifications:8080")
        assert config.notifications.token == "token"

        assert isinstance(cluster.orchestrator, KubeConfig)
        assert cluster.orchestrator.endpoint_url == "https://localhost:8443"
        assert not cluster.orchestrator.cert_authority_data_pem
        assert not cluster.orchestrator.cert_authority_path
        assert not cluster.orchestrator.auth_cert_path
        assert not cluster.orchestrator.auth_cert_key_path
        assert cluster.orchestrator.namespace == "default"
        assert cluster.orchestrator.client_conn_timeout_s == 300
        assert cluster.orchestrator.client_read_timeout_s == 300
        assert cluster.orchestrator.jobs_ingress_class == "traefik"
        assert cluster.orchestrator.jobs_ingress_oauth_url == URL(
            "http://neu.ro/oauth/authorize"
        )
        assert cluster.orchestrator.client_conn_pool_size == 100
        assert not cluster.orchestrator.is_http_ingress_secure
        assert cluster.orchestrator.jobs_domain_name_template == "{job_id}.jobs.domain"
        assert cluster.orchestrator.ssh_auth_domain_name == "ssh-auth.domain"

        assert cluster.orchestrator.resource_pool_types == [ResourcePoolType()]
        assert cluster.orchestrator.node_label_gpu is None
        assert cluster.orchestrator.node_label_preemptible is None

        assert config.database.redis is None

        assert config.env_prefix == "NP"

        assert config.auth.server_endpoint_url == URL("https://auth")
        assert config.auth.service_token == "token"
        assert config.auth.service_name == "compute"

        assert config.oauth is not None
        assert config.oauth.base_url == URL("https://oauth")
        assert config.oauth.client_id == "oauth_client_id"
        assert config.oauth.audience == "https://platform-url"
        assert config.oauth.success_redirect_url == URL("https://platform-default-url")
        assert config.oauth.headless_callback_url == URL(
            "https://dev.neu.ro/oauth/show-code"
        )

        assert cluster.registry.host == "registry.dev.neuromation.io"

        assert config.config_client is not None

    def test_create_value_error_invalid_port(self) -> None:
        environ = {
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_API_PORT": "port",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL": "http://neu.ro/oauth/authorize",
        }
        with pytest.raises(ValueError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_custom(self, cert_authority_path: str) -> None:
        environ = {
            "NP_ENV_PREFIX": "TEST",
            "NP_API_PORT": "1111",
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_STORAGE_CONTAINER_MOUNT_PATH": "/opt/storage",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_K8S_CA_PATH": cert_authority_path,
            "NP_K8S_AUTH_CERT_PATH": "/cert_path",
            "NP_K8S_AUTH_CERT_KEY_PATH": "/cert_key_path",
            "NP_K8S_NS": "other",
            "NP_K8S_CLIENT_CONN_TIMEOUT": "111",
            "NP_K8S_CLIENT_READ_TIMEOUT": "222",
            "NP_K8S_CLIENT_CONN_POOL_SIZE": "333",
            "NP_K8S_JOBS_INGRESS_CLASS": "nginx",
            "NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL": "http://neu.ro/oauth/authorize",
            "NP_K8S_JOBS_INGRESS_HTTPS": "True",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME_TEMPLATE": "{job_id}.jobs.domain",
            "NP_K8S_SSH_AUTH_INGRESS_DOMAIN_NAME": "ssh-auth.domain",
            "NP_K8S_JOB_DELETION_DELAY": "3600",
            "NP_DB_REDIS_URI": "redis://localhost:6379/0",
            "NP_DB_REDIS_CONN_POOL_SIZE": "444",
            "NP_DB_REDIS_CONN_TIMEOUT": "555",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_AUTH_NAME": "servicename",
            "NP_REGISTRY_HOST": "testregistry:5000",
            "NP_REGISTRY_HTTPS": "True",
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
            "NP_K8S_NODE_LABEL_PREEMPTIBLE": "testpreempt",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_OAUTH_HEADLESS_CALLBACK_URL": "https://oauth/show-code",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_NOTIFICATIONS_URL": "http://notifications:8080",
            "NP_NOTIFICATIONS_TOKEN": "token",
        }
        config = EnvironConfigFactory(environ=environ).create()
        cluster = EnvironConfigFactory(environ=environ).create_cluster()

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 1111

        assert cluster.storage.host_mount_path == PurePath("/tmp")
        assert cluster.storage.container_mount_path == PurePath("/opt/storage")
        assert cluster.storage.uri_scheme == "storage"

        assert cluster.ingress.storage_url == URL("https://neu.ro/api/v1/storage")
        # assert cluster.ingress.users_url == URL("https://neu.ro/api/v1/users")
        assert cluster.ingress.monitoring_url == URL("https://neu.ro/api/v1/jobs")

        assert config.jobs.deletion_delay_s == 3600
        assert config.jobs.deletion_delay == timedelta(seconds=3600)
        assert config.jobs.orphaned_job_owner == "servicename"

        assert config.notifications.url == URL("http://notifications:8080")
        assert config.notifications.token == "token"

        assert isinstance(cluster.orchestrator, KubeConfig)
        assert cluster.orchestrator.endpoint_url == "https://localhost:8443"
        assert cluster.orchestrator.cert_authority_data_pem == CA_DATA_PEM
        assert cluster.orchestrator.cert_authority_path is None  # disabled
        assert cluster.orchestrator.auth_cert_path == "/cert_path"
        assert cluster.orchestrator.auth_cert_key_path == "/cert_key_path"
        assert cluster.orchestrator.namespace == "other"
        assert cluster.orchestrator.client_conn_timeout_s == 111
        assert cluster.orchestrator.client_read_timeout_s == 222
        assert cluster.orchestrator.client_conn_pool_size == 333
        assert cluster.orchestrator.jobs_ingress_class == "nginx"
        assert cluster.orchestrator.jobs_ingress_oauth_url == URL(
            "http://neu.ro/oauth/authorize"
        )
        assert cluster.orchestrator.is_http_ingress_secure
        assert cluster.orchestrator.jobs_domain_name_template == "{job_id}.jobs.domain"
        assert cluster.orchestrator.ssh_auth_domain_name == "ssh-auth.domain"

        assert cluster.orchestrator.resource_pool_types == [
            ResourcePoolType(),
            ResourcePoolType(gpu=1, gpu_model=GKEGPUModels.K80.value.id),
            ResourcePoolType(gpu=1, gpu_model="unknown"),
            ResourcePoolType(gpu=1, gpu_model=GKEGPUModels.V100.value.id),
        ]
        assert cluster.orchestrator.node_label_gpu == "testlabel"
        assert cluster.orchestrator.node_label_preemptible == "testpreempt"

        assert config.database.redis is not None
        assert config.database.redis.uri == "redis://localhost:6379/0"
        assert config.database.redis.conn_pool_size == 444
        assert config.database.redis.conn_timeout_s == 555.0

        assert config.env_prefix == "TEST"

        assert config.auth.server_endpoint_url == URL("https://auth")
        assert config.auth.service_token == "token"
        assert config.auth.service_name == "servicename"

        assert cluster.registry.email == "registry@neuromation.io"
        assert cluster.registry.host == "testregistry:5000"
        assert cluster.registry.url == URL("https://testregistry:5000")

        assert config.config_client is not None

    def test_create_nfs(self) -> None:
        environ = {
            "NP_STORAGE_TYPE": "nfs",
            "NP_STORAGE_NFS_SERVER": "1.2.3.4",
            "NP_STORAGE_NFS_PATH": "/tmp",
            "NP_STORAGE_HOST_MOUNT_PATH": "/tmp",
            "NP_K8S_API_URL": "https://localhost:8443",
            "NP_K8S_JOBS_INGRESS_DOMAIN_NAME_TEMPLATE": "{job_id}.jobs.domain",
            "NP_JOBS_INGRESS_OAUTH_AUTHORIZE_URL": "http://neu.ro/oauth/authorize",
            "NP_K8S_SSH_AUTH_INGRESS_DOMAIN_NAME": "ssh-auth.domain",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_OAUTH_HEADLESS_CALLBACK_URL": "https://oauth/show-code",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_NOTIFICATIONS_URL": "http://notifications:8080",
            "NP_NOTIFICATIONS_TOKEN": "token",
        }
        cluster = EnvironConfigFactory(environ=environ).create_cluster()
        assert cluster.storage.nfs_server == "1.2.3.4"
        assert cluster.storage.nfs_export_path == PurePath("/tmp")

    def test_create_ssh_auth(self) -> None:
        environ = {
            "NP_PLATFORM_API_URL": "http://neu.ro/api/v1",
            "NP_AUTH_URL": "http://auth.com",
            "NP_AUTH_TOKEN": "auth-token",
            "NP_AUTH_NAME": "auth-name",
            "NP_LOG_FIFO": "log.txt",
            "NP_K8S_NS": "other",
        }
        config = EnvironConfigFactory(environ=environ).create_ssh_auth()
        assert config.platform.server_endpoint_url == URL("http://neu.ro/api/v1")
        assert config.auth.server_endpoint_url == URL("http://auth.com")
        assert config.auth.service_token == "auth-token"
        assert config.auth.service_name == "auth-name"
        assert config.log_fifo == PurePath("log.txt")
        assert config.env_prefix == "NP"  # default
        assert config.jobs_namespace == "other"

    def test_registry_config_invalid_missing_host(self) -> None:
        with pytest.raises(ValueError, match="missing url hostname"):
            RegistryConfig(url=URL("registry.com"))

    def test_registry_config_host_default_port(self) -> None:
        config = RegistryConfig(url=URL("http://registry.com"))
        assert config.host == "registry.com"

    def test_registry_config_host_default_port_explicit(self) -> None:
        config = RegistryConfig(url=URL("http://registry.com:80"))
        assert config.host == "registry.com:80"

    def test_registry_config_host_custom_port(self) -> None:
        config = RegistryConfig(url=URL("http://registry.com:5000"))
        assert config.host == "registry.com:5000"


class TestOrchestratorConfig:
    def test_default_presets(self) -> None:
        config = OrchestratorConfig(
            jobs_domain_name_template="test",
            ssh_auth_domain_name="test",
            resource_pool_types=(),
        )
        assert config.presets == DEFAULT_PRESETS

    def test_custom_presets(self) -> None:
        presets = (Preset(name="test", cpu=1.0, memory_mb=1024),)
        config = OrchestratorConfig(
            jobs_domain_name_template="test",
            ssh_auth_domain_name="test",
            resource_pool_types=(ResourcePoolType(presets=presets),),
        )
        assert config.presets == presets


class TestKubeConfig:
    def test_missing_api_url(self) -> None:
        with pytest.raises(ValueError, match="Missing required settings"):
            KubeConfig(
                endpoint_url="",
                cert_authority_data_pem="value",
                cert_authority_path="value",
                auth_cert_path="value",
                auth_cert_key_path="value",
                token="value",
                token_path="value",
                namespace="value",
                jobs_domain_name_template="value",
                ssh_auth_domain_name="value",
                resource_pool_types=[],
                node_label_gpu="value",
                node_label_preemptible="value",
                jobs_ingress_oauth_url=URL("value"),
            )

    def test_traefik_missing_jobs_ingress_oauth_url(self) -> None:
        with pytest.raises(ValueError, match="Missing required settings"):
            KubeConfig(
                endpoint_url="value",
                cert_authority_data_pem="value",
                cert_authority_path="value",
                auth_cert_path="value",
                auth_cert_key_path="value",
                token="value",
                token_path="value",
                namespace="value",
                jobs_domain_name_template="value",
                ssh_auth_domain_name="value",
                resource_pool_types=[],
                node_label_gpu="value",
                node_label_preemptible="value",
                jobs_ingress_class="traefik",
                jobs_ingress_oauth_url=URL(""),
            )

    def test_non_traefik_missing_jobs_ingress_oauth_url(self) -> None:
        # does not raise ValueError
        KubeConfig(
            endpoint_url="value",
            cert_authority_data_pem="value",
            cert_authority_path="value",
            auth_cert_path="value",
            auth_cert_key_path="value",
            token="value",
            token_path="value",
            namespace="value",
            jobs_domain_name_template="value",
            ssh_auth_domain_name="value",
            resource_pool_types=[],
            node_label_gpu="value",
            node_label_preemptible="value",
            jobs_ingress_class="nginx",
            jobs_ingress_oauth_url=URL(""),
        )
