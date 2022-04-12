from datetime import timedelta
from decimal import Decimal
from pathlib import PurePath

import pytest
from yarl import URL

from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import (
    RegistryConfig,
    SentryConfig,
    StorageConfig,
    StorageType,
    ZipkinConfig,
)
from platform_api.config_factory import EnvironConfigFactory
from platform_api.orchestrator.job_request import ContainerVolume
from platform_api.orchestrator.kube_client import SecretVolume
from platform_api.orchestrator.kube_config import KubeClientAuthType
from platform_api.orchestrator.kube_orchestrator import (
    HostVolume,
    KubeConfig,
    KubeOrchestrator,
    NfsVolume,
    PVCVolume,
)
from platform_api.resource import ResourcePoolType


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
    def test_create_storage_volume_nfs(self, registry_config: RegistryConfig) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.NFS,
            nfs_server="4.3.2.1",
            nfs_export_path=PurePath("/tmp"),
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        container_volume = ContainerVolume.create(
            "storage://neuromation/public",
            dst_path=PurePath("/vat/storage"),
            read_only=True,
        )
        volume = kube_orchestrator.create_storage_volume(container_volume)
        assert volume == NfsVolume(
            name="storage", path=None, server="4.3.2.1", export_path=PurePath("/tmp")
        )

    def test_create_storage_volume_host(self, registry_config: RegistryConfig) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"), type=StorageType.HOST
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        container_volume = ContainerVolume.create(
            "storage://neuromation/public",
            dst_path=PurePath("/vat/storage"),
            read_only=True,
        )
        volume = kube_orchestrator.create_storage_volume(container_volume)
        assert volume == HostVolume(
            name="storage", path=None, host_path=PurePath("/tmp")
        )

    def test_create_storage_volume_pvc(self, registry_config: RegistryConfig) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"), type=StorageType.PVC, pvc_name="testclaim"
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        container_volume = ContainerVolume.create(
            "storage://neuromation/public",
            dst_path=PurePath("/vat/storage"),
            read_only=True,
        )
        volume = kube_orchestrator.create_storage_volume(container_volume)
        assert volume == PVCVolume(name="storage", path=None, claim_name="testclaim")

    def test_create_main_storage_volume(self, registry_config: RegistryConfig) -> None:
        main_storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.PVC,
            pvc_name="main-claim",
        )
        isolated_storage_config = StorageConfig(
            path=PurePath("/user_other"),
            host_mount_path=PurePath("/tmp"),
            type=StorageType.PVC,
            pvc_name="user-other-claim",
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[main_storage_config, isolated_storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        container_volume = ContainerVolume.create(
            "storage://cluster/user",
            dst_path=PurePath("/vat/storage"),
            read_only=True,
        )
        volume = kube_orchestrator.create_storage_volume(container_volume)
        assert volume == PVCVolume(name="storage", path=None, claim_name="main-claim")

    def test_create_extra_storage_volume(self, registry_config: RegistryConfig) -> None:
        main_storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"),
            type=StorageType.PVC,
            pvc_name="main-claim",
        )
        isolated_storage_config = StorageConfig(
            path=PurePath("/isolated"),
            host_mount_path=PurePath("/tmp"),
            type=StorageType.PVC,
            pvc_name="isolated-claim",
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[main_storage_config, isolated_storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        container_volume = ContainerVolume.create(
            "storage://cluster/isolated/dir",
            dst_path=PurePath("/vat/storage"),
            read_only=True,
        )
        volume = kube_orchestrator.create_storage_volume(container_volume)
        assert volume == PVCVolume(
            name="storage-isolated",
            path=PurePath("/isolated"),
            claim_name="isolated-claim",
        )


class TestSecretVolume:
    def test_create_secret_volume(self, registry_config: RegistryConfig) -> None:
        storage_config = StorageConfig(
            host_mount_path=PurePath("/tmp"), type=StorageType.PVC, pvc_name="testclaim"
        )
        orchestrator_config = OrchestratorConfig(
            jobs_domain_name_template="{job_id}.testdomain",
            jobs_internal_domain_name_template="{job_id}.testinternaldomain",
            resource_pool_types=[ResourcePoolType()],
            presets=[],
        )
        kube_config = KubeConfig(endpoint_url="http://1.2.3.4")
        kube_orchestrator = KubeOrchestrator(
            storage_configs=[storage_config],
            registry_config=registry_config,
            orchestrator_config=orchestrator_config,
            kube_config=kube_config,
        )
        user_name = "test-user"
        volume = kube_orchestrator.create_secret_volume(user_name)
        assert volume == SecretVolume(
            name="user--test-user--secrets", k8s_secret_name="user--test-user--secrets"
        )


class TestEnvironConfigFactory:
    def test_create_key_error(self) -> None:
        environ: dict[str, str] = {}
        with pytest.raises(KeyError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_defaults(self) -> None:
        environ = {
            "NP_OAUTH_AUTH_URL": "https://oauth-auth",
            "NP_OAUTH_TOKEN_URL": "https://oauth-token",
            "NP_OAUTH_LOGOUT_URL": "https://oauth-logout",
            "NP_OAUTH_CLIENT_ID": "oauth_client_id",
            "NP_OAUTH_AUDIENCE": "https://platform-url",
            "NP_OAUTH_SUCCESS_REDIRECT_URL": "https://platform-default-url",
            "NP_OAUTH_HEADLESS_CALLBACK_URL": "https://dev.neu.ro/oauth/show-code",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_AUTH_URL": "-",
            "NP_AUTH_PUBLIC_URL": "-",
            "NP_ADMIN_URL": "-",
            "NP_ADMIN_PUBLIC_URL": "-",
            "NP_ENFORCER_PLATFORM_API_URL": "http://platformapi:8080/api/v1",
            "NP_ENFORCER_CREDIT_NOTIFICATION_THRESHOLD": "200.33",
            "NP_ENFORCER_RETENTION_DELAY_DAYS": "200",
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.config_url == URL("http://platformconfig:8080/api/v1")
        assert config.admin_url is None
        assert config.admin_public_url is None

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 8080

        assert config.jobs.deletion_delay_s == 900
        assert config.jobs.deletion_delay == timedelta(minutes=15)
        assert config.jobs.orphaned_job_owner == "compute"

        assert config.job_policy_enforcer.platform_api_url == URL(
            "http://platformapi:8080/api/v1"
        )
        assert config.job_policy_enforcer.credit_notification_threshold == Decimal(
            "200.33"
        )
        assert config.job_policy_enforcer.retention_delay_days == 200
        assert config.notifications.url == URL()
        assert config.notifications.token == ""

        assert config.auth.server_endpoint_url is None
        assert config.auth.public_endpoint_url is None
        assert config.auth.service_token == ""
        assert config.auth.service_name == "compute"

        assert config.oauth is not None
        assert config.oauth.auth_url == URL("https://oauth-auth")
        assert config.oauth.token_url == URL("https://oauth-token")
        assert config.oauth.logout_url == URL("https://oauth-logout")
        assert config.oauth.client_id == "oauth_client_id"
        assert config.oauth.audience == "https://platform-url"
        assert config.oauth.success_redirect_url == URL("https://platform-default-url")
        assert config.oauth.headless_callback_url == URL(
            "https://dev.neu.ro/oauth/show-code"
        )

        assert not config.cors.allowed_origins

        assert config.zipkin is None
        assert config.sentry is None

    def test_create_value_error_invalid_port(self) -> None:
        environ = {
            "NP_API_PORT": "port",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_TOKEN": "token",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_ADMIN_URL": "https://neu.ro/apis/admin/v1",
            "NP_ADMIN_PUBLIC_URL": "https://neu.ro/apis/admin/v1",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_AUTH_PUBLIC_URL": "https://neu.ro/api/v1/users",
        }
        with pytest.raises(ValueError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_custom(self, cert_authority_path: str) -> None:
        environ = {
            "NP_API_PORT": "1111",
            "NP_K8S_JOB_DELETION_DELAY": "3600",
            "NP_DB_POSTGRES_DSN": "postgresql://postgres@localhost:5432/postgres",
            "NP_DB_POSTGRES_POOL_MIN": "50",
            "NP_DB_POSTGRES_POOL_MAX": "500",
            "NP_AUTH_URL": "https://auth",
            "NP_AUTH_PUBLIC_URL": "https://neu.ro/api/v1/users",
            "NP_AUTH_TOKEN": "token",
            "NP_AUTH_NAME": "servicename",
            "NP_API_URL": "https://neu.ro/api/v1",
            "NP_ADMIN_URL": "https://platform-admin:8080/apis/admin/v1",
            "NP_ADMIN_PUBLIC_URL": "https://neu.ro/apis/admin/v1",
            "NP_OAUTH_HEADLESS_CALLBACK_URL": "https://oauth/show-code",
            "NP_PLATFORM_CONFIG_URI": "http://platformconfig:8080/api/v1",
            "NP_NOTIFICATIONS_URL": "http://notifications:8080",
            "NP_NOTIFICATIONS_TOKEN": "token",
            "NP_ENFORCER_PLATFORM_API_URL": "http://platformapi:8080/api/v1",
            "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
            "NP_ZIPKIN_URL": "https://zipkin:9411",
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_CLUSTER_NAME": "test",
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.server.host == "0.0.0.0"
        assert config.server.port == 1111

        assert config.admin_url == URL("https://platform-admin:8080/apis/admin/v1")
        assert config.admin_public_url == URL("https://neu.ro/apis/admin/v1")

        assert config.jobs.deletion_delay_s == 3600
        assert config.jobs.deletion_delay == timedelta(seconds=3600)
        assert config.jobs.orphaned_job_owner == "servicename"

        assert config.job_policy_enforcer.platform_api_url == URL(
            "http://platformapi:8080/api/v1"
        )

        assert config.notifications is not None
        assert config.notifications.url == URL("http://notifications:8080")
        assert config.notifications.token == "token"

        assert config.database.postgres is not None
        async_dsn = "postgresql+asyncpg://postgres@localhost:5432/postgres"
        assert config.database.postgres.postgres_dsn == async_dsn
        assert config.database.postgres.pool_min_size == 50
        assert config.database.postgres.pool_max_size == 500

        assert config.auth.server_endpoint_url == URL("https://auth")
        assert config.auth.public_endpoint_url == URL("https://neu.ro/api/v1/users")
        assert config.auth.service_token == "token"
        assert config.auth.service_name == "servicename"

        assert config.cors.allowed_origins == ["https://domain1.com", "http://do.main"]

        assert config.zipkin
        assert config.sentry

    def test_alembic_with_escaped_symbol(self) -> None:
        async_dsn = "postgresql+asyncpg://postgres%40@localhost:5432/postgres"
        config = EnvironConfigFactory(environ={}).create_alembic(async_dsn)

        sync_dsn = "postgresql://postgres%40@localhost:5432/postgres"
        assert config.get_main_option("sqlalchemy.url") == sync_dsn

    def test_registry_default(self) -> None:
        config = EnvironConfigFactory(
            environ={
                "NP_REGISTRY_URL": "https://registry.neu.ro",
                "NP_AUTH_NAME": "user",
                "NP_AUTH_TOKEN": "token",
            }
        ).create_registry()

        assert config == RegistryConfig(
            url=URL("https://registry.neu.ro"), username="user", password="token"
        )

    def test_registry_custom(self) -> None:
        config = EnvironConfigFactory(
            environ={
                "NP_REGISTRY_URL": "https://registry.neu.ro",
                "NP_REGISTRY_EMAIL": "user@neu.ro",
                "NP_AUTH_NAME": "user",
                "NP_AUTH_TOKEN": "token",
            }
        ).create_registry()

        assert config == RegistryConfig(
            url=URL("https://registry.neu.ro"),
            username="user",
            password="token",
            email="user@neu.ro",
        )

    def test_storage_host(self) -> None:
        configs = EnvironConfigFactory(
            environ={
                "NP_STORAGE_TYPE_0": "host",
                "NP_STORAGE_HOST_MOUNT_PATH_0": "/storage",
            }
        ).create_storages()

        assert len(configs) == 1
        assert configs[0] == StorageConfig(
            type=StorageType.HOST, host_mount_path=PurePath("/storage")
        )

    def test_storage_nfs(self) -> None:
        configs = EnvironConfigFactory(
            environ={
                "NP_STORAGE_TYPE_0": "nfs",
                "NP_STORAGE_NFS_SERVER_0": "nfs-server",
                "NP_STORAGE_NFS_EXPORT_PATH_0": "/nfs",
            }
        ).create_storages()

        assert len(configs) == 1
        assert configs[0] == StorageConfig(
            type=StorageType.NFS,
            nfs_server="nfs-server",
            nfs_export_path=PurePath("/nfs"),
            host_mount_path=PurePath("/nfs"),
        )

    def test_storage_pvc(self) -> None:
        configs = EnvironConfigFactory(
            environ={"NP_STORAGE_TYPE_0": "pvc", "NP_STORAGE_PVC_NAME_0": "storage"}
        ).create_storages()

        assert len(configs) == 1
        assert configs[0] == StorageConfig(
            type=StorageType.PVC,
            pvc_name="storage",
            host_mount_path=PurePath("/mnt/storage"),
        )

    def test_multiple_storage(self) -> None:
        configs = EnvironConfigFactory(
            environ={
                "NP_STORAGE_TYPE_0": "pvc",
                "NP_STORAGE_PVC_NAME_0": "storage0",
                "NP_STORAGE_TYPE_1": "pvc",
                "NP_STORAGE_PVC_NAME_1": "storage1",
            }
        ).create_storages()

        assert configs == [
            StorageConfig(
                type=StorageType.PVC,
                pvc_name="storage0",
                host_mount_path=PurePath("/mnt/storage"),
            ),
            StorageConfig(
                type=StorageType.PVC,
                pvc_name="storage1",
                host_mount_path=PurePath("/mnt/storage"),
            ),
        ]

    def test_kube_config_default(self) -> None:
        config = EnvironConfigFactory(
            environ={"NP_KUBE_URL": "https://kubernetes.default.svc"}
        ).create_kube()

        assert config == KubeConfig(endpoint_url="https://kubernetes.default.svc")

    def test_kube_config_custom(self) -> None:
        config = EnvironConfigFactory(
            environ={
                "NP_KUBE_URL": "https://kubernetes.default.svc",
                "NP_KUBE_AUTH_TYPE": "token",
                "NP_KUBE_CA_DATA": "ca-data",
                "NP_KUBE_CA_DATA_PATH": "ca-data-path",
                "NP_KUBE_CERT_PATH": "cert-path",
                "NP_KUBE_CERT_KEY_PATH": "cert-key-path",
                "NP_KUBE_TOKEN": "token",
                "NP_KUBE_NAMESPACE": "platform-jobs",
                "NP_KUBE_CONN_TIMEOUT": "1",
                "NP_KUBE_READ_TIMEOUT": "2",
                "NP_KUBE_CONN_POOL_SIZE": "3",
                "NP_KUBE_INGRESS_CLASS": "nginx",
                "NP_KUBE_INGRESS_AUTH_MIDDLEWARE": "custom-ingress-auth@kubernetescrd",
                "NP_KUBE_INGRESS_ERROR_PAGE_MIDDLEWARE": (
                    "custom-error-page@kubernetescrd"
                ),
                "NP_KUBE_INGRESS_OAUTH_AUTHORIZE_URL": "http://ingress-auth",
                "NP_KUBE_POD_JOB_TOLERATION_KEY": "job",
                "NP_KUBE_POD_PREEMPTIBLE_TOLERATION_KEY": "preemptible",
                "NP_KUBE_POD_PRIORITY_CLASS_NAME": "job-priority",
                "NP_KUBE_NODE_LABEL_JOB": "job-label",
                "NP_KUBE_NODE_LABEL_GPU": "gpu-label",
                "NP_KUBE_NODE_LABEL_PREEMPTIBLE": "preemptible-label",
                "NP_KUBE_NODE_LABEL_NODE_POOL": "node-pool-label",
                "NP_KUBE_IMAGE_PULL_SECRET": "test-secret",
            }
        ).create_kube()

        assert config == KubeConfig(
            endpoint_url="https://kubernetes.default.svc",
            auth_type=KubeClientAuthType.TOKEN,
            cert_authority_data_pem="ca-data",
            cert_authority_path="ca-data-path",
            auth_cert_path="cert-path",
            auth_cert_key_path="cert-key-path",
            token="token",
            namespace="platform-jobs",
            client_conn_timeout_s=1,
            client_read_timeout_s=2,
            client_conn_pool_size=3,
            jobs_ingress_class="nginx",
            jobs_ingress_auth_middleware="custom-ingress-auth@kubernetescrd",
            jobs_ingress_error_page_middleware="custom-error-page@kubernetescrd",
            jobs_pod_job_toleration_key="job",
            jobs_pod_preemptible_toleration_key="preemptible",
            jobs_pod_priority_class_name="job-priority",
            node_label_job="job-label",
            node_label_gpu="gpu-label",
            node_label_preemptible="preemptible-label",
            node_label_node_pool="node-pool-label",
            image_pull_secret_name="test-secret",
        )

    def test_create_zipkin_none(self) -> None:
        result = EnvironConfigFactory({}).create_zipkin("platform-api")

        assert result is None

    def test_create_zipkin_default(self) -> None:
        env = {"NP_ZIPKIN_URL": "https://zipkin:9411"}
        result = EnvironConfigFactory(env).create_zipkin("platform-api")

        assert result == ZipkinConfig(
            url=URL("https://zipkin:9411"), app_name="platform-api"
        )

    def test_create_zipkin_custom(self) -> None:
        env = {
            "NP_ZIPKIN_URL": "https://zipkin:9411",
            "NP_ZIPKIN_APP_NAME": "api",
            "NP_ZIPKIN_SAMPLE_RATE": "1",
        }
        result = EnvironConfigFactory(env).create_zipkin("platform-api")

        assert result == ZipkinConfig(
            url=URL("https://zipkin:9411"), app_name="api", sample_rate=1
        )

    def test_create_sentry_none(self) -> None:
        result = EnvironConfigFactory({}).create_sentry("platform-api")

        assert result is None

    def test_create_sentry_default(self) -> None:
        env = {
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_CLUSTER_NAME": "test",
        }
        result = EnvironConfigFactory(env).create_sentry("platform-api")

        assert result == SentryConfig(
            dsn=URL("https://sentry"), app_name="platform-api", cluster_name="test"
        )

    def test_create_sentry_custom(self) -> None:
        env = {
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_APP_NAME": "api",
            "NP_SENTRY_CLUSTER_NAME": "test",
            "NP_SENTRY_SAMPLE_RATE": "1",
        }
        result = EnvironConfigFactory(env).create_sentry("platform-api")

        assert result == SentryConfig(
            dsn=URL("https://sentry"),
            app_name="api",
            cluster_name="test",
            sample_rate=1,
        )


class TestRegistryConfig:
    def test_registry_config_invalid_missing_host(self) -> None:
        with pytest.raises(ValueError, match="missing url hostname"):
            RegistryConfig(
                url=URL("registry.com"), username="compute", password="compute_token"
            )

    def test_registry_config_host_default_port(self) -> None:
        config = RegistryConfig(
            url=URL("http://registry.com"), username="compute", password="compute_token"
        )
        assert config.host == "registry.com"

    def test_registry_config_host_default_port_explicit(self) -> None:
        config = RegistryConfig(
            url=URL("http://registry.com:80"),
            username="compute",
            password="compute_token",
        )
        assert config.host == "registry.com:80"

    def test_registry_config_host_custom_port(self) -> None:
        config = RegistryConfig(
            url=URL("http://registry.com:5000"),
            username="compute",
            password="compute_token",
        )
        assert config.host == "registry.com:5000"


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
                node_label_gpu="value",
                node_label_preemptible="value",
            )
