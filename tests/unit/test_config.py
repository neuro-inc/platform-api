from pathlib import PurePath

import pytest

from platform_api.config import EnvironConfigFactory


class TestEnvironConfigFactory:
    def test_create_key_error(self):
        environ = {}
        with pytest.raises(KeyError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_defaults(self):
        environ = {
            'NP_STORAGE_HOST_MOUNT_PATH': '/tmp',
            'NP_K8S_API_URL': 'https://localhost:8443',
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.server.host == '0.0.0.0'
        assert config.server.port == 8080

        assert config.storage.host_mount_path == PurePath('/tmp')
        assert config.storage.container_mount_path == PurePath('/var/storage')
        assert config.storage.uri_scheme == 'storage'

        assert config.orchestrator.storage_mount_path == PurePath('/tmp')
        assert config.orchestrator.endpoint_url == 'https://localhost:8443'
        assert not config.orchestrator.cert_authority_path
        assert not config.orchestrator.auth_cert_path
        assert not config.orchestrator.auth_cert_key_path
        assert config.orchestrator.namespace == 'default'
        assert config.orchestrator.client_conn_timeout_s == 300
        assert config.orchestrator.client_read_timeout_s == 300
        assert config.orchestrator.client_conn_pool_size == 100

        assert config.env_prefix == 'NP'

    def test_create_value_error(self):
        environ = {
            'NP_STORAGE_HOST_MOUNT_PATH': '/tmp',
            'NP_API_PORT': 'port',
            'NP_K8S_API_URL': 'https://localhost:8443',
        }
        with pytest.raises(ValueError):
            EnvironConfigFactory(environ=environ).create()

    def test_create_custom(self):
        environ = {
            'NP_ENV_PREFIX': 'TEST',
            'NP_API_PORT': '1111',
            'NP_STORAGE_HOST_MOUNT_PATH': '/tmp',
            'NP_STORAGE_CONTAINER_MOUNT_PATH': '/opt/storage',
            'NP_STORAGE_URI_SCHEME': 'something',

            'NP_K8S_API_URL': 'https://localhost:8443',
            'NP_K8S_CA_PATH': '/ca_path',

            'NP_K8S_AUTH_CERT_PATH': '/cert_path',
            'NP_K8S_AUTH_CERT_KEY_PATH': '/cert_key_path',
            'NP_K8S_NS': 'other',
            'NP_K8S_CLIENT_CONN_TIMEOUT': 111,
            'NP_K8S_CLIENT_READ_TIMEOUT': 222,
            'NP_K8S_CLIENT_CONN_POOL_SIZE': 333,
        }
        config = EnvironConfigFactory(environ=environ).create()

        assert config.server.host == '0.0.0.0'
        assert config.server.port == 1111

        assert config.storage.host_mount_path == PurePath('/tmp')
        assert config.storage.container_mount_path == PurePath('/opt/storage')
        assert config.storage.uri_scheme == 'something'

        assert config.orchestrator.storage_mount_path == PurePath('/tmp')
        assert config.orchestrator.endpoint_url == 'https://localhost:8443'
        assert config.orchestrator.cert_authority_path == '/ca_path'
        assert config.orchestrator.auth_cert_path == '/cert_path'
        assert config.orchestrator.auth_cert_key_path == '/cert_key_path'
        assert config.orchestrator.namespace == 'other'
        assert config.orchestrator.client_conn_timeout_s == 111
        assert config.orchestrator.client_read_timeout_s == 222
        assert config.orchestrator.client_conn_pool_size == 333

        assert config.env_prefix == 'TEST'
