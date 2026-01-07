"""
Fixtures for accessing platform services deployed inside minikube.

These fixtures provide access to platform services (auth, admin, secrets, disk-api)
that are deployed as Kubernetes pods inside minikube. This enables tests to work
with both minikube and vcluster environments, since services running inside k8s
can access vcluster's internal DNS.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest
from jose import jwt
from neuro_admin_client import AdminClient
from neuro_auth_client import AuthClient
from neuro_config_client import ConfigClient
from neuro_notifications_client import Client as NotificationsClient
from yarl import URL

from platform_api.config import AuthConfig

if TYPE_CHECKING:
    pass


def get_service_url(service_name: str, namespace: str = "default") -> str:
    """
    Get the localhost-accessible URL for a LoadBalancer service in minikube.

    This function uses `minikube service --url` to get the URL that can be used
    to access a LoadBalancer service from outside the cluster.

    Args:
        service_name: The name of the Kubernetes service
        namespace: The namespace where the service is deployed

    Returns:
        The URL string (e.g., "http://127.0.0.1:12345")

    Raises:
        pytest.fail: If the service is not available within the timeout
    """
    timeout_s = 120
    interval_s = 10
    elapsed = 0

    while elapsed < timeout_s:
        process = subprocess.Popen(
            ("minikube", "service", "-n", namespace, service_name, "--url"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setpgrp,
        )
        stdout = process.stdout
        assert stdout is not None
        output = stdout.readline()

        if output:
            url = output.decode().strip()
            if url:
                # Sometimes minikube prefixes output with status messages
                start_idx = url.find("http")
                if start_idx > 0:
                    url = url[start_idx:]
                # Clean up the process
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                return url

        # Clean up the process before retry
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

        time.sleep(interval_s)
        elapsed += interval_s

    pytest.fail(f"Service {service_name} is unavailable after {timeout_s}s.")


def create_token(name: str) -> str:
    """Create a JWT token for the given identity."""
    payload = {"identity": name}
    return jwt.encode(payload, "secret", algorithm="HS256")


@pytest.fixture(scope="session")
def k8s_auth_url() -> URL:
    """Get the URL for the platformauthapi service deployed in minikube."""
    url_str = get_service_url("platformauthapi")
    return URL(url_str)


@pytest.fixture(scope="session")
def k8s_admin_url() -> URL:
    """Get the URL for the platformadmin service deployed in minikube."""
    url_str = get_service_url("platformadmin")
    return URL(url_str) / "apis/admin/v1"


@pytest.fixture(scope="session")
def k8s_secrets_url() -> URL:
    """Get the URL for the platformsecrets service deployed in minikube."""
    url_str = get_service_url("platformsecrets")
    return URL(url_str)


@pytest.fixture(scope="session")
def k8s_diskapi_url() -> URL:
    """Get the URL for the platformdiskapi service deployed in minikube."""
    url_str = get_service_url("platformdiskapi")
    return URL(url_str)


@pytest.fixture(scope="session")
def k8s_auth_config(k8s_auth_url: URL) -> AuthConfig:
    """Create AuthConfig for the K8s-deployed auth service."""
    token = create_token("compute")
    public_endpoint_url = URL("https://neu.ro/api/v1/users")
    return AuthConfig(
        server_endpoint_url=k8s_auth_url,
        service_token=token,
        public_endpoint_url=public_endpoint_url,
    )


@asynccontextmanager
async def create_k8s_auth_client(config: AuthConfig) -> AsyncIterator[AuthClient]:
    """Create an AuthClient for the K8s-deployed auth service."""
    async with AuthClient(
        url=config.server_endpoint_url, token=config.service_token
    ) as client:
        yield client


@asynccontextmanager
async def create_k8s_admin_client(
    url: URL, service_token: str
) -> AsyncIterator[AdminClient]:
    """Create an AdminClient for the K8s-deployed admin service."""
    async with AdminClient(base_url=url, service_token=service_token) as client:
        yield client


@pytest.fixture(scope="session")
async def k8s_auth_client(k8s_auth_config: AuthConfig) -> AsyncIterator[AuthClient]:
    """Create an AuthClient for the K8s-deployed auth service."""
    async with create_k8s_auth_client(k8s_auth_config) as client:
        yield client


@pytest.fixture(scope="session")
async def k8s_admin_client(
    k8s_admin_url: URL,
) -> AsyncIterator[AdminClient]:
    """Create an AdminClient for the K8s-deployed admin service.

    Uses admin token to have full permissions for creating orgs, clusters,
    org_clusters, and cluster_users.
    """
    admin_token = create_token("admin")
    async with create_k8s_admin_client(k8s_admin_url, admin_token) as client:
        yield client


@pytest.fixture(scope="session")
def k8s_secrets_server_url(k8s_secrets_url: URL) -> URL:
    """Get the secrets server URL from k8s-deployed service."""
    return k8s_secrets_url


@pytest.fixture(scope="session")
def k8s_disk_server_url(k8s_diskapi_url: URL) -> URL:
    """Get the disk-api server URL from k8s-deployed service."""
    return k8s_diskapi_url


@pytest.fixture(scope="session")
def k8s_config_url() -> URL:
    """Get the URL for the platformconfig service deployed in minikube."""
    url_str = get_service_url("platformconfig")
    return URL(url_str)


@pytest.fixture(scope="session")
def k8s_notifications_url() -> URL:
    """Get the URL for the notifications service deployed in minikube."""
    url_str = get_service_url("platformnotifications")
    return URL(url_str)


@pytest.fixture(scope="session")
def k8s_postgres_api_dsn() -> str:
    url_str = get_service_url("postgres-api")
    url = URL(url_str)
    return f"postgresql+asyncpg://postgres:postgres@{url.host}:{url.port}/postgres"


@asynccontextmanager
async def create_k8s_config_client(url: URL) -> AsyncIterator[ConfigClient]:
    """Create a ConfigClient for the K8s-deployed config service."""
    token = create_token("admin")
    client = ConfigClient(url=url, token=token)
    async with client:
        yield client


@pytest.fixture(scope="session")
async def k8s_config_client(k8s_config_url: URL) -> AsyncIterator[ConfigClient]:
    """Create a ConfigClient for the K8s-deployed config service."""
    async with create_k8s_config_client(k8s_config_url) as client:
        yield client


@asynccontextmanager
async def create_k8s_notifications_client(
    url: URL,
) -> AsyncIterator[NotificationsClient]:
    """Create a ConfigClient for the K8s-deployed config service."""
    token = create_token("notifications")
    client = NotificationsClient(url=url, token=token)
    async with client:
        yield client


@pytest.fixture(scope="session")
async def k8s_notifications_client(
    k8s_notifications_url: URL,
) -> AsyncIterator[NotificationsClient]:
    """Create a NotificationsClient for the K8s-deployed notifications service."""
    async with create_k8s_notifications_client(k8s_notifications_url) as client:
        yield client
