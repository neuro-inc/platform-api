import pytest
from yarl import URL

from platform_api.cluster_config import RegistryConfig
from platform_api.orchestrator.kube_client import Container, ContainerResources
from platform_api.utils.kube_utils import (
    container_belongs_to_registry,
    container_to_image_uri,
)


class TestKubeUtils:
    def test_command_list_empty(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        assert container.command_list == []

    def test_command_list(self) -> None:
        container = Container(
            image="testimage",
            command="bash -c date",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        assert container.command_list == ["bash", "-c", "date"]

    def test_container_belongs_to_registry_no_host(self) -> None:
        container = Container(
            image="testimage", resources=ContainerResources(cpu=1, memory_mb=128)
        )
        registry_config = RegistryConfig(host="example.com")
        assert not container_belongs_to_registry(container, registry_config)

    def test_container_belongs_to_registry_different_host(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        assert not container_belongs_to_registry(container, registry_config)

    def test_container_belongs_to_registry(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        assert container_belongs_to_registry(container, registry_config)

    def test_container_to_image_uri_failure(self) -> None:
        container = Container(
            image="registry.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        with pytest.raises(AssertionError, match="Unknown registry"):
            container_to_image_uri(container, registry_config)

    def test_container_to_image_uri(self) -> None:
        container = Container(
            image="example.com/project/testimage",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        uri = container_to_image_uri(container, registry_config)
        assert uri == URL("image://project/testimage")

    def test_container_to_image_uri_ignore_tag(self) -> None:
        container = Container(
            image="example.com/project/testimage:latest",
            resources=ContainerResources(cpu=1, memory_mb=128),
        )
        registry_config = RegistryConfig(host="example.com")
        uri = container_to_image_uri(container, registry_config)
        assert uri == URL("image://project/testimage")
