from yarl import URL

from platform_api.cluster_config import RegistryConfig
from platform_api.orchestrator.kube_client import Container


def container_belongs_to_registry(
    container: Container, registry_config: RegistryConfig
) -> bool:
    prefix = f"{registry_config.host}/"
    return container.image.startswith(prefix)


def container_to_image_uri(
    container: Container, registry_config: RegistryConfig
) -> URL:
    assert container_belongs_to_registry(container, registry_config), "Unknown registry"
    prefix = f"{registry_config.host}/"
    repo = container.image.replace(prefix, "", 1)
    uri = URL(f"image://{repo}")
    path, *_ = uri.path.split(":", 1)
    return uri.with_path(path)
