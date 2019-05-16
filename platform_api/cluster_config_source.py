from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence

from yarl import URL

from .cluster_config import ClusterConfig
from .config_client import ConfigClient


class ClusterConfigSource(ABC):
    @abstractmethod
    async def get_cluster_configs(self) -> Sequence[ClusterConfig]:
        pass


@dataclass(frozen=True)
class ClusterConfigFromPlatformConfig(ClusterConfigSource):
    platform_config_url: URL
    users_url: URL
    ssh_domain_name: str

    async def get_cluster_configs(self) -> Sequence[ClusterConfig]:
        async with self.create_config_client() as client:
            return await client.get_clusters(
                users_url=self.users_url, ssh_domain_name=self.ssh_domain_name
            )

    def create_config_client(self) -> ConfigClient:
        return ConfigClient(base_url=self.platform_config_url)
