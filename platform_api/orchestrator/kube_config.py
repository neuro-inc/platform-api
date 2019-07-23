import enum
from dataclasses import dataclass
from typing import Optional

from yarl import URL

from platform_api.config import OrchestratorConfig


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig(OrchestratorConfig):
    endpoint_url: str = ""
    cert_authority_data_pem: Optional[str] = None
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token: Optional[str] = None
    token_path: Optional[str] = None

    namespace: str = "default"

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    jobs_ingress_class: str = "traefik"
    jobs_ingress_oauth_url: URL = URL()

    storage_volume_name: str = "storage"

    node_label_gpu: Optional[str] = None
    node_label_preemptible: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.endpoint_url:
            raise ValueError("Missing required settings")
