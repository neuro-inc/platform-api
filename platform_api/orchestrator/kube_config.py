import enum
from dataclasses import dataclass
from typing import Optional

from yarl import URL


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: Optional[str] = None
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.NONE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token: Optional[str] = None
    token_path: Optional[str] = None

    namespace: str = "default"

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    jobs_ingress_class: str = "traefik"
    jobs_ingress_oauth_url: URL = URL(
        "https://neu.ro/oauth/authorize"
    )  # TODO: not used in traefik v2
    jobs_ingress_auth_middleware: str = "ingress-auth@kubernetescrd"
    jobs_ingress_error_page_middleware: str = "error-page@kubernetescrd"
    jobs_pod_job_toleration_key: str = "platform.neuromation.io/job"
    jobs_pod_preemptible_toleration_key: Optional[str] = None
    jobs_pod_priority_class_name: Optional[str] = None

    storage_volume_name: str = "storage"

    node_label_gpu: Optional[str] = None
    node_label_preemptible: Optional[str] = None
    node_label_job: Optional[str] = None
    node_label_node_pool: Optional[str] = None

    image_pull_secret_name: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.endpoint_url or (
            self.jobs_ingress_class == "traefik" and not self.jobs_ingress_oauth_url
        ):
            raise ValueError("Missing required settings")
