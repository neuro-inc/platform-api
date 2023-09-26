from __future__ import annotations

import enum
from dataclasses import dataclass, field


class KubeClientAuthType(str, enum.Enum):
    NONE = "none"
    TOKEN = "token"
    CERTIFICATE = "certificate"


@dataclass(frozen=True)
class KubeConfig:
    endpoint_url: str
    cert_authority_data_pem: str | None = None
    cert_authority_path: str | None = None

    auth_type: KubeClientAuthType = KubeClientAuthType.NONE
    auth_cert_path: str | None = None
    auth_cert_key_path: str | None = None
    token: str | None = None
    token_path: str | None = None

    namespace: str = "default"

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    jobs_ingress_class: str = "traefik"
    jobs_ingress_auth_middleware: str = "ingress-auth@kubernetescrd"
    jobs_ingress_error_page_middleware: str = "error-page@kubernetescrd"
    jobs_pod_job_toleration_key: str = "platform.neuromation.io/job"
    jobs_pod_preemptible_toleration_key: str | None = None
    jobs_pod_priority_class_name: str | None = None

    storage_volume_name: str = "storage"

    node_label_gpu: str | None = None
    node_label_preemptible: str | None = None
    node_label_job: str | None = None
    node_label_node_pool: str | None = None

    image_pull_secret_name: str | None = None

    external_job_runner_image: str = "ghcr.io/neuro-inc/externaljobrunner:latest"
    external_job_runner_command: list[str] = field(default_factory=list)
    external_job_runner_args: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.endpoint_url:
            raise ValueError("Missing required settings")
