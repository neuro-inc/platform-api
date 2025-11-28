from __future__ import annotations

from apolo_kube_client import KubeConfig as ApoloKubeConfig


class KubeConfig(ApoloKubeConfig):
    jobs_ingress_class: str = "traefik"
    jobs_ingress_auth_middleware: str = "ingress-auth@kubernetescrd"
    jobs_ingress_error_page_middleware: str = "error-page@kubernetescrd"
    jobs_pod_job_toleration_key: str = "platform.neuromation.io/job"
    jobs_pod_preemptible_toleration_key: str | None = None

    node_label_preemptible: str | None = None
    node_label_job: str | None = None
    node_label_node_pool: str | None = None

    image_pull_secret_name: str | None = None

    external_job_runner_image: str = "ghcr.io/neuro-inc/externaljobrunner:latest"
    external_job_runner_command: list[str] = []
    external_job_runner_args: list[str] = []
