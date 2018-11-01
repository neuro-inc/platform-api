import logging
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from pathlib import PurePath
from typing import Dict, Optional

from ..config import OrchestratorConfig  # noqa
from .base import LogReader, Orchestrator
from .job import Job, JobStatusItem
from .job_request import Container, JobStatus
from .kube_client import *  # noqa
from .kube_client import (
    DockerRegistrySecret,
    HostVolume,
    IngressRule,
    KubeClient,
    KubeClientAuthType,
    NfsVolume,
    PodDescriptor,
    PodStatus,
    Service,
    Volume,
)
from .logs import PodContainerLogReader


logger = logging.getLogger(__name__)


class AbstractNamespaceStrategy:
    """
    Helper utility class that would take into account information on user, and would
    provide details on the namespace where Pod shall be scheduled.
    It shall in future come as a part of an umbrella strategy classes.
    """

    def provide_namespace(self, job_owner: Optional[str]) -> str:  # pragma no cover
        pass


class SingleNamespaceStrategy(AbstractNamespaceStrategy):

    default_namespace: str = "default"

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace

    def provide_namespace(self, job_owner: Optional[str]) -> str:
        return self._namespace


class JobStatusItemFactory:
    def __init__(self, pod_status: PodStatus) -> None:
        self._pod_status = pod_status
        self._container_status = pod_status.container_status

        self._status = self._parse_status()

    def _parse_status(self) -> JobStatus:
        """Map a pod phase and its container statuses to a job status.

        See
        https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-phase
        """
        phase = self._pod_status.phase
        if phase == "Succeeded":
            return JobStatus.SUCCEEDED
        elif phase in ("Failed", "Unknown"):
            return JobStatus.FAILED
        elif phase == "Running":
            return JobStatus.RUNNING
        elif phase == "Pending":
            if not self._pod_status.is_container_creating:
                return JobStatus.FAILED
        return JobStatus.PENDING

    def _parse_reason(self) -> Optional[str]:
        if self._status == JobStatus.FAILED:
            return self._container_status.reason
        return None

    def _compose_description(self) -> Optional[str]:
        if self._status == JobStatus.FAILED:
            if (
                self._container_status.is_terminated
                and self._container_status.exit_code
            ):
                description = self._container_status.message or ""
                return description + (
                    f"\nExit code: {self._container_status.exit_code}"
                )
        return None

    def create(self) -> JobStatusItem:
        return JobStatusItem.create(
            self._status,
            reason=self._parse_reason(),
            description=self._compose_description(),
        )


@dataclass(frozen=True)
class KubeConfig(OrchestratorConfig):
    jobs_ingress_name: str = ""

    endpoint_url: str = ""
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None

    namespace_provider: AbstractNamespaceStrategy = SingleNamespaceStrategy(
        SingleNamespaceStrategy.default_namespace
    )

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    storage_volume_name: str = "storage"

    job_deletion_delay_s: int = 60 * 60 * 24

    node_label_gpu: Optional[str] = None

    def __post_init__(self):
        if not all((self.jobs_ingress_name, self.endpoint_url)):
            raise ValueError("Missing required settings")

    @property
    def storage_mount_path(self) -> PurePath:
        return self.storage.host_mount_path

    @property
    def jobs_ingress_domain_name(self) -> str:
        return self.jobs_domain_name

    @property
    def ssh_ingress_domain_name(self) -> str:
        return self.ssh_domain_name

    def create_storage_volume(self) -> Volume:
        if self.storage.is_nfs:
            return NfsVolume(  # type: ignore
                name=self.storage_volume_name,
                server=self.storage.nfs_server,
                path=self.storage.nfs_export_path,
            )
        return HostVolume(  # type: ignore
            name=self.storage_volume_name, path=self.storage_mount_path
        )


def convert_pod_status_to_job_status(pod_status: PodStatus) -> JobStatusItem:
    return JobStatusItemFactory(pod_status).create()


class KubeOrchestrator(Orchestrator):
    def __init__(
        self, *, config: KubeConfig, loop: Optional[AbstractEventLoop] = None
    ) -> None:
        self._loop = loop

        self._config = config

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing

        user_name = config.orphaned_job_owner
        namespace_provider = config.namespace_provider
        client_namespace = namespace_provider.provide_namespace(user_name)

        self._client = KubeClient(
            base_url=config.endpoint_url,
            cert_authority_path=config.cert_authority_path,
            auth_type=config.auth_type,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,
            namespace=client_namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size,
        )

        self._storage_volume = self._config.create_storage_volume()

    @property
    def config(self) -> KubeConfig:
        return self._config

    async def __aenter__(self) -> "KubeOrchestrator":
        await self._client.init()
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.close()

    async def start_job(self, job: Job, token: str) -> JobStatus:
        pod_namespace = self._config.namespace_provider.provide_namespace(job.owner)
        secret = DockerRegistrySecret(
            name=job.owner,
            password=token,
            namespace=pod_namespace,
            email=self._config.registry.email,
            registry_server=self._config.registry.host,
        )
        await self._client.create_secret(secret)
        secret_names = [secret.objname]
        node_selector = await self._get_pod_node_selector(job.request.container)
        descriptor = PodDescriptor.from_job_request(
            self._storage_volume, job.request, secret_names, node_selector=node_selector
        )
        status = await self._client.create_pod(descriptor)
        if job.has_http_server_exposed or job.has_ssh_server_exposed:
            logger.info(f"Starting Service for {job.id}.")
            service = await self._create_service(descriptor)
            if job.has_http_server_exposed:
                logger.info(f"Starting Ingress for {job.id}")
                await self._client.add_ingress_rule(
                    name=self._config.jobs_ingress_name,
                    rule=IngressRule.from_service(
                        domain_name=self._config.jobs_ingress_domain_name,
                        service=service,
                    ),
                )
        job.status = convert_pod_status_to_job_status(status).status
        return job.status

    async def _get_pod_node_selector(self, container: Container) -> Dict[str, str]:
        selector: Dict[str, str] = {}

        if not self._config.node_label_gpu:
            return selector

        pool_types = await self.get_resource_pool_types()
        for pool_type in pool_types:
            if container.resources.check_fit_into_pool_type(pool_type):
                if pool_type.gpu_model:
                    selector[self._config.node_label_gpu] = pool_type.gpu_model.id
                break

        return selector

    async def get_job_status(self, job_id: str) -> JobStatusItem:
        pod_id = job_id
        status = await self._client.get_pod_status(pod_id)
        return convert_pod_status_to_job_status(status)

    async def get_job_log_reader(self, job: Job) -> LogReader:
        return PodContainerLogReader(
            client=self._client, pod_name=job.id, container_name=job.id
        )

    async def _create_service(self, pod: PodDescriptor) -> Service:
        return await self._client.create_service(Service.create_for_pod(pod))

    def _get_ingress_rule_host_for_pod(self, pod_id) -> str:
        ingress_rule = IngressRule.from_service(
            domain_name=self._config.jobs_ingress_domain_name,
            service=Service(name=pod_id, target_port=0),  # type: ignore
        )
        return ingress_rule.host

    async def _delete_service(self, pod_id: str) -> None:
        # TODO (Rafa) we shall ensure that ingress exists, as it is not required
        # for SSH, thus Pods without HTTP but thus which are having SSH,
        # will not have it
        host = self._get_ingress_rule_host_for_pod(pod_id)
        try:
            await self._client.remove_ingress_rule(
                name=self._config.jobs_ingress_name, host=host
            )
        except Exception:
            logger.exception(f"Failed to remove ingress rule {host}")
        try:
            await self._client.delete_service(name=pod_id)
        except Exception:
            logger.exception(f"Failed to remove service {pod_id}")

    async def delete_job(self, job: Job) -> JobStatus:
        pod_id = job.id
        if job.has_http_server_exposed or job.has_ssh_server_exposed:
            await self._delete_service(pod_id)
        status = await self._client.delete_pod(pod_id)
        return convert_pod_status_to_job_status(status).status
