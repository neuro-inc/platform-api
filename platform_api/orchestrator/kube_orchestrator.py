import logging
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from pathlib import PurePath
from typing import Optional

from ..config import OrchestratorConfig  # noqa
from .base import LogReader, Orchestrator
from .job import Job, JobStatusItem
from .job_request import JobStatus
from .kube_client import *  # noqa
from .kube_client import (
    HostVolume, IngressRule, KubeClient, KubeClientAuthType, NfsVolume,
    PodDescriptor, PodStatus, Service, Volume
)
from .logs import PodContainerLogReader


logger = logging.getLogger(__name__)


class JobStatusItemFactory:
    def __init__(self, pod_status: PodStatus) -> None:
        self._pod_status = pod_status

    def _parse_status(self) -> JobStatus:
        """Map a pod phase and its container statuses to a job status.

        See
        https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-phase
        """
        phase = self._pod_status.phase
        if phase == 'Succeeded':
            return JobStatus.SUCCEEDED
        elif phase in ('Failed', 'Unknown'):
            return JobStatus.FAILED
        elif phase == 'Running':
            return JobStatus.RUNNING
        elif phase == 'Pending':
            if not self._pod_status.is_container_creating:
                return JobStatus.PENDING
            else:
                return JobStatus.FAILED
        return JobStatus.PENDING

    def _parse_reason(self) -> Optional[str]:
        return None

    def _compose_description(self) -> Optional[str]:
        return None

    def create(self) -> JobStatusItem:
        return JobStatusItem.create(
            self._parse_status(),
            reason=self._parse_reason(),
            description=self._compose_description())


@dataclass(frozen=True)
class KubeConfig(OrchestratorConfig):
    jobs_ingress_name: str = ''

    endpoint_url: str = ''
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None

    namespace: str = 'default'

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    storage_volume_name: str = 'storage'

    job_deletion_delay_s: int = 60 * 60 * 24

    def __post_init__(self):
        if not all((self.jobs_ingress_name, self.endpoint_url)):
            raise ValueError('Missing required settings')

    @property
    def storage_mount_path(self) -> PurePath:
        return self.storage.host_mount_path

    @property
    def jobs_ingress_domain_name(self) -> str:
        return self.jobs_domain_name

    def create_storage_volume(self) -> Volume:
        if self.storage.is_nfs:
            return NfsVolume(  # type: ignore
                name=self.storage_volume_name,
                server=self.storage.nfs_server,
                path=self.storage.nfs_export_path,
            )
        return HostVolume(  # type: ignore
            name=self.storage_volume_name,
            path=self.storage_mount_path,
        )


def convert_pod_status_to_job_status(pod_status: PodStatus) -> JobStatusItem:
    return JobStatusItemFactory(pod_status).create()


class KubeOrchestrator(Orchestrator):
    def __init__(
            self, *, config: KubeConfig,
            loop: Optional[AbstractEventLoop]=None) -> None:
        self._loop = loop

        self._config = config

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing

        self._client = KubeClient(
            base_url=config.endpoint_url,

            cert_authority_path=config.cert_authority_path,

            auth_type=config.auth_type,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,

            namespace=config.namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size
        )

        self._storage_volume = self._config.create_storage_volume()

    @property
    def config(self) -> KubeConfig:
        return self._config

    async def __aenter__(self) -> 'KubeOrchestrator':
        await self._client.init()
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.close()

    async def start_job(self, job: Job) -> JobStatus:
        descriptor = PodDescriptor.from_job_request(
            self._storage_volume, job.request)
        status = await self._client.create_pod(descriptor)
        if job.has_http_server_exposed:
            await self._create_service(descriptor)
        job.status = status.status
        return status.status

    async def get_job_status(self, job_id: str) -> JobStatusItem:
        pod_id = job_id
        status = await self._client.get_pod_status(pod_id)
        return convert_pod_status_to_job_status(status)

    async def get_job_log_reader(self, job: Job) -> LogReader:
        return PodContainerLogReader(
            client=self._client, pod_name=job.id, container_name=job.id)

    async def _create_service(self, pod: PodDescriptor) -> None:
        service = await self._client.create_service(
            Service.create_for_pod(pod))
        await self._client.add_ingress_rule(
            name=self._config.jobs_ingress_name,
            rule=IngressRule.from_service(
                domain_name=self._config.jobs_ingress_domain_name,
                service=service))

    def _get_ingress_rule_host_for_pod(self, pod_id) -> str:
        ingress_rule = IngressRule.from_service(
            domain_name=self._config.jobs_ingress_domain_name,
            service=Service(name=pod_id, target_port=0)  # type: ignore
        )
        return ingress_rule.host

    async def _delete_service(self, pod_id: str) -> None:
        host = self._get_ingress_rule_host_for_pod(pod_id)
        try:
            await self._client.remove_ingress_rule(
                name=self._config.jobs_ingress_name, host=host)
        except Exception:
            logger.exception(f'Failed to remove ingress rule {host}')
        try:
            await self._client.delete_service(name=pod_id)
        except Exception:
            logger.exception(f'Failed to remove service {pod_id}')

    async def delete_job(self, job: Job) -> JobStatus:
        pod_id = job.id
        if job.has_http_server_exposed:
            await self._delete_service(pod_id)
        status = await self._client.delete_pod(pod_id)
        return status.status
