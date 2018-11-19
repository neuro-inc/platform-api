import logging
from asyncio import AbstractEventLoop
from dataclasses import dataclass
from pathlib import PurePath
from typing import Dict, List, Optional

from ..config import OrchestratorConfig  # noqa
from .base import LogReader, Orchestrator
from .job import Job, JobStatusItem
from .job_request import Container, JobNotFoundException, JobStatus
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
    Toleration,
    Volume,
)
from .logs import PodContainerLogReader


logger = logging.getLogger(__name__)


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
        if self._status in (JobStatus.PENDING, JobStatus.FAILED):
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

    namespace: str = "default"

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

        self._client = KubeClient(
            base_url=config.endpoint_url,
            cert_authority_path=config.cert_authority_path,
            auth_type=config.auth_type,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,
            namespace=config.namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size,
        )

        self._storage_volume = self._config.create_storage_volume()

        # TODO (A Danshyn 11/16/18): make this configurable at some point
        self._docker_secret_name_prefix = "neurouser-"

    @property
    def config(self) -> KubeConfig:
        return self._config

    async def __aenter__(self) -> "KubeOrchestrator":
        await self._client.init()
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.close()

    def _get_pod_namespace(self, descriptor: PodDescriptor) -> str:
        # TODO (A Yushkovskiy 31.10.2018): get namespace for the pod, not statically
        return self._config.namespace

    def _get_docker_secret_name(self, job: Job) -> str:
        return (self._docker_secret_name_prefix + job.owner).lower()

    async def _create_docker_secret(self, job: Job, token: str) -> DockerRegistrySecret:
        secret = DockerRegistrySecret(
            name=self._get_docker_secret_name(job),
            namespace=self._config.namespace,
            username=job.owner,
            password=token,
            email=self._config.registry.email,
            registry_server=self._config.registry.host,
        )
        await self._client.update_docker_secret(secret, create_non_existent=True)
        return secret

    async def _create_pod_descriptor(self, job: Job) -> PodDescriptor:
        secret_names = [self._get_docker_secret_name(job)]
        node_selector = await self._get_pod_node_selector(job)
        tolerations = self._get_pod_tolerations(job)
        return PodDescriptor.from_job_request(
            self._storage_volume,
            job.request,
            secret_names,
            node_selector=node_selector,
            tolerations=tolerations,
        )

    async def start_job(self, job: Job, token: str) -> JobStatus:
        await self._create_docker_secret(job, token)
        descriptor = await self._create_pod_descriptor(job)
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
        job.internal_hostname = self._get_service_internal_hostname(job.id, descriptor)
        return job.status

    def _get_service_internal_hostname(
        self, service_name: str, pod_descriptor: PodDescriptor
    ) -> str:
        return f"{service_name}.{self._get_pod_namespace(pod_descriptor)}"

    def _get_pod_tolerations(self, job: Job) -> List[Toleration]:
        tolerations = []
        if job.is_preemptible:
            tolerations.append(
                Toleration(
                    key="cloud.google.com/gke-preemptible",
                    value="true",
                    effect="NoSchedule",
                )
            )
        return tolerations

    async def _get_pod_node_selector(self, job: Job) -> Dict[str, str]:
        container = job.request.container
        selector: Dict[str, str] = {}

        if job.force_preemptible_resource_pool_type:
            selector["cloud.google.com/gke-preemptible"] = "true"

        if not self._config.node_label_gpu:
            return selector

        pool_types = await self.get_resource_pool_types()
        for pool_type in pool_types:
            if container.resources.check_fit_into_pool_type(pool_type):
                if pool_type.gpu_model:
                    selector[self._config.node_label_gpu] = pool_type.gpu_model.id
                break

        return selector

    def _get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        # kube pod names explicitly at some point
        return job.id

    async def get_job_status_v2(self, job: Job) -> JobStatusItem:
        if job.is_finished:
            return job.status

        # PENDING/RUNNING

        # TODO: ask Paul to set up taints on preemptible nodes

        pod_name = self._get_job_pod_name(job)
        if job.is_preemptible:
            try:
                pod_status = await self._client.get_pod_status(pod_name)
                if pod_status.is_node_lost:
                    logger.info(
                        f"Detected NodeLost in pod '{pod_name}'. Job '{job.id}'"
                    )
                    # if the pod's status reason is `NodeLost` regardless of
                    # the pod's status phase, we need to forcefully delete the
                    # pod and reschedule another one instead.
                    logger.info(f"Forcefully deleting pod '{pod_name}'. Job '{job.id}'")
                    await self._client.delete_pod(pod_name, force=True)
                    pod_status = None
            except JobNotFoundException:
                logger.info(f"Pod '{pod_name}' was lost. Job '{job.id}'")
                # if the job is still in PENDING/RUNNING, but the underlying
                # pod is gone, this may mean that the node was
                # preempted/failed (the node resource may no longer exist)
                # and the pods GC evicted the pod, effectively by
                # forcefully deleting it.
                pod_status = None

            if not pod_status:
                logger.info(f"Recreating preempted pod '{pod_name}'. Job '{job.id}'")
                descriptor = await self._create_pod_descriptor(job)
                # TODO: this may raise StatusException AlreadyExists in case
                # the pod is still being deleted.
                status = await self._client.create_pod(descriptor)
                assert status

        return await self.get_job_status(job.id)

    async def get_job_status(self, job_id: str) -> JobStatusItem:
        pod_name = job_id
        pod_status = await self._client.get_pod_status(pod_name)
        job_status = convert_pod_status_to_job_status(pod_status)

        # Pod in pending state, and no container information available
        # possible we are observing the case when Container requested
        # too much resources, check events for NotTriggerScaleUp event
        if not pod_status.is_scheduled:
            if not await self._check_pod_is_schedulable(pod_name):
                logger.info(f"Found pod that requested too much resources. ID={job_id}")
                # Update the reason field of the job to Too Much Requested
                job_status = JobStatusItem.create(
                    job_status.status,
                    transition_time=job_status.transition_time,
                    reason="Cluster doesn't have resources to fulfill request.",
                    description=job_status.description,
                )

        return job_status

    async def _check_pod_is_schedulable(self, pod_name: str) -> bool:
        pod_events = await self._client.get_pod_events(pod_name, self._config.namespace)
        if not pod_events:
            return True

        # TODO (A Danshyn 11/17/18): note that "FailedScheduling" goes prior
        # "TriggerScaleUp" as well. seems unclear whether this condition is
        # correct. In other words, regardless of presence of any of
        # "TriggerScaleUp"/"NotTriggerScaleUp", we could just look for
        # "FailedScheduling" and get the same result.
        # Instead, for clusters without autoscalers, we could just query all
        # nodes (cache) and check job a job's container resources against the
        # nodes' capacities.
        return not any(
            event.reason == "NotTriggerScaleUp" or event.reason == "FailedScheduling"
            for event in pod_events
        )

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
