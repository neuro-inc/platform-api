import asyncio
import logging
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Dict, Iterable, List, Optional, Union

from aioelasticsearch import Elasticsearch

from ..config import OrchestratorConfig  # noqa
from .base import LogReader, Orchestrator, Telemetry
from .job import Job, JobStatusItem
from .job_request import JobError, JobNotFoundException, JobStatus
from .jobs_telemetry import KubeTelemetry
from .kube_client import *  # noqa
from .kube_client import (
    AlreadyExistsException,
    DockerRegistrySecret,
    HostVolume,
    IngressRule,
    KubeClient,
    KubeClientAuthType,
    NfsVolume,
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodDescriptor,
    PodExec,
    PodStatus,
    Service,
    Toleration,
    Volume,
)
from .logs import ElasticsearchLogReader, PodContainerLogReader


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
        else:
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
    jobs_ingress_auth_name: str = ""

    endpoint_url: str = ""
    cert_authority_path: Optional[str] = None

    auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE
    auth_cert_path: Optional[str] = None
    auth_cert_key_path: Optional[str] = None
    token_path: Optional[str] = None

    namespace: str = "default"

    client_conn_timeout_s: int = 300
    client_read_timeout_s: int = 300
    client_conn_pool_size: int = 100

    storage_volume_name: str = "storage"

    job_deletion_delay_s: int = 60 * 60 * 24

    node_label_gpu: Optional[str] = None
    node_label_preemptible: Optional[str] = None

    def __post_init__(self) -> None:
        if not all((self.jobs_ingress_name, self.endpoint_url)):
            raise ValueError("Missing required settings")

    @property
    def storage_mount_path(self) -> PurePath:
        return self.storage.host_mount_path

    @property
    def ssh_ingress_domain_name(self) -> str:
        return self.ssh_domain_name

    def create_storage_volume(self) -> Volume:
        if self.storage.is_nfs:
            return NfsVolume(  # type: ignore # noqa
                name=self.storage_volume_name,
                server=self.storage.nfs_server,
                path=self.storage.nfs_export_path,
            )
        return HostVolume(name=self.storage_volume_name, path=self.storage_mount_path)


def convert_pod_status_to_job_status(pod_status: PodStatus) -> JobStatusItem:
    return JobStatusItemFactory(pod_status).create()


class KubeOrchestrator(Orchestrator):
    def __init__(
        self, *, config: KubeConfig, es_client: Optional[Elasticsearch] = None
    ) -> None:
        self._loop = asyncio.get_event_loop()

        self._config = config

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing

        self._client = KubeClient(
            base_url=config.endpoint_url,
            cert_authority_path=config.cert_authority_path,
            auth_type=config.auth_type,
            auth_cert_path=config.auth_cert_path,
            auth_cert_key_path=config.auth_cert_key_path,
            token_path=config.token_path,
            namespace=config.namespace,
            conn_timeout_s=config.client_conn_timeout_s,
            read_timeout_s=config.client_read_timeout_s,
            conn_pool_size=config.client_conn_pool_size,
        )

        self._storage_volume = self._config.create_storage_volume()

        # TODO (A Danshyn 11/16/18): make this configurable at some point
        self._docker_secret_name_prefix = "neurouser-"

        self._es_client = es_client

    @property
    def config(self) -> KubeConfig:
        return self._config

    async def __aenter__(self) -> "KubeOrchestrator":
        await self._client.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.close()

    def _get_pod_namespace(self, descriptor: PodDescriptor) -> str:
        # TODO (A Yushkovskiy 31.10.2018): get namespace for the pod, not statically
        return self._config.namespace

    def _get_user_resource_name(self, job: Job) -> str:
        return (self._docker_secret_name_prefix + job.owner).lower()

    def _get_docker_secret_name(self, job: Job) -> str:
        return self._get_user_resource_name(job)

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

    async def _create_user_network_policy(self, job: Job) -> None:
        name = self._get_user_resource_name(job)
        pod_labels = self._get_user_pod_labels(job)
        try:
            await self._client.create_default_network_policy(
                name, pod_labels, namespace_name=self._config.namespace
            )
            logger.info(f"Created default network policy for user '{job.owner}'")
        except AlreadyExistsException:
            logger.info(
                f"Default network policy for user '{job.owner}' already exists."
            )

    async def _create_pod_descriptor(self, job: Job) -> PodDescriptor:
        secret_names = [self._get_docker_secret_name(job)]
        node_selector = await self._get_pod_node_selector(job)
        tolerations = self._get_pod_tolerations(job)
        node_affinity = self._get_pod_node_affinity(job)
        labels = self._get_pod_labels(job)
        return PodDescriptor.from_job_request(
            self._storage_volume,
            job.request,
            secret_names,
            node_selector=node_selector,
            tolerations=tolerations,
            node_affinity=node_affinity,
            labels=labels,
        )

    def _get_user_pod_labels(self, job: Job) -> Dict[str, str]:
        return {"platform.neuromation.io/user": job.owner}

    def _get_pod_labels(self, job: Job) -> Dict[str, str]:
        labels = {"platform.neuromation.io/job": job.id}
        labels.update(self._get_user_pod_labels(job))
        return labels

    async def start_job(self, job: Job, token: str) -> JobStatus:
        await self._create_docker_secret(job, token)
        await self._create_user_network_policy(job)

        descriptor = await self._create_pod_descriptor(job)
        status = await self._client.create_pod(descriptor)

        logger.info(f"Starting Service for {job.id}.")
        service = await self._create_service(descriptor)

        if job.has_http_server_exposed:
            logger.info(f"Starting Ingress for {job.id}")
            await self._create_ingress(job, service)

        job.status = convert_pod_status_to_job_status(status).status
        job.internal_hostname = self._get_service_internal_hostname(job.id, descriptor)
        return job.status

    def _get_service_internal_hostname(
        self, service_name: str, pod_descriptor: PodDescriptor
    ) -> str:
        return f"{service_name}.{self._get_pod_namespace(pod_descriptor)}"

    def _get_pod_tolerations(self, job: Job) -> List[Toleration]:
        tolerations = []
        if self._config.node_label_preemptible and job.is_preemptible:
            tolerations.append(
                Toleration(
                    key=self._config.node_label_preemptible,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        return tolerations

    def _get_pod_node_affinity(self, job: Job) -> Optional[NodeAffinity]:
        if not self._config.node_label_preemptible:
            return None

        required_terms = []
        preferred_terms = []

        if job.is_preemptible:
            node_selector_term = NodeSelectorTerm(
                [
                    NodeSelectorRequirement.create_exists(
                        self._config.node_label_preemptible
                    )
                ]
            )
            if job.is_forced_to_preemptible_pool:
                required_terms.append(node_selector_term)
            else:
                preferred_terms.append(NodePreferredSchedulingTerm(node_selector_term))
        else:
            node_selector_term = NodeSelectorTerm(
                [
                    NodeSelectorRequirement.create_does_not_exist(
                        self._config.node_label_preemptible
                    )
                ]
            )
            required_terms.append(node_selector_term)

        return NodeAffinity(required=required_terms, preferred=preferred_terms)

    async def _get_pod_node_selector(self, job: Job) -> Dict[str, str]:
        container = job.request.container
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

    def _get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        # kube pod names explicitly at some point
        return job.id

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if job.is_finished:
            return job.status_history.current

        # handling PENDING/RUNNING jobs

        pod_name = self._get_job_pod_name(job)
        if job.is_preemptible:
            pod_status = await self._check_preemptible_job_pod(job)
        else:
            pod_status = await self._client.get_pod_status(pod_name)

        job_status = convert_pod_status_to_job_status(pod_status)

        # Pod in pending state, and no container information available
        # possible we are observing the case when Container requested
        # too much resources, check events for NotTriggerScaleUp event
        if not pod_status.is_scheduled:
            if not await self._check_pod_is_schedulable(pod_name):
                logger.info(
                    f"Found pod that requested too much resources. Job '{job.id}'"
                )
                # Update the reason field of the job to Too Much Requested
                job_status = JobStatusItem.create(
                    job_status.status,
                    transition_time=job_status.transition_time,
                    reason="Cluster doesn't have resources to fulfill request.",
                    description=job_status.description,
                )

        return job_status

    async def _check_preemptible_job_pod(self, job: Job) -> PodStatus:
        assert job.is_preemptible

        pod_name = self._get_job_pod_name(job)
        do_recreate_pod = False
        try:
            pod_status = await self._client.get_pod_status(pod_name)
            if pod_status.is_node_lost:
                logger.info(f"Detected NodeLost in pod '{pod_name}'. Job '{job.id}'")
                # if the pod's status reason is `NodeLost` regardless of
                # the pod's status phase, we need to forcefully delete the
                # pod and reschedule another one instead.
                logger.info(f"Forcefully deleting pod '{pod_name}'. Job '{job.id}'")
                await self._client.delete_pod(pod_name, force=True)
                do_recreate_pod = True
        except JobNotFoundException:
            logger.info(f"Pod '{pod_name}' was lost. Job '{job.id}'")
            # if the job is still in PENDING/RUNNING, but the underlying
            # pod is gone, this may mean that the node was
            # preempted/failed (the node resource may no longer exist)
            # and the pods GC evicted the pod, effectively by
            # forcefully deleting it.
            do_recreate_pod = True

        if do_recreate_pod:
            logger.info(f"Recreating preempted pod '{pod_name}'. Job '{job.id}'")
            descriptor = await self._create_pod_descriptor(job)
            try:
                pod_status = await self._client.create_pod(descriptor)
            except JobError:
                # handing possible 422 and other failures
                raise JobNotFoundException(
                    f"Pod '{pod_name}' not found. Job '{job.id}'"
                )

        return pod_status

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

    async def _check_pod_exists(self, pod_name: str) -> bool:
        try:
            await self._client.get_pod_status(pod_name)
            return True
        except JobNotFoundException:
            return False

    async def get_job_log_reader(self, job: Job) -> LogReader:
        assert self._es_client
        pod_name = self._get_job_pod_name(job)
        if await self._check_pod_exists(pod_name):
            return PodContainerLogReader(
                client=self._client, pod_name=pod_name, container_name=pod_name
            )
        return ElasticsearchLogReader(
            es_client=self._es_client,
            namespace_name=self._config.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )

    async def get_job_telemetry(self, job: Job) -> Telemetry:
        pod_name = self._get_job_pod_name(job)
        return KubeTelemetry(
            self._client,
            namespace_name=self._config.namespace,
            pod_name=pod_name,
            container_name=pod_name,
        )

    async def exec_pod(
        self, job_id: str, command: Union[str, Iterable[str]], *, tty: bool
    ) -> PodExec:
        return await self._client.exec_pod(job_id, command, tty=tty)

    async def _create_service(self, pod: PodDescriptor) -> Service:
        return await self._client.create_service(Service.create_headless_for_pod(pod))

    async def _delete_service(self, job: Job) -> None:
        pod_id = self._get_job_pod_name(job)
        try:
            await self._client.delete_service(name=pod_id)
        except Exception:
            logger.exception(f"Failed to remove service {pod_id}")

    async def delete_job(self, job: Job) -> JobStatus:
        if job.has_http_server_exposed:
            await self._delete_ingress(job)

        await self._delete_service(job)

        pod_id = self._get_job_pod_name(job)
        status = await self._client.delete_pod(pod_id)
        return convert_pod_status_to_job_status(status).status

    async def _create_ingress(self, job: Job, service: Service) -> None:
        ingress_name = self._get_ingress_name(job)
        for host in job.http_hosts:
            await self._client.add_ingress_rule(
                name=ingress_name,
                rule=IngressRule.from_service(host=host, service=service),
            )

    async def _delete_ingress(self, job: Job) -> None:
        ingress_name = self._get_ingress_name(job)
        for host in job.http_hosts:
            try:
                await self._client.remove_ingress_rule(name=ingress_name, host=host)
            except Exception:
                logger.exception(f"Failed to remove ingress rule {host}")

    def _get_ingress_name(self, job: Job) -> str:
        if job.requires_http_auth and self._config.jobs_ingress_auth_name:
            return self._config.jobs_ingress_auth_name
        return self._config.jobs_ingress_name
