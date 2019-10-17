import asyncio
import logging
import operator
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union

from platform_api.cluster_config import (
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)

from .base import Orchestrator
from .job import Job, JobStatusItem, JobStatusReason
from .job_request import JobError, JobNotFoundException, JobStatus
from .kube_client import (
    AlreadyExistsException,
    DockerRegistrySecret,
    HostVolume,
    IngressRule,
    KubeClient,
    NfsVolume,
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeSelectorRequirement,
    NodeSelectorTerm,
    PodDescriptor,
    PodExec,
    PodStatus,
    PVCVolume,
    Service,
    Toleration,
    Volume,
)
from .kube_config import KubeConfig


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
                return self._container_status.message
        return None

    def create(self) -> JobStatusItem:
        return JobStatusItem.create(
            self._status,
            reason=self._parse_reason(),
            description=self._compose_description(),
            exit_code=(
                self._container_status.exit_code
                if self._container_status.is_terminated
                else None
            ),
        )


def convert_pod_status_to_job_status(pod_status: PodStatus) -> JobStatusItem:
    return JobStatusItemFactory(pod_status).create()


class KubeOrchestrator(Orchestrator):
    def __init__(
        self,
        *,
        storage_config: StorageConfig,
        registry_config: RegistryConfig,
        kube_config: OrchestratorConfig,
    ) -> None:
        self._loop = asyncio.get_event_loop()

        assert isinstance(kube_config, KubeConfig)

        self._storage_config = storage_config
        self._registry_config = registry_config
        self._kube_config: KubeConfig = kube_config

        # TODO (A Danshyn 05/21/18): think of the namespace life-time;
        # should we ensure it does exist before continuing

        self._client = KubeClient(
            base_url=kube_config.endpoint_url,
            cert_authority_data_pem=kube_config.cert_authority_data_pem,
            cert_authority_path=kube_config.cert_authority_path,
            auth_type=kube_config.auth_type,
            auth_cert_path=kube_config.auth_cert_path,
            auth_cert_key_path=kube_config.auth_cert_key_path,
            token=kube_config.token,
            token_path=kube_config.token_path,
            namespace=kube_config.namespace,
            conn_timeout_s=kube_config.client_conn_timeout_s,
            read_timeout_s=kube_config.client_read_timeout_s,
            conn_pool_size=kube_config.client_conn_pool_size,
        )

        self._storage_volume = self.create_storage_volume()

        # TODO (A Danshyn 11/16/18): make this configurable at some point
        self._docker_secret_name_prefix = "neurouser-"

    @property
    def config(self) -> OrchestratorConfig:
        return self._kube_config

    @property
    def storage_config(self) -> StorageConfig:
        return self._storage_config

    async def __aenter__(self) -> "KubeOrchestrator":
        await self._client.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.close()

    def create_storage_volume(self) -> Volume:
        if self._storage_config.is_nfs:
            return NfsVolume(
                name=self._kube_config.storage_volume_name,
                server=self._storage_config.nfs_server,  # type: ignore
                path=self._storage_config.nfs_export_path,  # type: ignore
            )
        if self._storage_config.is_pvc:
            assert self._storage_config.pvc_name
            return PVCVolume(
                name=self._kube_config.storage_volume_name,
                path=self._storage_config.host_mount_path,
                claim_name=self._storage_config.pvc_name,
            )
        return HostVolume(
            name=self._kube_config.storage_volume_name,
            path=self._storage_config.host_mount_path,
        )

    def _get_pod_namespace(self, descriptor: PodDescriptor) -> str:
        # TODO (A Yushkovskiy 31.10.2018): get namespace for the pod, not statically
        return self._kube_config.namespace

    def _get_user_resource_name(self, job: Job) -> str:
        return (self._docker_secret_name_prefix + job.owner).lower()

    def _get_docker_secret_name(self, job: Job) -> str:
        return self._get_user_resource_name(job)

    async def _create_docker_secret(self, job: Job, token: str) -> DockerRegistrySecret:
        secret = DockerRegistrySecret(
            name=self._get_docker_secret_name(job),
            namespace=self._kube_config.namespace,
            username=job.owner,
            password=token,
            email=self._registry_config.email,
            registry_server=self._registry_config.host,
        )
        await self._client.update_docker_secret(secret, create_non_existent=True)
        return secret

    async def _create_user_network_policy(self, job: Job) -> None:
        name = self._get_user_resource_name(job)
        pod_labels = self._get_user_pod_labels(job)
        try:
            await self._client.create_default_network_policy(
                name, pod_labels, namespace_name=self._kube_config.namespace
            )
            logger.info(f"Created default network policy for user '{job.owner}'")
        except AlreadyExistsException:
            logger.info(
                f"Default network policy for user '{job.owner}' already exists."
            )

    async def _create_pod_network_policy(self, job: Job) -> None:
        tpu_ipv4_cidr_block = self._kube_config.tpu_ipv4_cidr_block
        if not job.request.container.resources.tpu or not tpu_ipv4_cidr_block:
            # no need to create a network policy
            return

        name = self._get_job_pod_name(job)
        pod_labels = self._get_job_labels(job)
        rules: List[Dict[str, Any]] = [
            # allowing the pod to connect to TPU nodes within internal network
            {"to": [{"ipBlock": {"cidr": tpu_ipv4_cidr_block}}]}
        ]
        labels = self._get_pod_labels(job)
        await self._client.create_egress_network_policy(
            name,
            pod_labels=pod_labels,
            rules=rules,
            namespace_name=self._kube_config.namespace,
            labels=labels,
        )

    async def _delete_pod_network_policy(self, job: Job) -> None:
        name = self._get_job_pod_name(job)
        try:
            await self._client.delete_network_policy(
                name, namespace_name=self._kube_config.namespace
            )
        except Exception as e:
            logger.warning(f"Failed to remove network policy {name}: {e}")

    async def _create_pod_descriptor(self, job: Job) -> PodDescriptor:
        secret_names = [self._get_docker_secret_name(job)]
        node_selector = await self._get_pod_node_selector(job)
        tolerations = self._get_pod_tolerations(job)
        node_affinity = self._get_pod_node_affinity(job)
        labels = self._get_pod_labels(job)
        # NOTE: both node selector and affinity must be satisfied for the pod
        # to be scheduled onto a node.
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

    def _get_job_labels(self, job: Job) -> Dict[str, str]:
        return {"platform.neuromation.io/job": job.id}

    def _get_pod_labels(self, job: Job) -> Dict[str, str]:
        labels = self._get_job_labels(job)
        labels.update(self._get_user_pod_labels(job))
        return labels

    async def start_job(self, job: Job, token: str) -> JobStatus:
        await self._create_docker_secret(job, token)
        await self._create_user_network_policy(job)
        await self._create_pod_network_policy(job)

        descriptor = await self._create_pod_descriptor(job)
        pod = await self._client.create_pod(descriptor)
        status = pod.status
        assert status is not None

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
        tolerations = [
            Toleration(
                key=self._kube_config.jobs_pod_toleration_key,
                operator="Exists",
                effect="NoSchedule",
            )
        ]
        if self._kube_config.node_label_preemptible and job.is_preemptible:
            tolerations.append(
                Toleration(
                    key=self._kube_config.node_label_preemptible,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        return tolerations

    def _get_pod_node_affinity(self, job: Job) -> Optional[NodeAffinity]:
        requirements = []
        preferences = []

        if self._kube_config.node_label_job:
            # requiring a job node
            requirements.append(
                NodeSelectorRequirement.create_exists(self._kube_config.node_label_job)
            )

        if self._kube_config.node_label_preemptible:
            if job.is_preemptible:
                preemptible_requirement = NodeSelectorRequirement.create_exists(
                    self._kube_config.node_label_preemptible
                )
                if job.is_forced_to_preemptible_pool:
                    # requiring a preemptible node
                    requirements.append(preemptible_requirement)
                else:
                    # preferring a preemptible node
                    preferences.append(preemptible_requirement)
            else:
                # requiring a non-preemptible node
                requirements.append(
                    NodeSelectorRequirement.create_does_not_exist(
                        self._kube_config.node_label_preemptible
                    )
                )

        required_terms = []
        preferred_terms = []

        if requirements:
            node_selector_term = NodeSelectorTerm(requirements)
            required_terms.append(node_selector_term)

        if preferences:
            node_selector_term = NodeSelectorTerm(preferences)
            preferred_terms.append(NodePreferredSchedulingTerm(node_selector_term))

        # NOTE:
        # The pod is scheduled onto a node only if at least one of
        # `NodeSelectorTerm`s is satisfied.
        # `NodeSelectorTerm` is satisfied only if its `match_expressions` are
        # satisfied.

        if required_terms or preferred_terms:
            return NodeAffinity(required=required_terms, preferred=preferred_terms)
        return None

    async def _get_pod_node_selector(self, job: Job) -> Dict[str, str]:
        container = job.request.container
        selector: Dict[str, str] = {}

        if not self._kube_config.node_label_gpu:
            return selector

        pool_types = await self.get_resource_pool_types()
        for pool_type in pool_types:
            if container.resources.check_fit_into_pool_type(pool_type):
                if pool_type.gpu_model:
                    selector[self._kube_config.node_label_gpu] = pool_type.gpu_model
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
            pod = await self._check_preemptible_job_pod(job)
        else:
            pod = await self._client.get_pod(pod_name)

        pod_status = pod.status
        assert pod_status is not None  # should always be present
        job_status = convert_pod_status_to_job_status(pod_status)

        if pod_status.is_scheduled:
            return job_status

        # Pod in pending state, and not scheduled yet.
        # Possible we are observing the case when Container requested
        # too much resources, check events for NotTriggerScaleUp event
        now = datetime.now(timezone.utc)
        assert pod.created_at is not None

        schedule_timeout = (
            job.schedule_timeout or self._kube_config.job_schedule_timeout
        )

        logger.info(f"Found pod that requested too much resources. Job '{job.id}'")
        pod_events = await self._client.get_pod_events(
            pod_name, self._kube_config.namespace
        )
        scaleup_events = [e for e in pod_events if e.reason == "TriggeredScaleUp"]
        scaleup_events.sort(key=operator.attrgetter("last_timestamp"))
        if scaleup_events and (
            (now - scaleup_events[-1].last_timestamp).seconds
            < self._kube_config.job_schedule_scaleup_timeout + schedule_timeout
        ):
            # waiting for cluster scaleup
            return JobStatusItem.create(
                JobStatus.PENDING,
                transition_time=now,
                reason=JobStatusReason.CLUSTER_SCALING_UP,
                description="Scaling up the cluster to get more resources",
            )

        if (now - pod.created_at).seconds < schedule_timeout:
            # Wait for scheduling for 3 minutes at least by default
            if job_status.reason is None:
                job_status = replace(job_status, reason=JobStatusReason.SCHEDULING)
            return job_status

        return JobStatusItem.create(
            JobStatus.FAILED,
            transition_time=now,
            reason=JobStatusReason.CLUSTER_SCALE_UP_FAILED,
            description="Failed to scale up the cluster to get more resources",
        )

    async def _check_preemptible_job_pod(self, job: Job) -> PodDescriptor:
        assert job.is_preemptible

        pod_name = self._get_job_pod_name(job)
        do_recreate_pod = False
        try:
            pod = await self._client.get_pod(pod_name)
            pod_status = pod.status
            assert pod_status is not None
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
                pod = await self._client.create_pod(descriptor)
            except JobError:
                # handing possible 422 and other failures
                raise JobNotFoundException(
                    f"Pod '{pod_name}' not found. Job '{job.id}'"
                )

        return pod

    async def _check_pod_exists(self, pod_name: str) -> bool:
        try:
            await self._client.get_pod_status(pod_name)
            return True
        except JobNotFoundException:
            return False

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

        await self._delete_pod_network_policy(job)

        pod_id = self._get_job_pod_name(job)
        status = await self._client.delete_pod(pod_id)
        return convert_pod_status_to_job_status(status).status

    def _get_job_ingress_name(self, job: Job) -> str:
        return job.id

    def _get_ingress_annotations(self, job: Job) -> Dict[str, str]:
        annotations: Dict[str, str] = {}
        if self._kube_config.jobs_ingress_class == "traefik":
            annotations = {
                "kubernetes.io/ingress.class": "traefik",
                "traefik.ingress.kubernetes.io/error-pages": (
                    "default:\n"
                    "  status:\n"
                    '  - "500-600"\n'
                    "  backend: error-pages\n"
                    "  query: /"
                ),
            }
            if job.requires_http_auth:
                oauth_url = self._kube_config.jobs_ingress_oauth_url
                assert oauth_url
                annotations.update(
                    {
                        "ingress.kubernetes.io/auth-type": "forward",
                        "ingress.kubernetes.io/auth-trust-headers": "true",
                        "ingress.kubernetes.io/auth-url": str(oauth_url),
                    }
                )
        return annotations

    async def _create_ingress(self, job: Job, service: Service) -> None:
        name = self._get_job_ingress_name(job)
        rules = [
            IngressRule.from_service(host=host, service=service)
            for host in job.http_hosts
        ]
        annotations = self._get_ingress_annotations(job)
        await self._client.create_ingress(
            name, rules=rules, annotations=annotations, labels=service.labels
        )

    async def _delete_ingress(self, job: Job) -> None:
        name = self._get_job_ingress_name(job)
        try:
            await self._client.delete_ingress(name)
        except Exception as e:
            logger.warning(f"Failed to remove ingress {name}: {e}")
