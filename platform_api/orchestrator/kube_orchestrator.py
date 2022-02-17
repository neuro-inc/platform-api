import asyncio
import logging
import operator
import secrets
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import PurePath
from typing import Any, Optional, Union

import aiohttp

from platform_api.cluster_config import (
    OrchestratorConfig,
    RegistryConfig,
    StorageConfig,
)
from platform_api.resource import ResourcePoolType

from .base import Orchestrator
from .job import Job, JobRestartPolicy, JobStatusItem, JobStatusReason
from .job_request import (
    ContainerVolume,
    Disk,
    JobAlreadyExistsException,
    JobError,
    JobNotFoundException,
    JobStatus,
)
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
    NotFoundException,
    PodDescriptor,
    PodExec,
    PodRestartPolicy,
    PodStatus,
    PVCVolume,
    SecretVolume,
    Service,
    StatusException,
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
        if self._status.is_running and (
            self._container_status.is_waiting or self._container_status.is_terminated
        ):
            return JobStatusReason.RESTARTING
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

    def _parse_exit_code(self) -> Optional[int]:
        if self._status.is_finished and self._container_status.is_terminated:
            return self._container_status.exit_code
        return None

    def create(self) -> JobStatusItem:
        return JobStatusItem.create(
            self._status,
            reason=self._parse_reason(),
            description=self._compose_description(),
            exit_code=self._parse_exit_code(),
        )


def convert_pod_status_to_job_status(pod_status: PodStatus) -> JobStatusItem:
    return JobStatusItemFactory(pod_status).create()


class KubeOrchestrator(Orchestrator):
    def __init__(
        self,
        *,
        storage_configs: Sequence[StorageConfig],
        registry_config: RegistryConfig,
        orchestrator_config: OrchestratorConfig,
        kube_config: KubeConfig,
        trace_configs: Optional[list[aiohttp.TraceConfig]] = None,
    ) -> None:
        self._loop = asyncio.get_event_loop()
        self._storage_configs = storage_configs
        self._registry_config = registry_config
        self._orchestrator_config = orchestrator_config
        self._kube_config = kube_config

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
            trace_configs=trace_configs,
        )

        # TODO (A Danshyn 11/16/18): make this configurable at some point
        self._docker_secret_name_prefix = "neurouser-"

        self._restart_policy_map = {
            JobRestartPolicy.ALWAYS: PodRestartPolicy.ALWAYS,
            JobRestartPolicy.ON_FAILURE: PodRestartPolicy.ON_FAILURE,
            JobRestartPolicy.NEVER: PodRestartPolicy.NEVER,
        }

    @property
    def orchestrator_config(self) -> OrchestratorConfig:
        return self._orchestrator_config

    @property
    def kube_config(self) -> KubeConfig:
        return self._kube_config

    async def __aenter__(self) -> "KubeOrchestrator":
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._client.close()

    @property
    def main_storage_config(self) -> StorageConfig:
        for sc in self._storage_configs:
            if sc.path is None:
                return sc
        raise JobError("Main storage is not configured")

    @property
    def extra_storage_configs(self) -> Sequence[StorageConfig]:
        result = []
        for sc in self._storage_configs:
            if sc.path is not None:
                result.append(sc)
        return result

    def create_storage_volume(self, container_volume: ContainerVolume) -> Volume:
        for sc in self.extra_storage_configs:
            try:
                container_volume.src_path.relative_to(str(sc.path))
                storage_config = sc
                break
            except ValueError:
                pass
        else:
            storage_config = self.main_storage_config

        name = self._create_storage_volume_name(storage_config.path)

        if storage_config.is_nfs:
            return NfsVolume(
                name=name,
                path=storage_config.path,
                server=storage_config.nfs_server,  # type: ignore
                export_path=storage_config.nfs_export_path,  # type: ignore
            )
        if storage_config.is_pvc:
            assert storage_config.pvc_name
            return PVCVolume(
                name=name,
                path=storage_config.path,
                claim_name=storage_config.pvc_name,
            )
        return HostVolume(
            name=name,
            path=storage_config.path,
            host_path=storage_config.host_mount_path,
        )

    def _create_storage_volume_name(self, path: Optional[PurePath] = None) -> str:
        if path is None or path == PurePath("/"):
            return self._kube_config.storage_volume_name
        name_suffix = str(path).replace("/", "-").replace("_", "-")
        return self._kube_config.storage_volume_name + name_suffix

    @classmethod
    def create_secret_volume(cls, user_name: str) -> SecretVolume:
        name = cls._get_k8s_secret_name(user_name)
        if len(name) > 63:
            volume_name = name[:50] + secrets.token_hex(6)
        else:
            volume_name = name
        return SecretVolume(name=volume_name, k8s_secret_name=name)

    @classmethod
    def _get_k8s_secret_name(cls, user_name: str) -> str:
        return f"user--{user_name.replace('/', '--')}--secrets"

    def _get_user_resource_name(self, job: Job) -> str:
        return (self._docker_secret_name_prefix + job.owner.replace("/", "--")).lower()

    def _get_docker_secret_name(self, job: Job) -> str:
        return self._get_user_resource_name(job)

    async def _create_docker_secret(self, job: Job) -> DockerRegistrySecret:
        secret = DockerRegistrySecret(
            name=self._get_docker_secret_name(job),
            namespace=self._kube_config.namespace,
            username=self._registry_config.username,
            password=self._registry_config.password,
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
        tpu_ipv4_cidr_block = self._orchestrator_config.tpu_ipv4_cidr_block
        if not job.request.container.resources.tpu or not tpu_ipv4_cidr_block:
            # no need to create a network policy
            return

        name = self._get_job_pod_name(job)
        pod_labels = self._get_job_labels(job)
        rules: list[dict[str, Any]] = [
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

    async def _create_pod_descriptor(
        self, job: Job, tolerate_unreachable_node: bool = False
    ) -> PodDescriptor:
        pool_types = self._get_cheapest_pool_types(job)
        if not pool_types:
            raise JobError("Job will not fit into cluster")
        logger.info(
            "Job %s is scheduled to run in pool types %s",
            job.id,
            ", ".join(p.name for p in pool_types),
        )
        tolerations = self._get_pod_tolerations(
            job, tolerate_unreachable_node=tolerate_unreachable_node
        )
        node_affinity = self._get_pod_node_affinity(job, pool_types)
        labels = self._get_pod_labels(job)
        # NOTE: both node selector and affinity must be satisfied for the pod
        # to be scheduled onto a node.

        def _to_env_str(value: Any) -> str:
            if value:
                return str(value)
            return ""

        meta_env = {
            "NEURO_JOB_ID": job.id,
            "NEURO_JOB_NAME": _to_env_str(job.name),
            "NEURO_JOB_OWNER": job.owner,
            "NEURO_JOB_CLUSTER": job.cluster_name,
            "NEURO_JOB_INTERNAL_HOSTNAME": _to_env_str(job.internal_hostname),
            "NEURO_JOB_INTERNAL_HOSTNAME_NAMED": _to_env_str(
                job.internal_hostname_named
            ),
            "NEURO_JOB_HTTP_PORT": _to_env_str(job.request.container.port),
            "NEURO_JOB_HTTP_AUTH": _to_env_str(
                job.request.container.requires_http_auth
            ),
        }

        if job.preset_name:
            meta_env["NEURO_JOB_PRESET"] = job.preset_name

        pull_secrets = [self._get_docker_secret_name(job)]
        if self._kube_config.image_pull_secret_name:
            pull_secrets += [self._kube_config.image_pull_secret_name]

        pod = PodDescriptor.from_job_request(
            job.request,
            storage_volume_factory=self.create_storage_volume,
            secret_volume_factory=self.create_secret_volume,
            image_pull_secret_names=pull_secrets,
            tolerations=tolerations,
            node_affinity=node_affinity,
            labels=labels,
            priority_class_name=self._kube_config.jobs_pod_priority_class_name,
            restart_policy=self._get_pod_restart_policy(job),
            meta_env=meta_env,
            privileged=job.privileged,
        )
        pod = self._update_pod_container_resources(pod, pool_types)
        return pod

    def _get_cheapest_pool_types(self, job: Job) -> Sequence[ResourcePoolType]:
        # NOTE:
        # Config service exposes resource_affinity field in preset.
        # But currently it cannot be used in case of restartable jobs
        # because during job lifetime node pools, presets can change and
        # node affinity assigned to the job won't be valid anymore.
        container_resources = job.request.container.resources
        TKey = tuple[int, float, int]
        pool_types: dict[TKey, list[ResourcePoolType]] = defaultdict(list)

        for pool_type in self._orchestrator_config.resource_pool_types:
            # Schedule jobs only on preemptible nodes if such node specified
            if job.preemptible_node and not pool_type.is_preemptible:
                continue
            if not job.preemptible_node and pool_type.is_preemptible:
                continue

            # Do not schedule cpu jobs on gpu nodes
            if not container_resources.gpu and pool_type.gpu:
                continue

            if not container_resources.check_fit_into_pool_type(pool_type):
                continue

            key = (
                pool_type.gpu or 0,
                pool_type.available_cpu or 0,
                pool_type.available_memory_mb or 0,
            )
            pool_types[key].append(pool_type)

        if not pool_types:
            return []

        sorted_pool_types = sorted(pool_types.items(), key=lambda x: x[0])
        return sorted_pool_types[0][1]

    def _update_pod_container_resources(
        self, pod: PodDescriptor, pool_types: Sequence[ResourcePoolType]
    ) -> PodDescriptor:
        if not pod.resources:
            return pod
        max_node_cpu = max(p.available_cpu or 0 for p in pool_types)
        max_node_memory_mb = max(p.available_memory_mb or 0 for p in pool_types)
        max_node_gpu = max(p.gpu or 0 for p in pool_types)
        pod_gpu = pod.resources.gpu or 0
        if (
            max_node_cpu > pod.resources.cpu
            or max_node_memory_mb > pod.resources.memory
            or max_node_gpu > pod_gpu
        ):
            # Ignore pods that don't require all node's resources
            return pod
        # By default resources request is not specified which means
        # that request = limit. This can prevent pod scheduling on a node
        # which don't have enough memory. By lowering memory request we
        # guarantee that pod will be scheduled on a node.
        # We need to set some memory for cluster autoscaler to trigger scale up.
        # It's scale up triggering algorithm is based on the pod resources.
        # The more resources you request the more there is a chance that scale up
        # will be triggered.
        new_resources = replace(pod.resources, memory_request=1024)
        return replace(pod, resources=new_resources)

    def _get_user_pod_labels(self, job: Job) -> dict[str, str]:
        return {"platform.neuromation.io/user": job.owner.replace("/", "--")}

    def _get_job_labels(self, job: Job) -> dict[str, str]:
        return {"platform.neuromation.io/job": job.id}

    def _get_preset_labels(self, job: Job) -> dict[str, str]:
        if job.preset_name:
            return {"platform.neuromation.io/preset": job.preset_name}
        return {}

    def _get_gpu_labels(self, job: Job) -> dict[str, str]:
        if not job.has_gpu or not job.gpu_model_id:
            return {}
        return {"platform.neuromation.io/gpu-model": job.gpu_model_id}

    def _get_pod_labels(self, job: Job) -> dict[str, str]:
        labels = self._get_job_labels(job)
        labels.update(self._get_user_pod_labels(job))
        labels.update(self._get_gpu_labels(job))
        labels.update(self._get_preset_labels(job))
        return labels

    def _get_pod_restart_policy(self, job: Job) -> PodRestartPolicy:
        return self._restart_policy_map[job.restart_policy]

    async def get_missing_disks(self, disks: list[Disk]) -> list[Disk]:
        assert disks, "no disks"
        missing = []
        for disk in disks:
            try:
                pvc = await self._client.get_raw_pvc(
                    disk.disk_id, self._kube_config.namespace
                )
                pvc_path: str = pvc["metadata"]["labels"][
                    "platform.neuromation.io/user"
                ].replace("--", "/")
                pvc_org: Optional[str] = pvc["metadata"]["labels"].get(
                    "platform.neuromation.io/disk-api-org-name"
                )
                if pvc_org:
                    pvc_path = f"{pvc_org}/{pvc_path}"
                if disk.path != pvc_path:
                    missing.append(disk)
            except (StatusException, KeyError):
                missing.append(disk)
        return sorted(missing, key=lambda disk: disk.disk_id)

    async def get_missing_secrets(
        self, secret_path: str, secret_names: list[str]
    ) -> list[str]:
        assert secret_names, "no sec names"
        user_secret_name = self._get_k8s_secret_name(secret_path)
        try:
            raw = await self._client.get_raw_secret(
                user_secret_name, self._kube_config.namespace
            )
            keys = raw.get("data", {}).keys()
            missing = set(secret_names) - set(keys)
            return sorted(missing)

        except StatusException:
            return secret_names

    def _get_service_name_for_named(self, job: Job) -> str:
        from platform_api.handlers.validators import JOB_USER_NAMES_SEPARATOR

        return f"{job.name}{JOB_USER_NAMES_SEPARATOR}{job.base_owner}"

    async def start_job(
        self, job: Job, tolerate_unreachable_node: bool = False
    ) -> JobStatus:
        """
        tolerate_unreachable_node: used only in tests
        """

        await self._create_docker_secret(job)
        await self._create_user_network_policy(job)
        try:
            await self._create_pod_network_policy(job)

            descriptor = await self._create_pod_descriptor(
                job, tolerate_unreachable_node=tolerate_unreachable_node
            )
            pod = await self._client.create_pod(descriptor)

            logger.info(f"Starting Service for {job.id}.")
            service = await self._create_service(descriptor)
            if job.is_named:
                # If old job finished recently, it pod can be still there
                # with corresponding service, so we should delete it here
                service_name = self._get_service_name_for_named(job)
                await self._delete_service(service_name, ignore_missing=True)
                await self._create_service(descriptor, name=service_name)

            if job.has_http_server_exposed:
                logger.info(f"Starting Ingress for {job.id}")
                await self._create_ingress(job, service)
        except AlreadyExistsException as e:
            raise JobAlreadyExistsException(str(e))

        job.status_history.current = await self._get_pod_status(job, pod)
        return job.status

    def _get_pod_tolerations(
        self, job: Job, tolerate_unreachable_node: bool = False
    ) -> list[Toleration]:
        tolerations = [
            Toleration(
                key=self._kube_config.jobs_pod_job_toleration_key,
                operator="Exists",
                effect="NoSchedule",
            )
        ]
        if (
            self._kube_config.jobs_pod_preemptible_toleration_key
            and job.preemptible_node
        ):
            tolerations.append(
                Toleration(
                    key=self._kube_config.jobs_pod_preemptible_toleration_key,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        if tolerate_unreachable_node:
            # Used only in tests. Minikube puts taint on nodes
            # soon after it is created through K8s Api.
            tolerations.append(
                Toleration(
                    key="node.kubernetes.io/unreachable",
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        return tolerations

    def _get_pod_node_affinity(
        self, job: Job, pool_types: Sequence[ResourcePoolType]
    ) -> Optional[NodeAffinity]:
        # NOTE:
        # The pod is scheduled onto a node only if at least one of
        # `NodeSelectorTerm`s is satisfied.
        # `NodeSelectorTerm` is satisfied only if its `match_expressions` are
        # satisfied.
        required_terms: list[NodeSelectorTerm] = []
        preferred_terms: list[NodePreferredSchedulingTerm] = []

        if self._kube_config.node_label_node_pool:
            for pool_type in pool_types:
                required_terms.append(
                    NodeSelectorTerm(
                        [
                            NodeSelectorRequirement.create_in(
                                self._kube_config.node_label_node_pool, pool_type.name
                            )
                        ]
                    )
                )
        if not required_terms:
            return None
        return NodeAffinity(required=required_terms, preferred=preferred_terms)

    def _get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        # kube pod names explicitly at some point
        return job.id

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if job.is_finished:
            return job.status_history.current

        # handling PENDING/RUNNING jobs

        if job.is_restartable:
            pod = await self._check_restartable_job_pod(job)
        else:
            pod = await self._client.get_pod(self._get_job_pod_name(job))
        return await self._get_pod_status(job, pod)

    async def _get_pod_status(self, job: Job, pod: PodDescriptor) -> JobStatusItem:
        pod_status = pod.status
        assert pod_status is not None  # should always be present
        job_status = convert_pod_status_to_job_status(pod_status)

        if not pod_status.is_phase_pending:
            return job_status

        # Pod in pending state.
        # Possible we are observing the case when Container requested
        # too much resources -- we will check for NotTriggerScaleUp event.
        # Or it tries to mount disk that is used by another job
        # -- we will check for FailedAttachVolume event.
        now = datetime.now(timezone.utc)
        assert pod.created_at is not None
        pod_events = await self._client.get_pod_events(
            self._get_job_pod_name(job), self._kube_config.namespace
        )

        if pod_status.is_scheduled:
            if pod_events:
                pod_events.sort(key=operator.attrgetter("last_timestamp"))
                last_event = pod_events[-1]
                if last_event.reason == "Pulling":
                    return JobStatusItem.create(
                        JobStatus.PENDING,
                        transition_time=now,
                        reason=JobStatusReason.PULLING,
                        description=last_event.message,
                    )
                if last_event.reason == "FailedAttachVolume":
                    return JobStatusItem.create(
                        JobStatus.PENDING,
                        transition_time=now,
                        reason=JobStatusReason.DISK_UNAVAILABLE,
                        description="Waiting for another job to release disk resource",
                    )
            return job_status

        logger.info(f"Found unscheduled pod. Job '{job.id}'")

        # Jobs with scheduling enabled never timeout on k8s scheduler
        if job.scheduler_enabled:
            return job_status

        schedule_timeout = (
            job.schedule_timeout or self._orchestrator_config.job_schedule_timeout
        )

        scaleup_events = [e for e in pod_events if e.reason == "TriggeredScaleUp"]
        scaleup_events.sort(key=operator.attrgetter("last_timestamp"))
        if scaleup_events and (
            (now - scaleup_events[-1].last_timestamp).total_seconds()
            < self._orchestrator_config.job_schedule_scaleup_timeout + schedule_timeout
        ):
            # waiting for cluster scaleup
            return JobStatusItem.create(
                JobStatus.PENDING,
                transition_time=now,
                reason=JobStatusReason.CLUSTER_SCALING_UP,
                description="Scaling up the cluster to get more resources",
            )

        if (now - pod.created_at).total_seconds() < schedule_timeout:
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

    async def _check_restartable_job_pod(self, job: Job) -> PodDescriptor:
        assert job.is_restartable

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

    async def _create_service(
        self, pod: PodDescriptor, name: Optional[str] = None
    ) -> Service:
        service = Service.create_headless_for_pod(pod)
        if name is not None:
            service = service.make_named(name)
        return await self._client.create_service(service)

    async def _get_services(self, job: Job) -> list[Service]:
        return await self._client.list_services(self._get_job_labels(job))

    async def _delete_service(
        self, name: str, *, uid: Optional[str] = None, ignore_missing: bool = False
    ) -> None:
        try:
            await self._client.delete_service(name=name, uid=uid)
        except NotFoundException:
            if ignore_missing:
                return
            logger.exception(f"Failed to remove service {name}")
        except Exception:
            logger.exception(f"Failed to remove service {name}")

    async def delete_job(self, job: Job) -> JobStatus:
        if job.has_http_server_exposed:
            await self._delete_ingress(job)

        for service in await self._get_services(job):
            await self._delete_service(
                service.name, uid=service.uid, ignore_missing=True
            )

        await self._delete_pod_network_policy(job)

        pod_id = self._get_job_pod_name(job)
        status = await self._client.delete_pod(pod_id)
        return convert_pod_status_to_job_status(status).status

    def _get_job_ingress_name(self, job: Job) -> str:
        return job.id

    def _get_ingress_annotations(self, job: Job) -> dict[str, str]:
        annotations: dict[str, str] = {
            "kubernetes.io/ingress.class": self._kube_config.jobs_ingress_class,
        }
        if self._kube_config.jobs_ingress_class == "traefik":
            middlewares = []
            middlewares.append(self._kube_config.jobs_ingress_error_page_middleware)
            if job.requires_http_auth:
                middlewares.append(self._kube_config.jobs_ingress_auth_middleware)
            annotations.update(
                {
                    "traefik.ingress.kubernetes.io/router.middlewares": ",".join(
                        middlewares
                    ),
                }
            )
        return annotations

    def _get_job_name_ingress_labels(
        self, job: Job, service: Service
    ) -> dict[str, str]:
        labels = self._get_user_pod_labels(job)
        if job.name:
            labels["platform.neuromation.io/job-name"] = job.name
        return labels

    def _get_ingress_labels(self, job: Job, service: Service) -> dict[str, str]:
        return {**service.labels, **self._get_job_name_ingress_labels(job, service)}

    async def _delete_ingresses_by_job_name(self, job: Job, service: Service) -> None:
        labels = self._get_job_name_ingress_labels(job, service)
        try:
            await self._client.delete_all_ingresses(labels=labels)
        except Exception as e:
            logger.warning(f"Failed to remove ingresses {labels}: {e}")

    async def _create_ingress(self, job: Job, service: Service) -> None:
        if job.name:
            await self._delete_ingresses_by_job_name(job, service)
        name = self._get_job_ingress_name(job)
        rules = [
            IngressRule.from_service(host=host, service=service)
            for host in job.http_hosts
        ]
        labels = self._get_ingress_labels(job, service)
        annotations = self._get_ingress_annotations(job)
        await self._client.create_ingress(
            name,
            ingress_class=self._kube_config.jobs_ingress_class,
            rules=rules,
            annotations=annotations,
            labels=labels,
        )

    async def _delete_ingress(self, job: Job) -> None:
        name = self._get_job_ingress_name(job)
        try:
            await self._client.delete_ingress(name)
        except Exception as e:
            logger.warning(f"Failed to remove ingress {name}: {e}")

    async def delete_all_job_resources(self, job_id: str) -> None:
        async for link in self._client.get_all_job_resources_links(job_id):
            await self._client.delete_resource_by_link(link)
