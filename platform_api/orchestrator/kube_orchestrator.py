import asyncio
import logging
import operator
import secrets
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import PurePath
from typing import Any

import aiohttp

from platform_api.cluster_config import OrchestratorConfig
from platform_api.config import RegistryConfig, StorageConfig
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
    JobUnschedulableException,
)
from .kube_client import (
    AlreadyExistsException,
    DockerRegistrySecret,
    HostVolume,
    IngressRule,
    KubeClient,
    LabelSelectorMatchExpression,
    LabelSelectorTerm,
    NfsVolume,
    NodeAffinity,
    NodePreferredSchedulingTerm,
    NodeWatcher,
    NotFoundException,
    PathVolume,
    PodAffinity,
    PodAffinityTerm,
    PodDescriptor,
    PodPreferredSchedulingTerm,
    PodRestartPolicy,
    PodStatus,
    PodWatcher,
    PVCVolume,
    Resources,
    SecretVolume,
    Service,
    StatusException,
    Toleration,
    VolumeMount,
)
from .kube_config import KubeConfig
from .kube_orchestrator_scheduler import (
    KubeOrchestratorPreemption,
    KubeOrchestratorScheduler,
    NodeResourcesHandler,
    NodesHandler,
)

logger = logging.getLogger(__name__)


JOB_LABEL_KEY = "platform.neuromation.io/job"
PROJECT_LABEL_KEY = "platform.apolo.us/project"
ORG_LABEL_KEY = "platform.apolo.us/org"


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
        if phase in ("Failed", "Unknown"):
            return JobStatus.FAILED
        if phase == "Running":
            return JobStatus.RUNNING
        return JobStatus.PENDING

    def _parse_reason(self) -> str | None:
        if self._status.is_running and (
            self._container_status.is_waiting or self._container_status.is_terminated
        ):
            return JobStatusReason.RESTARTING
        if self._status in (JobStatus.PENDING, JobStatus.FAILED):
            return self._container_status.reason
        return None

    def _compose_description(self) -> str | None:
        if self._status == JobStatus.FAILED:
            if (
                self._container_status.is_terminated
                and self._container_status.exit_code
            ):
                return self._container_status.message
        return None

    def _parse_exit_code(self) -> int | None:
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
        cluster_name: str,
        storage_configs: Sequence[StorageConfig],
        registry_config: RegistryConfig,
        orchestrator_config: OrchestratorConfig,
        kube_config: KubeConfig,
        kube_client: KubeClient,
    ) -> None:
        self._loop = asyncio.get_event_loop()
        self._cluster_name = cluster_name
        self._storage_configs = storage_configs
        self._registry_config = registry_config
        self._orchestrator_config = orchestrator_config
        self._kube_config = kube_config
        self._client = kube_client

        self._nodes_handler = NodesHandler()
        self._node_resources_handler = NodeResourcesHandler()
        self._scheduler = KubeOrchestratorScheduler(
            self._nodes_handler, self._node_resources_handler
        )
        self._preemption = KubeOrchestratorPreemption(
            kube_client, self._nodes_handler, self._node_resources_handler
        )

        # TODO (A Danshyn 11/16/18): make this configurable at some point
        self._docker_secret_name_prefix = "neurouser-"
        self._project_prefix = "project--"

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

    @property
    def _main_storage_config(self) -> StorageConfig:
        for sc in self._storage_configs:
            if sc.path is None:
                return sc
        raise JobError("Main storage is not configured")

    @property
    def _extra_storage_configs(self) -> list[StorageConfig]:
        result = []
        for sc in self._storage_configs:
            if sc.path is not None:
                result.append(sc)
        return result

    def subscribe_to_kube_events(
        self, node_watcher: NodeWatcher, pod_watcher: PodWatcher
    ) -> None:
        node_watcher.subscribe(self._nodes_handler)
        pod_watcher.subscribe(self._node_resources_handler)

    def create_storage_volumes(
        self, container_volume: ContainerVolume
    ) -> Sequence[PathVolume]:
        storage_configs = self._get_storage_configs_from_container(container_volume)
        volumes = []
        for sc in storage_configs:
            volumes.append(self._get_volume_from_storage_config(sc))
        return volumes

    def _get_storage_configs_from_container(
        self, container_volume: ContainerVolume
    ) -> Sequence[StorageConfig]:
        if container_volume.src_path == PurePath("/"):
            return [self._main_storage_config] + self._extra_storage_configs

        for sc in self._extra_storage_configs:
            try:
                container_volume.src_path.relative_to(str(sc.path))
                return [sc]
            except ValueError:
                pass
        else:
            return [self._main_storage_config]

    def _get_volume_from_storage_config(
        self, storage_config: StorageConfig
    ) -> PathVolume:
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

    def create_storage_volume_mounts(
        self, container_volume: ContainerVolume, volumes: Sequence[PathVolume]
    ) -> Sequence[VolumeMount]:
        if len(volumes) == 1:
            return [volumes[0].create_mount(container_volume)]

        result = []
        for v in volumes:
            if v.path is None or v.path == PurePath("/"):
                dst_path = PurePath(self._cluster_name)
            else:
                dst_path = PurePath(v.path.name)
            result.append(v.create_mount(container_volume, dst_path))
        return result

    def _create_storage_volume_name(self, path: PurePath | None = None) -> str:
        if path is None or path == PurePath("/"):
            return self._kube_config.storage_volume_name
        name_suffix = str(path).replace("/", "-").replace("_", "-")
        return self._kube_config.storage_volume_name + name_suffix

    def create_secret_volume(self, project_name: str) -> SecretVolume:
        name = self._get_k8s_secret_name(project_name)
        if len(name) > 63:
            volume_name = name[:50] + secrets.token_hex(6)
        else:
            volume_name = name
        return SecretVolume(name=volume_name, k8s_secret_name=name)

    def _get_k8s_secret_name(self, project_name: str) -> str:
        return f"{self._project_prefix}{project_name.replace('/', '--')}--secrets"

    def _get_user_resource_name(self, job: Job) -> str:
        return (self._docker_secret_name_prefix + job.owner.replace("/", "--")).lower()

    def _get_docker_secret_name(self, job: Job) -> str:
        return self._get_user_resource_name(job)

    def _get_project_resource_name(self, job: Job) -> str:
        return (self._project_prefix + job.org_project_hash.hex()).lower()

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
        org_project_labels = self._get_org_project_labels(job)
        try:
            await self._client.create_default_network_policy(
                name,
                pod_labels=pod_labels,
                org_project_labels=org_project_labels,
                namespace_name=self._kube_config.namespace,
            )
            logger.info("Created default network policy for user '%s'", job.owner)
        except AlreadyExistsException:
            logger.info(
                "Default network policy for user '%s' already exists.", job.owner
            )

    async def _create_project_network_policy(self, job: Job) -> None:
        name = self._get_project_resource_name(job)
        pod_labels = self._get_project_pod_labels(job)
        pod_labels.update(self._get_org_pod_labels(job))
        org_project_labels = self._get_org_project_labels(job)

        project = job.project_name
        org = job.org_name or "no_org"
        try:
            await self._client.create_default_network_policy(
                name,
                pod_labels=pod_labels,
                org_project_labels=org_project_labels,
                namespace_name=self.kube_config.namespace,
            )
            logger.info(
                "Created default network policy for org=%s project=%s", org, project
            )
        except AlreadyExistsException:
            logger.info(
                "Default network policy for project org=%s project=%s already exists.",
                org,
                project,
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
        except NotFoundException:
            logger.info("Network policy %s not found", name)
        except Exception as e:
            logger.error("Failed to remove network policy %s: %s", name, e)

    def _create_pod_descriptor(
        self, job: Job, tolerate_unreachable_node: bool = False
    ) -> PodDescriptor:
        pool_types = self._get_job_resource_pool_types(job)
        if not pool_types:
            raise JobUnschedulableException("Job cannot be scheduled")
        logger.info(
            "Job %s is scheduled to run in pool types %s",
            job.id,
            ", ".join(p.name for p in pool_types),
        )

        tolerations = self._get_pod_tolerations(
            job, pool_types, tolerate_unreachable_node=tolerate_unreachable_node
        )
        node_affinity = self._get_pod_node_affinity(pool_types)
        pod_affinity = self._get_job_pod_pod_affinity()
        labels = self._get_pod_labels(job)

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
            storage_volume_factory=self.create_storage_volumes,
            storage_volume_mount_factory=self.create_storage_volume_mounts,
            secret_volume_factory=self.create_secret_volume,
            image_pull_secret_names=pull_secrets,
            tolerations=tolerations,
            node_affinity=node_affinity,
            pod_affinity=pod_affinity,
            labels=labels,
            priority_class_name=self._kube_config.jobs_pod_priority_class_name,
            restart_policy=self._get_pod_restart_policy(job),
            meta_env=meta_env,
            privileged=job.privileged,
        )
        pod = self._update_pod_container_resources(pod, pool_types)
        pod = self._update_pod_image(job, pod)
        return self._update_pod_command(job, pod)

    def _get_job_resource_pool_types(self, job: Job) -> Sequence[ResourcePoolType]:
        job_preset = job.preset
        if job.preset_name and job_preset is None:
            logger.info("Preset %s not found for job %s", job.preset_name, job.id)
            return []
        if job_preset:
            return [
                p
                for p in self._orchestrator_config.resource_pool_types
                if p.name in job_preset.available_resource_pool_names
            ]

        job_resources = job.request.container.resources
        has_cpu_pools = any(
            not p.has_gpu for p in self._orchestrator_config.resource_pool_types
        )
        pool_types = []

        for pool_type in self._orchestrator_config.resource_pool_types:
            # Schedule jobs only on preemptible nodes if such node specified
            if job.preemptible_node and not pool_type.is_preemptible:
                continue
            if not job.preemptible_node and pool_type.is_preemptible:
                continue

            # Do not schedule cpu jobs on gpu nodes if cluster has
            # cpu only nodes.
            if has_cpu_pools and not job_resources.require_gpu and pool_type.has_gpu:
                continue

            if not job_resources.check_fit_into_pool_type(pool_type):
                continue

            pool_types.append(pool_type)

        if not pool_types:
            return []

        return pool_types

    # TODO: remove after cluster resources monitoring process is released
    def _update_pod_container_resources(
        self, pod: PodDescriptor, pool_types: Sequence[ResourcePoolType]
    ) -> PodDescriptor:
        if not pod.resources:
            return pod
        max_node_cpu = max(p.available_cpu or 0 for p in pool_types)
        max_node_memory = max(p.available_memory or 0 for p in pool_types)
        max_node_nvidia_gpu = max(p.nvidia_gpu or 0 for p in pool_types)
        max_node_amd_gpu = max(p.amd_gpu or 0 for p in pool_types)
        max_node_intel_gpu = max(p.intel_gpu or 0 for p in pool_types)
        pod_nvidia_gpu = pod.resources.nvidia_gpu or 0
        pod_amd_gpu = pod.resources.amd_gpu or 0
        pod_intel_gpu = pod.resources.intel_gpu or 0
        if (
            max_node_cpu > pod.resources.cpu
            or max_node_memory > pod.resources.memory
            or max_node_nvidia_gpu > pod_nvidia_gpu
            or max_node_amd_gpu > pod_amd_gpu
            or max_node_intel_gpu > pod_intel_gpu
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
        new_resources = replace(
            pod.resources, memory_request=int(pod.resources.memory * 0.8)
        )  # 1GB
        return replace(pod, resources=new_resources)

    def _update_pod_image(self, job: Job, pod: PodDescriptor) -> PodDescriptor:
        if job.is_external:
            return replace(pod, image=self._kube_config.external_job_runner_image)
        return pod

    def _update_pod_command(self, job: Job, pod: PodDescriptor) -> PodDescriptor:
        if job.is_external:
            return replace(
                pod,
                command=self._kube_config.external_job_runner_command,
                args=self._kube_config.external_job_runner_args,
            )
        return pod

    def _get_user_pod_labels(self, job: Job) -> dict[str, str]:
        return {"platform.neuromation.io/user": job.owner.replace("/", "--")}

    def _get_org_project_labels(self, job: Job) -> dict[str, str]:
        labels = {
            PROJECT_LABEL_KEY: job.project_name,
        }
        if job.org_name:
            labels[ORG_LABEL_KEY] = job.org_name
        return labels

    def _get_org_pod_labels(self, job: Job) -> dict[str, str]:
        # Org label must always be set. Prometheus doesn't return empty labels
        # in response which are required for Grafana tables plugin.
        return {"platform.neuromation.io/org": job.org_name or "no_org"}

    def _get_project_pod_labels(self, job: Job) -> dict[str, str]:
        return {"platform.neuromation.io/project": job.project_name}

    def _get_job_labels(self, job: Job) -> dict[str, str]:
        return {JOB_LABEL_KEY: job.id}

    def _get_preset_labels(self, job: Job) -> dict[str, str]:
        if not job.preset_name:
            return {}
        labels = {"platform.neuromation.io/preset": job.preset_name}
        if job.is_external:
            labels["platform.neuromation.io/external"] = "true"
        return labels

    def _get_pod_labels(self, job: Job) -> dict[str, str]:
        labels = self._get_job_labels(job)
        labels.update(self._get_user_pod_labels(job))
        labels.update(self._get_org_pod_labels(job))
        labels.update(self._get_project_pod_labels(job))
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
                pvc_user: str = pvc["metadata"]["labels"][
                    "platform.neuromation.io/user"
                ]
                pvc_project: str = pvc["metadata"]["labels"].get(
                    "platform.neuromation.io/project"
                )
                pvc_org: str | None = pvc["metadata"]["labels"].get(
                    "platform.neuromation.io/disk-api-org-name"
                )
                pvc_path = pvc_project or pvc_user
                pvc_path = pvc_path.replace("--", "/")
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
        return job.host_segment_named

    async def start_job(
        self, job: Job, tolerate_unreachable_node: bool = False
    ) -> JobStatus:
        """
        tolerate_unreachable_node: used only in tests
        """

        await self._create_docker_secret(job)
        await self._create_user_network_policy(job)
        await self._create_project_network_policy(job)
        try:
            await self._create_pod_network_policy(job)

            descriptor = self._create_pod_descriptor(
                job, tolerate_unreachable_node=tolerate_unreachable_node
            )
            pod = await self._client.create_pod(descriptor)

            logger.info("Starting Service for %s.", job.id)
            service = await self._create_service(descriptor)
            if job.is_named:
                # If old job finished recently, it pod can be still there
                # with corresponding service, so we should delete it here
                service_name = self._get_service_name_for_named(job)
                await self._delete_service(service_name, ignore_missing=True)
                await self._create_service(descriptor, name=service_name)

            if job.has_http_server_exposed:
                logger.info("Starting Ingress for %s", job.id)
                await self._create_ingress(job, service)
        except AlreadyExistsException as e:
            raise JobAlreadyExistsException(str(e))

        job.status_history.current = await self._get_job_status(job, pod)
        return job.status

    def _get_pod_tolerations(
        self,
        job: Job,
        pool_types: Sequence[ResourcePoolType],
        tolerate_unreachable_node: bool = False,
    ) -> list[Toleration]:
        tolerations = [
            Toleration(
                key=self._kube_config.jobs_pod_job_toleration_key,
                operator="Exists",
                effect="NoSchedule",
            )
        ]
        if job.has_nvidia_gpu or any(p.nvidia_gpu for p in pool_types):
            tolerations.append(
                Toleration(
                    key=Resources.nvidia_gpu_key,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        if job.has_amd_gpu or any(p.amd_gpu for p in pool_types):
            tolerations.append(
                Toleration(
                    key=Resources.amd_gpu_key,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        if job.has_intel_gpu or any(p.intel_gpu for p in pool_types):
            tolerations.append(
                Toleration(
                    key=Resources.intel_gpu_key,
                    operator="Exists",
                    effect="NoSchedule",
                )
            )
        if self._kube_config.jobs_pod_preemptible_toleration_key and (
            job.preemptible_node or any(p.is_preemptible for p in pool_types)
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
        self, pool_types: Sequence[ResourcePoolType]
    ) -> NodeAffinity | None:
        # NOTE:
        # The pod is scheduled onto a node only if at least one of
        # `LabelSelectorTerm`s is satisfied.
        # `LabelSelectorTerm` is satisfied only if its `match_expressions` are
        # satisfied.
        required_terms: list[LabelSelectorTerm] = []
        preferred_terms: list[NodePreferredSchedulingTerm] = []

        if self._kube_config.node_label_node_pool:
            for pool_type in pool_types:
                required_terms.append(
                    LabelSelectorTerm(
                        [
                            LabelSelectorMatchExpression.create_in(
                                self._kube_config.node_label_node_pool, pool_type.name
                            )
                        ]
                    )
                )
        if not required_terms:
            return None
        return NodeAffinity(required=required_terms, preferred=preferred_terms)

    def _get_job_pod_pod_affinity(self) -> PodAffinity:
        return PodAffinity(
            preferred=[
                PodPreferredSchedulingTerm(
                    PodAffinityTerm(
                        LabelSelectorTerm(
                            [
                                LabelSelectorMatchExpression.create_exists(
                                    JOB_LABEL_KEY
                                ),
                            ]
                        )
                    )
                )
            ]
        )

    def _get_job_pod_name(self, job: Job) -> str:
        # TODO (A Danshyn 11/15/18): we will need to start storing jobs'
        # kube pod names explicitly at some point
        return job.id

    async def get_job_status(self, job: Job) -> JobStatusItem:
        if job.is_finished:
            return job.status_history.current

        # handling PENDING/RUNNING jobs

        # In case this is an external job and node/pod was lost
        # we need to reschedule external job runner to continue manage
        # external job state, we treat it as restartable.
        if job.is_restartable or job.is_external:
            pod = await self._check_pod_lost(job)
        else:
            pod = await self._client.get_pod(self._get_job_pod_name(job))

        return await self._get_job_status(job, pod)

    async def _get_job_status(self, job: Job, pod: PodDescriptor) -> JobStatusItem:
        if job.is_external:
            return await self._get_external_job_status(job, pod)
        return await self._get_pod_status(job, pod)

    async def _get_external_job_status(
        self, job: Job, pod: PodDescriptor
    ) -> JobStatusItem:
        now = datetime.now(UTC)

        try:
            port = int(job.env.get("EXTERNAL_JOB_RUNNER_PORT", 8080))
            status_url = f"http://{job.internal_hostname}:{port}/api/v1/status"
            resp = await self._client.request("GET", status_url, raise_for_status=True)
            return JobStatusItem.create(
                JobStatus(resp["status"]),
                transition_time=now,
                reason=resp.get("reason"),
                description=resp.get("description"),
            )
        except aiohttp.ClientError:
            pass

        pod_status = await self._get_pod_status(job, pod)
        if pod_status.is_finished:
            return pod_status

        # Job is in terminating/terminated but pod phase has not moved to
        # succeeded/failed yet, continue returning running status.
        if job.is_running:
            return job.status_history.last

        return JobStatusItem.create(
            JobStatus.PENDING,
            transition_time=now,
            reason=JobStatusReason.CREATING,
            description="Starting external job runner",
        )

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
        now = datetime.now(UTC)
        assert pod.created_at is not None
        pod_events = await self._client.get_pod_events(
            self._get_job_pod_name(job), self._kube_config.namespace
        )

        if pod_status.is_scheduled:
            if pod_events:
                pod_events.sort(key=operator.attrgetter("timestamp"))
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

        logger.info("Found unscheduled pod. Job '%s'", job.id)

        # Jobs with scheduling enabled never timeout on k8s scheduler
        if job.scheduler_enabled:
            return job_status

        schedule_timeout = (
            job.schedule_timeout or self._orchestrator_config.job_schedule_timeout
        )

        scaleup_events = [e for e in pod_events if e.reason == "TriggeredScaleUp"]
        scaleup_events.sort(key=operator.attrgetter("last_timestamp"))
        if scaleup_events and (
            (now - scaleup_events[-1].timestamp).total_seconds()
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

    async def _check_pod_lost(self, job: Job) -> PodDescriptor:
        pod_name = self._get_job_pod_name(job)
        do_recreate_pod = False
        try:
            pod = await self._client.get_pod(pod_name)
            pod_status = pod.status
            assert pod_status is not None
            if pod_status.is_node_lost:
                logger.info("Detected NodeLost in pod '%s'. Job '%s'", pod_name, job.id)
                # if the pod's status reason is `NodeLost` regardless of
                # the pod's status phase, we need to forcefully delete the
                # pod and reschedule another one instead.
                logger.info("Forcefully deleting pod '%s'. Job '%s'", pod_name, job.id)
                await self._client.delete_pod(pod_name, force=True)
                do_recreate_pod = True
        except JobNotFoundException:
            logger.info("Pod '%s' was lost. Job '%s'", pod_name, job.id)
            # if the job is still in PENDING/RUNNING, but the underlying
            # pod is gone, this may mean that the node was
            # preempted/failed (the node resource may no longer exist)
            # and the pods GC evicted the pod, effectively by
            # forcefully deleting it.
            do_recreate_pod = True

        if do_recreate_pod:
            logger.info("Recreating preempted pod '%s'. Job '%s'", pod_name, job.id)
            descriptor = self._create_pod_descriptor(job)
            try:
                pod = await self._client.create_pod(descriptor)
            except JobError:
                # handling possible 422 and other failures
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

    async def _create_service(
        self, pod: PodDescriptor, name: str | None = None
    ) -> Service:
        service = Service.create_headless_for_pod(pod)
        if name is not None:
            service = service.make_named(name)
        return await self._client.create_service(service)

    async def _get_services(self, job: Job) -> list[Service]:
        return await self._client.list_services(self._get_job_labels(job))

    async def _delete_service(
        self, name: str, *, uid: str | None = None, ignore_missing: bool = False
    ) -> None:
        try:
            await self._client.delete_service(name=name, uid=uid)
        except NotFoundException:
            if ignore_missing:
                return
            logger.exception("Failed to remove service %s", name)

        except Exception:
            logger.exception("Failed to remove service %s", name)

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
            logger.warning("Failed to remove ingresses %s: %s", labels, e)

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
            logger.warning("Failed to remove ingress %s: %s", name, e)

    async def delete_all_job_resources(self, job_id: str) -> None:
        labels = {JOB_LABEL_KEY: job_id}
        await self._client.delete_all_pods(labels=labels)
        await self._client.delete_all_ingresses(labels=labels)
        await self._client.delete_all_services(labels=labels)
        await self._client.delete_all_network_policies(labels=labels)

    async def preempt_jobs(
        self, jobs_to_schedule: list[Job], preemptible_jobs: list[Job]
    ) -> list[Job]:
        job_pods_to_schedule = [
            self._create_pod_descriptor(job) for job in jobs_to_schedule
        ]
        preemptible_job_pods = []
        for job in preemptible_jobs:
            try:
                preemptible_job_pods.append(self._create_pod_descriptor(job))
            except JobError as exc:
                logger.warning(
                    "Failed to create pod from preemptible job %r. Reason: ",
                    job.id,
                    exc,
                )
        pods_to_preempt = self._preemption.get_pods_to_preempt(
            job_pods_to_schedule, preemptible_job_pods
        )
        pod_names_to_preempt = {pod.name for pod in pods_to_preempt}
        preempted_jobs = [
            job for job in preemptible_jobs if job.id in pod_names_to_preempt
        ]
        for job in preempted_jobs:
            await self.delete_job(job)
        return preempted_jobs

    async def get_scheduled_jobs(self, jobs: list[Job]) -> list[Job]:
        scheduled = []
        for job in jobs:
            if self._scheduler.is_pod_scheduled(job.id):
                scheduled.append(job)
        return scheduled

    async def get_schedulable_jobs(self, jobs: list[Job]) -> list[Job]:
        job_pods = []
        for job in jobs:
            try:
                job_pods.append(self._create_pod_descriptor(job))
            except JobError as exc:
                logger.debug("Job %r cannot be scheduled. Reason: %s", job.id, exc)
        schedulable_pods = self._scheduler.get_schedulable_pods(job_pods)
        schedulable_pod_names = {pod.name for pod in schedulable_pods}
        return [job for job in jobs if job.id in schedulable_pod_names]
