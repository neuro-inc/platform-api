import abc
import asyncio
import enum
import json
import logging
from asyncio import timeout
from base64 import b64encode
from collections.abc import (
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import PurePath
from typing import Any, ClassVar, NoReturn, Optional, Self

import aiohttp
from apolo_kube_client import (
    CollectionModel,
    CoreV1Event,
    KubeClient as KubeClient,
    KubeClientException,
    KubeClientProxy,
    KubeClientUnauthorized,
    ResourceExists,
    ResourceExists as ApoloResourceExists,
    ResourceGone,
    ResourceInvalid,
    ResourceModel,
    ResourceNotFound as ApoloResourceNotFound,
    V1Affinity,
    V1Container,
    V1ContainerPort,
    V1ContainerStatus,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1EnvVarSource,
    V1HTTPGetAction,
    V1HTTPIngressPath,
    V1HTTPIngressRuleValue,
    V1Ingress,
    V1IngressBackend,
    V1IngressRule,
    V1IngressServiceBackend,
    V1IngressSpec,
    V1LabelSelector,
    V1LabelSelectorRequirement,
    V1LocalObjectReference,
    V1Node,
    V1NodeAffinity,
    V1NodeCondition,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1NodeStatus,
    V1ObjectMeta,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
    V1PodAffinity,
    V1PodAffinityTerm,
    V1PodCondition,
    V1PodSpec,
    V1PodStatus,
    V1PreferredSchedulingTerm,
    V1Probe,
    V1ResourceRequirements,
    V1Secret,
    V1SecretKeySelector,
    V1SecretVolumeSource,
    V1SecurityContext,
    V1Service,
    V1ServiceBackendPort,
    V1ServicePort,
    V1ServiceSpec,
    V1Taint,
    V1Toleration,
    V1Volume,
    V1VolumeMount,
    V1WeightedPodAffinityTerm,
    Watch,
    WatchEvent,
)
from yarl import URL

from .job_request import (
    Container,
    ContainerResources,
    ContainerTPUResource,
    ContainerVolume,
    DiskContainerVolume,
    JobAlreadyExistsException,
    JobError,
    JobNotFoundException,
    JobRequest,
    Secret,
    SecretContainerVolume,
)

logger = logging.getLogger(__name__)


class ServiceType(enum.StrEnum):
    CLUSTER_IP = "ClusterIP"
    NODE_PORT = "NodePort"
    LOAD_BALANCER = "LoadBalancer"


class GroupVersion(enum.StrEnum):
    NETWORKING_V1 = "networking.k8s.io/v1"


def _raise_status_job_exception(pod: dict[str, Any], job_id: str | None) -> NoReturn:
    if pod["code"] == 409:
        raise ResourceExists(pod.get("reason", "job already exists"))
    if pod["code"] == 404:
        raise JobNotFoundException(f"job {job_id} was not found")
    if pod["code"] == 422:
        raise JobError(f"cant create job with id {job_id}")
    raise JobError(f"unexpected payload: {pod}")


@dataclass(frozen=True)
class Volume(metaclass=abc.ABCMeta):
    name: str

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: PurePath | None = None,
    ) -> "VolumeMount":
        raise NotImplementedError("Cannot create mount for abstract Volume type.")

    def to_model(self) -> V1Volume:
        raise NotImplementedError


@dataclass(frozen=True)
class PathVolume(Volume):
    # None for cluster storage.
    # /org for organization/additional storage.
    path: PurePath | None

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: PurePath | None = None,
    ) -> "VolumeMount":
        try:
            sub_path = container_volume.src_path.relative_to(
                "/" if self.path is None else str(self.path)
            )
        except ValueError:
            sub_path = container_volume.src_path.relative_to("/")
        mount_sub_path = mount_sub_path or PurePath()
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path / mount_sub_path,
            sub_path=sub_path,
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class SharedMemoryVolume(Volume):
    def to_model(self) -> V1Volume:
        return V1Volume(
            name=self.name, empty_dir=V1EmptyDirVolumeSource(medium="Memory")
        )

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: PurePath | None = None,
    ) -> "VolumeMount":
        mount_sub_path = mount_sub_path or PurePath()
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path / mount_sub_path,
            sub_path=PurePath(),
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class SecretEnvVar:
    name: str
    secret: Secret

    @classmethod
    def create(cls, name: str, secret: Secret) -> "SecretEnvVar":
        return cls(name=name, secret=secret)

    def to_model(self) -> V1EnvVar:
        return V1EnvVar(
            name=self.name,
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name=self.secret.k8s_secret_name, key=self.secret.secret_key
                )
            ),
        )


@dataclass(frozen=True)
class VolumeMount:
    volume: Volume
    mount_path: PurePath
    sub_path: PurePath = PurePath()
    read_only: bool = False

    def to_model(self) -> V1VolumeMount:
        ret = V1VolumeMount(
            name=self.volume.name,
            mount_path=str(self.mount_path),
            read_only=self.read_only,
        )
        sub_path = str(self.sub_path)
        if sub_path:
            ret.sub_path = sub_path
        return ret


@dataclass(frozen=True)
class SecretVolume(Volume):
    k8s_secret_name: str

    def create_secret_mount(self, sec_volume: SecretContainerVolume) -> "VolumeMount":
        return VolumeMount(
            volume=self,
            mount_path=sec_volume.dst_path,
            sub_path=PurePath(sec_volume.secret.secret_key),
            read_only=True,
        )

    def to_model(self) -> V1Volume:
        return V1Volume(
            name=self.name,
            secret=V1SecretVolumeSource(
                secret_name=self.k8s_secret_name, default_mode=0o400
            ),
        )


@dataclass(frozen=True)
class PVCDiskVolume(Volume):
    claim_name: str

    def create_disk_mount(self, disk_volume: DiskContainerVolume) -> "VolumeMount":
        return VolumeMount(
            volume=self,
            mount_path=disk_volume.dst_path,
            read_only=disk_volume.read_only,
        )

    def to_model(self) -> V1Volume:
        return V1Volume(
            name=self.name,
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=self.claim_name
            ),
        )


@dataclass(frozen=True)
class Resources:
    cpu: float
    memory: int
    memory_request: int | None = None
    nvidia_gpu: int | None = None
    nvidia_migs: Mapping[str, int] | None = None
    amd_gpu: int | None = None
    intel_gpu: int | None = None
    shm: bool | None = None
    tpu_version: str | None = None
    tpu_cores: int | None = None

    nvidia_gpu_key: ClassVar[str] = "nvidia.com/gpu"
    nvidia_mig_key_prefix: ClassVar[str] = "nvidia.com/mig-"
    amd_gpu_key: ClassVar[str] = "amd.com/gpu"
    intel_gpu_key: ClassVar[str] = "gpu.intel.com/i915"
    tpu_key_prefix: ClassVar[str] = "cloud-tpus.google.com/"

    def __post_init__(self) -> None:
        if bool(self.tpu_version) ^ bool(self.tpu_cores):
            raise ValueError("invalid TPU configuration")

    @property
    def cpu_mcores(self) -> int:
        return int(self.cpu * 1000)

    @property
    def memory_str(self) -> str:
        return f"{self.memory}"

    @property
    def memory_request_str(self) -> str:
        return f"{self.memory_request}"

    @property
    def tpu_key(self) -> str:
        assert self.tpu_version
        return self.tpu_key_prefix + self.tpu_version

    def to_model(self) -> V1ResourceRequirements:
        ret = V1ResourceRequirements(
            requests={"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
            limits={"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
        )
        if self.nvidia_gpu:
            ret.requests[self.nvidia_gpu_key] = str(self.nvidia_gpu)
            ret.limits[self.nvidia_gpu_key] = str(self.nvidia_gpu)
        if self.nvidia_migs:
            for key, value in self.nvidia_migs.items():
                ret.requests[self.nvidia_mig_key_prefix + key] = str(value)
                ret.limits[self.nvidia_mig_key_prefix + key] = str(value)
        if self.amd_gpu:
            ret.requests[self.amd_gpu_key] = str(self.amd_gpu)
            ret.limits[self.amd_gpu_key] = str(self.amd_gpu)
        if self.intel_gpu:
            ret.requests[self.intel_gpu_key] = str(self.intel_gpu)
            ret.limits[self.intel_gpu_key] = str(self.intel_gpu)
        if self.tpu_version:
            ret.requests[self.tpu_key] = str(self.tpu_cores)
            ret.limits[self.tpu_key] = str(self.tpu_cores)
        if self.memory_request:
            ret.requests["memory"] = self.memory_request_str
        return ret

    @classmethod
    def from_model(cls, model: V1ResourceRequirements) -> "Resources":
        requests = model.requests or {}
        nvidia_gpu = None
        if cls.nvidia_gpu_key in requests:
            nvidia_gpu = int(requests[cls.nvidia_gpu_key])
        nvidia_migs: dict[str, int] = {}
        for key, value in requests.items():
            if key.startswith(cls.nvidia_mig_key_prefix):
                nvidia_migs[key[len(cls.nvidia_mig_key_prefix) :]] = int(value)
        amd_gpu = None
        if cls.amd_gpu_key in requests:
            amd_gpu = int(requests[cls.amd_gpu_key])
        intel_gpu = None
        if cls.intel_gpu_key in requests:
            intel_gpu = int(requests[cls.intel_gpu_key])
        tpu_version, tpu_cores = cls._parse_tpu(requests)
        return cls(
            cpu=cls.parse_cpu(requests.get("cpu", "0")),
            memory=cls.parse_memory(requests.get("memory", "0Mi")),
            nvidia_gpu=nvidia_gpu,
            nvidia_migs=nvidia_migs or None,
            amd_gpu=amd_gpu,
            intel_gpu=intel_gpu,
            tpu_version=tpu_version,
            tpu_cores=tpu_cores,
        )

    @classmethod
    def parse_cpu(cls, cpu: str) -> float:
        try:
            return float(cpu)
        except ValueError:
            return float(cpu[:-1]) / 1000

    @classmethod
    def parse_memory(cls, memory: str) -> int:
        try:
            memory_b = int(memory)
        except ValueError:
            if memory.endswith("Ki"):
                memory_b = int(memory[:-2]) * 1024
            elif memory.endswith("k"):
                memory_b = int(memory[:-1]) * 1000
            elif memory.endswith("Mi"):
                memory_b = int(memory[:-2]) * 1024**2
            elif memory.endswith("M"):
                memory_b = int(memory[:-1]) * 1000**2
            elif memory.endswith("Gi"):
                memory_b = int(memory[:-2]) * 1024**3
            elif memory.endswith("G"):
                memory_b = int(memory[:-1]) * 1000**3
            elif memory.endswith("Ti"):
                memory_b = int(memory[:-2]) * 1024**4
            elif memory.endswith("T"):
                memory_b = int(memory[:-1]) * 1000**4
            else:
                raise ValueError(f"{memory!r} memory format is not supported")
        return memory_b

    @classmethod
    def _parse_tpu(cls, payload: dict[str, Any]) -> tuple[str | None, int | None]:
        for key, value in payload.items():
            if key.startswith(cls.tpu_key_prefix):
                return key.split("/")[-1], int(value)
        return None, None

    @classmethod
    def _parse_tpu_resource(cls, tpu: ContainerTPUResource) -> tuple[str, int]:
        try:
            tpu_version, tpu_cores = tpu.type.rsplit("-", 1)
            return tpu_version, int(tpu_cores)
        except (ValueError, TypeError):
            raise ValueError(f"invalid TPU type format: '{tpu.type}'")

    @classmethod
    def from_container_resources(cls, resources: ContainerResources) -> "Resources":
        kwargs: dict[str, Any] = {}
        if resources.tpu:
            kwargs["tpu_version"], kwargs["tpu_cores"] = cls._parse_tpu_resource(
                resources.tpu
            )
        return cls(
            cpu=resources.cpu,
            memory=resources.memory,
            nvidia_gpu=resources.nvidia_gpu,
            amd_gpu=resources.amd_gpu,
            intel_gpu=resources.intel_gpu,
            shm=resources.shm,
            **kwargs,
        )


@dataclass(frozen=True)
class Service:
    name: str
    target_port: int | None
    uid: str | None = None
    selector: dict[str, str] = field(default_factory=dict)
    port: int = 80
    service_type: ServiceType = ServiceType.CLUSTER_IP
    cluster_ip: str | None = None
    labels: dict[str, str] = field(default_factory=dict)

    def _add_port_map(
        self,
        port: int | None,
        target_port: int | None,
        port_name: str,
        ports: list[dict[str, Any]],
    ) -> None:
        if target_port:
            ports.append({"port": port, "targetPort": target_port, "name": port_name})

    def to_model(self) -> V1Service:
        ports: list[V1ServicePort] = []
        if self.target_port:
            ports.append(
                V1ServicePort(name="http", port=self.port, target_port=self.target_port)
            )
        spec = V1ServiceSpec(
            type=self.service_type.value,
            selector=self.selector or {},
            ports=ports or [],
        )
        if self.cluster_ip is not None:
            spec.cluster_ip = self.cluster_ip
        metadata = V1ObjectMeta(name=self.name)
        if self.labels:
            metadata.labels = self.labels.copy()
        return V1Service(metadata=metadata, spec=spec)

    @classmethod
    def create_for_pod(cls, pod: "PodDescriptor") -> "Service":
        return cls(
            name=pod.name,
            selector=pod.labels,
            target_port=pod.port,
            labels=pod.labels,
        )

    @classmethod
    def create_headless_for_pod(cls, pod: "PodDescriptor") -> "Service":
        http_port = pod.port or cls.port
        return cls(
            name=pod.name,
            selector=pod.labels,
            cluster_ip="None",
            target_port=http_port,
            labels=pod.labels,
        )

    def make_named(self, name: str) -> "Service":
        return replace(self, name=name)

    @classmethod
    def _find_port_by_name(
        cls, name: str, port_mappings: list[dict[str, Any]]
    ) -> dict[str, Any]:
        for port_mapping in port_mappings:
            if port_mapping.get("name", None) == name:
                return port_mapping
        return {}

    @classmethod
    def from_model(cls, model: V1Service) -> "Service":
        spec = model.spec or V1ServiceSpec()
        http_port: V1ServicePort | None = None
        for p in spec.ports or []:
            if p.name == "http":
                http_port = p
                break
        target_port: int | None = None
        if http_port is not None:
            tp = http_port.target_port
            if isinstance(tp, int):
                target_port = tp
        return cls(
            name=(model.metadata.name if model.metadata.name else ""),
            uid=model.metadata.uid,
            selector=(spec.selector or {}),
            target_port=target_port,
            port=(http_port.port if http_port and http_port.port else Service.port),
            service_type=ServiceType(spec.type or Service.service_type.value),
            cluster_ip=spec.cluster_ip,
            labels=(
                model.metadata.labels
                if model.metadata and model.metadata.labels
                else {}
            ),
        )


@dataclass(frozen=True)
class IngressRule:
    host: str
    service_name: str | None = None
    service_port: int | None = None

    @classmethod
    def from_model(cls, model: V1IngressRule) -> Self:
        host = model.host or ""
        if model.http is None:
            return cls(host=host, service_name=None, service_port=None)
        http_paths = model.http.paths
        if not http_paths:
            return cls(host=host, service_name=None, service_port=None)
        http_path = http_paths[0]
        service = http_path.backend.service
        if service is None:
            return cls(host=host, service_name=None, service_port=None)
        return cls(
            host=model.host or "",
            service_name=service.name,
            service_port=service.port.number,
        )

    @classmethod
    def from_service(cls, host: str, service: Service) -> "IngressRule":
        return cls(host=host, service_name=service.name, service_port=service.port)

    def to_model(self) -> V1IngressRule:
        http_rule = None
        if self.service_name and self.service_port:
            http_rule = V1HTTPIngressRuleValue(
                paths=[
                    V1HTTPIngressPath(
                        path_type="ImplementationSpecific",
                        backend=V1IngressBackend(
                            service=V1IngressServiceBackend(
                                name=self.service_name,
                                port=V1ServiceBackendPort(number=self.service_port),
                            )
                        ),
                    )
                ]
            )
        return V1IngressRule(host=self.host, http=http_rule)


@dataclass(frozen=True)
class Ingress:
    name: str
    ingress_class: str | None = None
    rules: list[IngressRule] = field(default_factory=list)
    annotations: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)

    def to_model(self) -> V1Ingress:
        spec = V1IngressSpec()
        annotations = self.annotations.copy()
        if self.ingress_class:
            spec.ingress_class_name = self.ingress_class
            annotations.pop(
                "kubernetes.io/ingress.class", None
            )  # deprecated and has conflict with ingressClassName
        metadata = V1ObjectMeta(name=self.name, annotations=annotations)
        if self.labels:
            metadata.labels = self.labels.copy()
        spec.rules = [rule.to_model() for rule in self.rules]
        return V1Ingress(metadata=metadata, spec=spec)

    @classmethod
    def from_model(cls, model: V1Ingress) -> "Ingress":
        metadata = model.metadata or V1ObjectMeta()
        spec = model.spec or V1IngressSpec()
        rules = []
        for r in spec.rules or []:
            host = r.host or ""
            service_name = None
            service_port = None
            if r.http and r.http.paths:
                path0 = r.http.paths[0]
                if path0 and path0.backend and path0.backend.service:
                    service_name = path0.backend.service.name
                    port = path0.backend.service.port
                    if port and port.number is not None:
                        service_port = port.number
            rules.append(
                IngressRule(
                    host=host, service_name=service_name, service_port=service_port
                )
            )
        return cls(
            name=metadata.name or "",
            ingress_class=(
                spec.ingress_class_name
                or (metadata.annotations or {}).get("kubernetes.io/ingress.class")
            ),
            rules=rules,
            annotations=(metadata.annotations or {}),
            labels=(metadata.labels or {}),
        )

    def find_rule_index_by_host(self, host: str) -> int:
        for idx, rule in enumerate(self.rules):
            if rule.host == host:
                return idx
        return -1


@dataclass(frozen=True)
class DockerRegistrySecret:
    # TODO (A Danshyn 11/16/18): these two attributes along with `type` and
    # `data` should be extracted into a parent class.
    name: str

    username: str
    password: str
    email: str
    registry_server: str

    # TODO (A Danshyn 11/16/18): should this be Optional?
    type: str = "kubernetes.io/dockerconfigjson"

    def _build_json(self) -> str:
        return b64encode(
            json.dumps(
                {
                    "auths": {
                        self.registry_server: {
                            "username": self.username,
                            "password": self.password,
                            "email": self.email,
                            "auth": b64encode(
                                (self.username + ":" + self.password).encode("utf-8")
                            ).decode("ascii"),
                        }
                    }
                }
            ).encode("utf-8")
        ).decode("ascii")

    def to_model(self) -> V1Secret:
        metadata = V1ObjectMeta(name=self.name)
        secret = V1Secret(metadata=metadata, type=self.type)
        # The client library expects .data as dict[str, str] base64 values
        secret.data = {".dockerconfigjson": self._build_json()}
        return secret


@dataclass(frozen=True)
class SecretRef:
    name: str

    def to_model(self) -> V1LocalObjectReference:
        return V1LocalObjectReference(name=self.name)

    @classmethod
    def from_model(cls, model: V1LocalObjectReference) -> Self:
        assert model.name is not None
        return cls(name=model.name)


@dataclass(frozen=True)
class Toleration:
    """
    https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.12/#toleration-v1-core
    """

    key: str
    operator: str = "Equal"
    value: str = ""
    effect: str = ""

    def to_model(self) -> V1Toleration:
        return V1Toleration(
            key=self.key,
            operator=self.operator,
            value=self.value,
            effect=self.effect,
        )


class SelectorOperator(enum.StrEnum):
    DOES_NOT_EXIST = "DoesNotExist"
    EXISTS = "Exists"
    IN = "In"
    NOT_IN = "NotIn"
    GT = "Gt"
    LT = "Lt"

    @property
    def requires_no_values(self) -> bool:
        return self in (self.DOES_NOT_EXIST, self.EXISTS)

    def apply(self, label_value: str | None, values: list[str]) -> bool:
        if self == self.IN:
            assert values, "Values are required"
            return label_value is not None and label_value in values
        if self == self.EXISTS:
            return label_value is not None
        if self == self.DOES_NOT_EXIST:
            return label_value is None
        if self == self.GT:
            assert len(values) == 1, "Exactly one value is required"
            return label_value is not None and int(label_value) > int(values[0])
        if self == self.LT:
            assert len(values) == 1, "Exactly one value is required"
            return label_value is not None and int(label_value) < int(values[0])
        raise NotImplementedError


@dataclass(frozen=True)
class LabelSelectorMatchExpression:
    key: str
    operator: SelectorOperator
    values: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("blank key")
        if self.operator.requires_no_values and self.values:
            raise ValueError("values must be empty")

    @classmethod
    def create_in(cls, key: str, *values: str) -> Self:
        return cls(key=key, operator=SelectorOperator.IN, values=[*values])

    @classmethod
    def create_exists(cls, key: str) -> Self:
        return cls(key=key, operator=SelectorOperator.EXISTS)

    @classmethod
    def create_does_not_exist(cls, key: str) -> Self:
        return cls(key=key, operator=SelectorOperator.DOES_NOT_EXIST)

    @classmethod
    def create_gt(cls, key: str, value: int) -> Self:
        return cls(key=key, operator=SelectorOperator.GT, values=[str(value)])

    @classmethod
    def create_lt(cls, key: str, value: int) -> Self:
        return cls(key=key, operator=SelectorOperator.LT, values=[str(value)])

    def is_satisfied(self, node_labels: dict[str, str]) -> bool:
        label_value = node_labels.get(self.key)
        return self.operator.apply(label_value, self.values)

    def to_model(self) -> V1LabelSelectorRequirement:
        ret = V1LabelSelectorRequirement(key=self.key, operator=self.operator.value)
        if self.values:
            ret.values = self.values.copy()
        return ret

    def to_node_requirement(self) -> V1NodeSelectorRequirement:
        ret = V1NodeSelectorRequirement(key=self.key, operator=self.operator.value)
        if self.values:
            ret.values = self.values.copy()
        return ret


@dataclass(frozen=True)
class LabelSelectorTerm:
    match_expressions: list[LabelSelectorMatchExpression]

    def __post_init__(self) -> None:
        if not self.match_expressions:
            raise ValueError("no expressions")

    def is_satisfied(self, node_labels: dict[str, str]) -> bool:
        return all(e.is_satisfied(node_labels) for e in self.match_expressions)

    def to_model(self) -> V1LabelSelector:
        return V1LabelSelector(
            match_expressions=[expr.to_model() for expr in self.match_expressions]
        )

    def to_node_selector_term(self) -> V1NodeSelectorTerm:
        return V1NodeSelectorTerm(
            match_expressions=[
                expr.to_node_requirement() for expr in self.match_expressions
            ]
        )


@dataclass(frozen=True)
class NodePreferredSchedulingTerm:
    preference: LabelSelectorTerm
    weight: int = 100

    def to_model(self) -> V1PreferredSchedulingTerm:
        return V1PreferredSchedulingTerm(
            preference=self.preference.to_node_selector_term(), weight=self.weight
        )


@dataclass(frozen=True)
class NodeAffinity:
    required: list[LabelSelectorTerm] = field(default_factory=list)
    preferred: list[NodePreferredSchedulingTerm] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.required and not self.preferred:
            raise ValueError("no terms")

    def is_satisfied(self, node_labels: dict[str, str]) -> bool:
        return any(t.is_satisfied(node_labels) for t in self.required)

    def to_model(self) -> V1NodeAffinity:
        ret = V1NodeAffinity()
        if self.required:
            ret.required_during_scheduling_ignored_during_execution = V1NodeSelector(
                node_selector_terms=[
                    term.to_node_selector_term() for term in self.required
                ]
            )
        if self.preferred:
            ret.preferred_during_scheduling_ignored_during_execution = [
                term.to_model() for term in self.preferred
            ]
        return ret


@dataclass(frozen=True)
class PodAffinityTerm:
    label_selector: LabelSelectorTerm
    topologyKey: str = "kubernetes.io/hostname"  # noqa: N815
    namespaces: list[str] = field(default_factory=list)

    def is_satisfied(self, pod_labels: dict[str, str]) -> bool:
        return self.label_selector.is_satisfied(pod_labels)

    def to_model(self) -> V1PodAffinityTerm:
        result = V1PodAffinityTerm(
            label_selector=self.label_selector.to_model(),
            topology_key=self.topologyKey,
        )
        if self.namespaces:
            result.namespaces = self.namespaces.copy()
        return result


@dataclass(frozen=True)
class PodPreferredSchedulingTerm:
    pod_affinity_term: PodAffinityTerm
    weight: int = 100

    def to_model(self) -> V1WeightedPodAffinityTerm:
        return V1WeightedPodAffinityTerm(
            pod_affinity_term=self.pod_affinity_term.to_model(),
            weight=self.weight,
        )


@dataclass(frozen=True)
class PodAffinity:
    preferred: list[PodPreferredSchedulingTerm] = field(default_factory=list)

    def to_model(self) -> V1PodAffinity:
        ret = V1PodAffinity()
        if self.preferred:
            ret.preferred_during_scheduling_ignored_during_execution = [
                term.to_model() for term in self.preferred
            ]
        return ret


@enum.unique
class PodRestartPolicy(enum.StrEnum):
    ALWAYS = "Always"
    ON_FAILURE = "OnFailure"
    NEVER = "Never"


@dataclass(frozen=True)
class PodDescriptor:
    name: str
    image: str
    command: list[str] = field(default_factory=list)
    args: list[str] = field(default_factory=list)
    working_dir: str | None = None
    env: dict[str, str] = field(default_factory=dict)
    # TODO (artem): create base type `EnvVar` and merge `env` and `secret_env`
    secret_env_list: list[SecretEnvVar] = field(default_factory=list)
    volume_mounts: list[VolumeMount] = field(default_factory=list)
    volumes: list[Volume] = field(default_factory=list)
    resources: Resources | None = None
    node_selector: dict[str, str] = field(default_factory=dict)
    tolerations: list[Toleration] = field(default_factory=list)
    node_affinity: NodeAffinity | None = None
    pod_affinity: PodAffinity | None = None
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)

    port: int | None = None
    health_check_path: str = "/"
    tty: bool = False

    status: Optional["PodStatus"] = None

    image_pull_secrets: list[SecretRef] = field(default_factory=list)

    # TODO (A Danshyn 12/09/2018): expose readiness probe properly
    readiness_probe: bool = False

    node_name: str | None = None
    priority_class_name: str | None = None

    created_at: datetime | None = None

    tpu_version_annotation_key: ClassVar[str] = "tf-version.cloud-tpus.google.com"

    restart_policy: PodRestartPolicy = PodRestartPolicy.NEVER

    privileged: bool = False

    @classmethod
    def _process_secret_volumes(
        cls,
        container: Container,
        secret_volume_factory: Callable[[str], SecretVolume] | None = None,
    ) -> tuple[list[SecretVolume], list[VolumeMount]]:
        path_to_secret_volumes = container.get_path_to_secret_volumes()
        if not secret_volume_factory:
            return [], []

        pod_volumes = []
        volume_mounts = []

        for path, secret_volumes in path_to_secret_volumes.items():
            volume = secret_volume_factory(path)
            pod_volumes.append(volume)

            for sec_volume in secret_volumes:
                volume_mounts.append(volume.create_secret_mount(sec_volume))

        return pod_volumes, volume_mounts

    @classmethod
    def _process_disk_volumes(
        cls, disk_volumes: list[DiskContainerVolume]
    ) -> tuple[list[PVCDiskVolume], list[VolumeMount]]:
        pod_volumes = []
        volume_mounts = []

        pvc_volumes: dict[str, PVCDiskVolume] = {}
        for index, disk_volume in enumerate(disk_volumes, 1):
            pvc_volume = pvc_volumes.get(disk_volume.disk.disk_id)
            if pvc_volume is None:
                pvc_volume = PVCDiskVolume(
                    name=f"disk-{index}", claim_name=disk_volume.disk.disk_id
                )
                pod_volumes.append(pvc_volume)
                pvc_volumes[disk_volume.disk.disk_id] = pvc_volume
            volume_mounts.append(pvc_volume.create_disk_mount(disk_volume))
        return pod_volumes, volume_mounts

    @classmethod
    def from_job_request(
        cls,
        job_request: JobRequest,
        secret_volume_factory: Callable[[str], SecretVolume] | None = None,
        image_pull_secret_names: list[str] | None = None,
        node_selector: dict[str, str] | None = None,
        tolerations: list[Toleration] | None = None,
        node_affinity: NodeAffinity | None = None,
        pod_affinity: PodAffinity | None = None,
        labels: dict[str, str] | None = None,
        restart_policy: PodRestartPolicy = PodRestartPolicy.NEVER,
        meta_env: dict[str, str] | None = None,
        privileged: bool = False,
    ) -> "PodDescriptor":
        container = job_request.container

        secret_volumes, secret_volume_mounts = cls._process_secret_volumes(
            container, secret_volume_factory
        )
        disk_volumes, disk_volume_mounts = cls._process_disk_volumes(
            container.disk_volumes
        )

        volumes = [*secret_volumes, *disk_volumes]
        volume_mounts = [
            *secret_volume_mounts,
            *disk_volume_mounts,
        ]

        sec_env_list = [
            SecretEnvVar.create(env_name, secret)
            for env_name, secret in container.secret_env.items()
        ]

        if job_request.container.resources.shm:
            dev_shm_volume = SharedMemoryVolume(name="dshm")
            container_volume = ContainerVolume(
                URL(""),
                dst_path=PurePath("/dev/shm"),
                read_only=False,
            )
            volume_mounts.append(dev_shm_volume.create_mount(container_volume))
            volumes.append(dev_shm_volume)

        resources = Resources.from_container_resources(container.resources)
        if image_pull_secret_names is not None:
            image_pull_secrets = [SecretRef(name) for name in image_pull_secret_names]
        else:
            image_pull_secrets = []

        annotations: dict[str, str] = {}
        if container.resources.tpu:
            annotations[cls.tpu_version_annotation_key] = (
                container.resources.tpu.software_version
            )

        env = container.env.copy()
        if meta_env:
            env.update(meta_env)

        return cls(
            name=job_request.job_id,
            image=container.image,
            command=container.entrypoint_list,
            args=container.command_list,
            working_dir=container.working_dir,
            env=env,
            secret_env_list=sec_env_list,
            volume_mounts=volume_mounts,
            volumes=volumes,
            resources=resources,
            port=container.port,
            health_check_path=container.health_check_path,
            tty=container.tty,
            image_pull_secrets=image_pull_secrets,
            node_selector=node_selector or {},
            tolerations=tolerations or [],
            node_affinity=node_affinity,
            pod_affinity=pod_affinity,
            labels=labels or {},
            annotations=annotations,
            restart_policy=restart_policy,
            privileged=privileged,
        )

    @property
    def env_list(self) -> list[dict[str, str]]:
        return [{"name": name, "value": value} for name, value in self.env.items()]

    def can_be_scheduled(self, node_labels: dict[str, str]) -> bool:
        affinities: list[NodeAffinity] = []
        if self.node_selector:
            requirements = [
                LabelSelectorMatchExpression.create_in(k, v)
                for k, v in self.node_selector.items()
            ]
            affinities.append(NodeAffinity(required=[LabelSelectorTerm(requirements)]))
        if self.node_affinity:
            affinities.append(self.node_affinity)
        return all(a.is_satisfied(node_labels) for a in affinities)

    def to_model(self) -> V1Pod:
        volume_mounts = [mount.to_model() for mount in self.volume_mounts]
        volumes = [volume.to_model() for volume in self.volumes]
        env_list = [V1EnvVar(name=k, value=v) for k, v in self.env.items()] + [
            env.to_model() for env in self.secret_env_list
        ]

        container = V1Container(
            name=f"{self.name}",
            image=f"{self.image}",
            image_pull_policy="Always",
            env=env_list,
            volume_mounts=volume_mounts,
            termination_message_policy="FallbackToLogsOnError",
        )
        if self.command:
            container.command = self.command
        if self.args:
            container.args = self.args
        if self.resources:
            container.resources = self.resources.to_model()
        if self.tty:
            container.tty = True
        container.stdin = True
        if self.working_dir is not None:
            container.working_dir = self.working_dir
        if self.privileged:
            container.security_context = V1SecurityContext(
                privileged=self.privileged,
            )

        ports = self._to_model_ports()
        if ports:
            container.ports = ports
        if self.readiness_probe and self.port:
            container.readiness_probe = self._to_model_readiness_probe()

        model = V1Pod(
            kind="Pod",
            api_version="v1",
            metadata=V1ObjectMeta(name=self.name),
            spec=V1PodSpec(
                automount_service_account_token=False,
                containers=[container],
                volumes=volumes,
                restart_policy=str(self.restart_policy),
                image_pull_secrets=[
                    secret.to_model() for secret in self.image_pull_secrets
                ],
                tolerations=[toleration.to_model() for toleration in self.tolerations],
            ),
        )
        assert model.spec is not None
        if self.labels:
            model.metadata.labels = self.labels
        if self.annotations:
            model.metadata.annotations = self.annotations.copy()
        if self.node_selector:
            model.spec.node_selector = self.node_selector.copy()
        if self.node_affinity or self.pod_affinity:
            model.spec.affinity = V1Affinity()
        if self.node_affinity:
            model.spec.affinity.node_affinity = self.node_affinity.to_model()
        if self.pod_affinity:
            model.spec.affinity.pod_affinity = self.pod_affinity.to_model()
        if self.priority_class_name:
            model.spec.priority_class_name = self.priority_class_name
        return model

    def _to_model_ports(self) -> list[V1ContainerPort]:
        ports = []
        if self.port:
            ports.append(V1ContainerPort(container_port=self.port))
        return ports

    def _to_model_readiness_probe(self) -> V1Probe:
        return V1Probe(
            http_get=V1HTTPGetAction(port=self.port, path=self.health_check_path),
            initial_delay_seconds=1,
            period_seconds=1,
        )

    @classmethod
    def _assert_resource_kind(cls, expected_kind: str, payload: dict[str, Any]) -> None:
        kind = payload["kind"]
        if kind == "Status":
            _raise_status_job_exception(payload, job_id="")
        elif kind != expected_kind:
            raise ValueError(f"unknown kind: {kind}")

    @classmethod
    def from_model(cls, model: V1Pod) -> Self:
        metadata = model.metadata
        assert model.spec is not None
        container = model.spec.containers[0]
        status = None
        if model.status is not None:
            status = PodStatus.from_model(model.status)
        if model.spec.image_pull_secrets is not None:
            secrets = [
                SecretRef.from_model(secret) for secret in model.spec.image_pull_secrets
            ]
        else:
            secrets = []
        tolerations = [
            Toleration(
                key=t.key or "",
                operator=t.operator or Toleration.operator,
                value=t.value or Toleration.value,
                effect=t.effect or Toleration.effect,
            )
            for t in (model.spec.tolerations or ())
        ]
        assert metadata.name is not None
        assert container.image is not None
        return cls(
            name=metadata.name,
            created_at=metadata.creation_timestamp,
            image=container.image,
            status=status,
            image_pull_secrets=secrets,
            node_name=model.spec.node_name,
            command=container.command,
            args=container.args,
            tty=container.tty or False,
            tolerations=tolerations,
            labels=metadata.labels or {},
            priority_class_name=model.spec.priority_class_name,
            restart_policy=PodRestartPolicy(
                model.spec.restart_policy or str(cls.restart_policy)
            ),
            working_dir=container.working_dir,
            resources=Resources.from_model(container.resources),
        )


@dataclass(frozen=True)
class ContainerStateRunning:
    started_at: datetime


class ContainerStatus:
    def __init__(self, model: V1ContainerStatus | None) -> None:
        self._model = model

    @property
    def is_waiting(self) -> bool:
        return not self.is_running and not self.is_terminated

    @property
    def is_running(self) -> bool:
        if self._model is None:
            return False
        return self._model.state.running is not None

    @property
    def is_terminated(self) -> bool:
        if self._model is None:
            return False
        return self._model.state.terminated is not None

    @property
    def reason(self) -> str | None:
        """Return the reason of the current state.

        'waiting' reasons:
            'PodInitializing'
            'ContainerCreating'
            'ErrImagePull'
            'CrashLoopBackOff'
        see
        https://github.com/kubernetes/kubernetes/blob/29232e3edc4202bb5e34c8c107bae4e8250cd883/pkg/kubelet/kubelet_pods.go#L1463-L1468
        https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43

        'terminated' reasons:
            'OOMKilled'
            'Completed'
            'Error'
            'ContainerCannotRun'
        see
        https://github.com/kubernetes/kubernetes/blob/c65f65cf6aea0f73115a2858a9d63fc2c21e5e3b/pkg/kubelet/dockershim/docker_container.go#L306-L409
        """
        if self.is_terminated:
            assert self._model is not None
            assert self._model.state.terminated is not None
            return self._model.state.terminated.reason
        if self.is_running:
            return None
        # waiting
        if self._model is None:
            return None
        if self._model.state.waiting is not None:
            return self._model.state.waiting.reason
        return None

    @property
    def message(self) -> str | None:
        if self.is_terminated:
            assert self._model is not None
            assert self._model.state.terminated is not None
            return self._model.state.terminated.message
        if self.is_running:
            return None
        # waiting
        if self._model is None:
            return None
        if self._model.state.waiting is not None:
            return self._model.state.waiting.message
        return None

    @property
    def exit_code(self) -> int | None:
        assert self.is_terminated
        assert self._model is not None
        assert self._model.state.terminated is not None
        return self._model.state.terminated.exit_code

    @property
    def is_creating(self) -> bool:
        # TODO (A Danshyn 07/20/18): handle PodInitializing
        # TODO (A Danshyn 07/20/18): consider handling other reasons
        # https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43
        return self.is_waiting and self.reason in (None, "ContainerCreating")

    @classmethod
    def from_model(cls, model: V1ContainerStatus) -> Self:
        return cls(model)


class PodConditionType(enum.StrEnum):
    UNKNOWN = "Unknown"
    POD_SCHEDULED = "PodScheduled"
    READY = "Ready"
    INITIALIZED = "Initialized"
    CONTAINERS_READY = "ContainersReady"


class PodCondition:
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions

    def __init__(
        self,
        last_transition_time: datetime,
        reason: str,
        message: str,
        status: bool | None,
        type: PodConditionType,
    ) -> None:
        self.transition_time = last_transition_time
        self.reason = reason
        self.message = message
        self.status = status
        self.type = type

    @classmethod
    def from_model(cls, model: V1PodCondition) -> Self:
        match model.status:
            case "Unknown":
                status = None
            case "True":
                status = True
            case "False":
                status = False
            case _:
                raise ValueError(f"Invalid status {model.status!r}")
        try:
            type = PodConditionType(model.type)
        except (KeyError, ValueError):
            type = PodConditionType.UNKNOWN
        assert model.last_transition_time is not None
        return cls(
            last_transition_time=model.last_transition_time,
            reason=model.reason or "",
            message=model.message or "",
            status=status,
            type=type,
        )


class KubernetesEvent:
    def __init__(
        self,
        involved_object: dict[str, str],
        count: int | None,
        reason: str | None,
        message: str | None,
        creation_timestamp: datetime | None,
        first_timestamp: datetime | None,
        event_time: datetime | None,
        last_timestamp: datetime | None,
    ) -> None:
        self.involved_object = involved_object
        self.creation_timestamp = creation_timestamp
        self.count = count
        self.reason = reason
        self.message = message
        self.first_timestamp = first_timestamp
        self.event_time = event_time
        self.last_timestamp = last_timestamp

    @classmethod
    def from_model(cls, event: CoreV1Event) -> Self:
        return cls(
            involved_object=event.involved_object.model_dump(),
            count=event.count,
            reason=event.reason,
            message=event.message,
            creation_timestamp=event.metadata.creation_timestamp,
            first_timestamp=event.first_timestamp,
            event_time=event.event_time,
            last_timestamp=event.last_timestamp,
        )

    @property
    def timestamp(self) -> datetime | None:
        return (
            self.last_timestamp
            or self.event_time
            or self.first_timestamp
            or self.creation_timestamp
        )


class PodStatus:
    def __init__(
        self,
        *,
        phase: str,
        container_statuses: tuple[ContainerStatus, ...],
        reason: str | None,
        conditions: list[PodCondition],
    ) -> None:
        self._phase = phase
        self._container_statuses = container_statuses
        self._reason = reason
        self._conditions = conditions

    @property
    def phase(self) -> str:
        """
        "Pending", "Running", "Succeeded", "Failed", "Unknown"
        """
        return self._phase

    @property
    def is_phase_pending(self) -> bool:
        return self.phase == "Pending"

    @property
    def is_phase_failed(self) -> bool:
        return self.phase == "Failed"

    @property
    def is_scheduled(self) -> bool:
        if not self.is_phase_pending:
            return True
        for cond in self.conditions:
            if cond.type == PodConditionType.POD_SCHEDULED:
                return bool(cond.status)
        return False

    @property
    def is_waiting(self) -> bool:
        return any(status.is_waiting for status in self._container_statuses)

    @property
    def is_terminated(self) -> bool:
        return all(status.is_terminated for status in self._container_statuses)

    @property
    def reason(self) -> str | None:
        """

        If kubelet decides to evict the pod, it sets the "Failed" phase along with
        the "Evicted" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/kubelet/eviction/eviction_manager.go#L543-L566
        If a node the pod scheduled on fails, node lifecycle controller sets
        the "NodeLost" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/controller/util/node/controller_utils.go#L109-L126
        """
        # the pod status reason has a greater priority
        return self._reason or (
            self.container_status.reason if self.container_status else None
        )

    @property
    def container_status(self) -> ContainerStatus:
        return self._container_statuses[0]

    @property
    def container_statuses(self) -> Sequence[ContainerStatus]:
        return self._container_statuses

    @property
    def is_node_lost(self) -> bool:
        return self.reason == "NodeLost"

    @property
    def conditions(self) -> list[PodCondition]:
        return list(self._conditions)

    @classmethod
    def from_model(cls, model: V1PodStatus) -> Self:
        if model.container_statuses:
            container_statuses = tuple(
                ContainerStatus.from_model(s) for s in model.container_statuses
            )
        else:
            container_statuses = (ContainerStatus(None),)
        return cls(
            phase=model.phase or "",
            container_statuses=container_statuses,
            reason=model.reason,
            conditions=[PodCondition.from_model(c) for c in model.conditions],
        )


@dataclass(frozen=True)
class NodeTaint:
    key: str
    value: str
    effect: str = "NoSchedule"

    def to_model(self) -> V1Taint:
        return V1Taint(
            effect=self.effect,
            key=self.key,
            value=self.value,
        )


@dataclass(frozen=True)
class NodeResources:
    cpu: float = 0
    memory: int = 0
    nvidia_gpu: int = 0
    nvidia_migs: Mapping[str, int] = field(default_factory=dict)
    amd_gpu: int = 0
    intel_gpu: int = 0

    nvidia_gpu_key: ClassVar[str] = "nvidia.com/gpu"
    nvidia_mig_key_prefix: ClassVar[str] = "nvidia.com/mig-"
    amd_gpu_key: ClassVar[str] = "amd.com/gpu"
    intel_gpu_key: ClassVar[str] = "gpu.intel.com/i915"

    def __post_init__(self) -> None:
        if self.cpu < 0:
            raise ValueError(f"Invalid cpu: {self.cpu}")
        if self.memory < 0:
            raise ValueError(f"Invalid memory: {self.memory}")
        if self.nvidia_gpu < 0:
            raise ValueError(f"Invalid nvidia gpu: {self.nvidia_gpu}")
        for k, v in self.nvidia_migs.items():
            if v < 0:
                raise ValueError(f"Invalid nvidia mig {k}: {v}")
        if self.amd_gpu < 0:
            raise ValueError(f"Invalid amd gpu:  {self.amd_gpu}")
        if self.intel_gpu < 0:
            raise ValueError(f"Invalid intel gpu:  {self.intel_gpu}")

    @classmethod
    def from_model(cls, payload: dict[str, str]) -> Self:
        return cls(
            cpu=Resources.parse_cpu(payload.get("cpu", "0")),
            memory=Resources.parse_memory(payload.get("memory", "0Mi")),
            nvidia_gpu=int(payload.get(cls.nvidia_gpu_key, 0)),
            nvidia_migs={
                k[len(cls.nvidia_mig_key_prefix) :]: int(v)
                for k, v in payload.items()
                if k.startswith(cls.nvidia_mig_key_prefix)
            },
            amd_gpu=int(payload.get(cls.amd_gpu_key, 0)),
            intel_gpu=int(payload.get(cls.intel_gpu_key, 0)),
        )

    @property
    def any(self) -> bool:
        return (
            self.cpu_mcores > 0
            or self.memory > 0
            or self.nvidia_gpu > 0
            or any(v > 0 for v in self.nvidia_migs.values())
            or self.amd_gpu > 0
            or self.intel_gpu > 0
        )

    @property
    def cpu_mcores(self) -> int:
        return int(self.cpu * 1000)

    def are_sufficient(self, pod: PodDescriptor) -> bool:
        r = pod.resources
        if not r:
            return True
        return (
            self.cpu_mcores >= r.cpu_mcores
            and self.memory >= r.memory
            and self.nvidia_gpu >= (r.nvidia_gpu or 0)
            and all(
                self.nvidia_migs.get(k, 0) >= v
                for k, v in (r.nvidia_migs or {}).items()
            )
            and self.amd_gpu >= (r.amd_gpu or 0)
            and self.intel_gpu >= (r.intel_gpu or 0)
        )

    def __add__(self, other: "NodeResources") -> "NodeResources":
        return self.__class__(
            cpu=self.cpu + other.cpu,
            memory=self.memory + other.memory,
            nvidia_gpu=self.nvidia_gpu + other.nvidia_gpu,
            nvidia_migs={
                k: self.nvidia_migs.get(k, 0) + other.nvidia_migs.get(k, 0)
                for k in set(self.nvidia_migs) | set(other.nvidia_migs)
            },
            amd_gpu=self.amd_gpu + other.amd_gpu,
            intel_gpu=self.intel_gpu + other.intel_gpu,
        )

    def __sub__(self, other: "NodeResources") -> "NodeResources":
        return self.__class__(
            cpu=self.cpu - other.cpu,
            memory=self.memory - other.memory,
            nvidia_gpu=self.nvidia_gpu - other.nvidia_gpu,
            nvidia_migs={
                k: self.nvidia_migs.get(k, 0) - other.nvidia_migs.get(k, 0)
                for k in set(self.nvidia_migs) | set(other.nvidia_migs)
            },
            amd_gpu=self.amd_gpu - other.amd_gpu,
            intel_gpu=self.intel_gpu - other.intel_gpu,
        )

    def __str__(self) -> str:
        return f"cpu={self.cpu_mcores}m, memory={self.memory}Mi, gpu={self.nvidia_gpu}"

    def __bool__(self) -> bool:
        return self.any


class NodeConditionType(enum.StrEnum):
    UNKNOWN = "Unknown"
    DISK_PRESSURE = "DiskPressure"
    MEMORY_PRESSURE = "MemoryPressure"
    NETWORK_UNAVAILABLE = "NetworkUnavailable"
    PID_PRESSURE = "PIDPressure"
    READY = "Ready"

    @classmethod
    def parse(cls, value: str) -> "NodeConditionType":
        try:
            return cls(value)
        except (KeyError, ValueError):
            return cls.UNKNOWN


@dataclass(frozen=True)
class NodeCondition:
    type: NodeConditionType
    status: bool | None
    transition_time: datetime | None
    message: str = ""
    reason: str = ""

    @classmethod
    def from_model(cls, model: V1NodeCondition) -> Self:
        return cls(
            type=NodeConditionType.parse(model.type),
            status=cls._parse_status(model.status),
            message=model.message or "",
            reason=model.reason or "",
            transition_time=model.last_transition_time,
        )

    @classmethod
    def _parse_status(cls, value: str) -> bool | None:
        if value == "Unknown":
            return None
        if value == "True":
            return True
        if value == "False":
            return False
        raise ValueError(f"Invalid status {value!r}")


@dataclass(frozen=True)
class NodeStatus:
    allocatable_resources: NodeResources
    conditions: list[NodeCondition] = field(default_factory=list)

    @classmethod
    def from_model(cls, model: V1NodeStatus) -> Self:
        return cls(
            allocatable_resources=NodeResources.from_model(model.allocatable),
            conditions=[NodeCondition.from_model(p) for p in model.conditions],
        )

    @property
    def is_ready(self) -> bool:
        for cond in self.conditions:
            if cond.type == NodeConditionType.READY:
                return bool(cond.status)
        return False


@dataclass(frozen=True)
class Node:
    name: str
    status: NodeStatus = field(compare=False)
    labels: dict[str, str] = field(default_factory=dict, compare=False)

    @classmethod
    def from_model(cls, model: V1Node) -> Self:
        metadata = model.metadata
        assert metadata.name is not None
        return cls(
            name=metadata.name,
            labels=metadata.labels,
            status=NodeStatus.from_model(model.status),
        )

    def get_free_resources(self, resource_requests: NodeResources) -> NodeResources:
        return self.status.allocatable_resources - resource_requests


class WatchEventType(enum.StrEnum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    ERROR = "ERROR"


class EventHandler[ModelT: ResourceModel]:
    @abc.abstractmethod
    async def init(self, resources: list[ModelT]) -> None:
        pass

    @abc.abstractmethod
    async def handle(self, event: WatchEvent[ModelT]) -> None:
        pass


class Watcher[ModelT: ResourceModel](abc.ABC):
    resource_version: str

    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._handlers: list[EventHandler[ModelT]] = []
        self._watcher_task: asyncio.Task[None] | None = None

    def subscribe(self, handler: EventHandler[ModelT]) -> None:
        if self._watcher_task is not None:
            raise Exception("Subscription is not possible after watcher start")
        self._handlers.append(handler)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def start(self) -> None:
        await self._init()
        self._watcher_task = asyncio.create_task(self._watch())

    async def stop(self) -> None:
        if self._watcher_task is None:
            return
        self._watcher_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._watcher_task
        self._watcher_task = None
        self._handlers.clear()

    async def _init(self) -> None:
        result = await self.list()
        for handler in self._handlers:
            await handler.init(result.items)

        assert result.metadata.resource_version is not None
        self.resource_version = result.metadata.resource_version

    async def _watch(self) -> None:
        watcher = await self.watch(self.resource_version)
        while True:
            try:
                async for event in watcher.stream():
                    for handler in self._handlers:
                        await handler.handle(event)
            except ResourceGone:
                logger.info("%s: resource gone", self.__class__.__name__)
                await self._init()
            except KubeClientUnauthorized:
                logger.info("%s: kube client unauthorized", self.__class__.__name__)
            except aiohttp.ClientError as exc:
                logger.warning(
                    "%s: watch client error", self.__class__.__name__, exc_info=exc
                )
            except Exception as exc:
                logger.warning(
                    "%s: unhandled error", self.__class__.__name__, exc_info=exc
                )
            finally:
                assert watcher.resource_version
                self.resource_version = watcher.resource_version

    @abc.abstractmethod
    async def list(self) -> CollectionModel[ModelT]:
        pass

    @abc.abstractmethod
    async def watch(self, resource_version: str) -> Watch[ModelT]:
        pass


class NodeWatcher(Watcher[V1Node]):
    def __init__(
        self, kube_client: KubeClient, labels: dict[str, str] | None = None
    ) -> None:
        super().__init__(kube_client)
        self._labels = (
            ",".join(f"{k}={v}" for k, v in labels.items()) if labels else None
        )

    async def list(self) -> CollectionModel[V1Node]:
        return await self._kube_client.core_v1.node.get_list(
            label_selector=self._labels,
        )

    async def watch(self, resource_version: str) -> Watch[V1Node]:
        return self._kube_client.core_v1.node.watch(
            resource_version=resource_version,
            label_selector=self._labels,
            allow_watch_bookmarks=True,
        )


class PodWatcher(Watcher[V1Pod]):
    def __init__(self, kube_client: KubeClient, all_namespaces: bool = True) -> None:
        super().__init__(kube_client)
        self._all_namespaces = all_namespaces

    async def list(self) -> CollectionModel[V1Pod]:
        return await self._kube_client.core_v1.pod.get_list(
            all_namespaces=self._all_namespaces,
        )

    async def watch(self, resource_version: str) -> Watch[V1Pod]:
        return self._kube_client.core_v1.pod.watch(
            resource_version=resource_version,
            all_namespaces=self._all_namespaces,
            allow_watch_bookmarks=True,
        )


# https://github.com/kubernetes/kubernetes/blob/c285e781331a3785a7f436042c65c5641ce8a9e9/pkg/kubelet/preemption/preemption.go
class KubePreemption:
    @classmethod
    def get_pods_to_preempt(
        cls, resources: NodeResources, pods: list[PodDescriptor]
    ) -> list[PodDescriptor]:
        if resources.nvidia_gpu:
            pods = [p for p in pods if p.resources and p.resources.nvidia_gpu]
        if resources.amd_gpu:
            pods = [p for p in pods if p.resources and p.resources.amd_gpu]
        if resources.intel_gpu:
            pods = [p for p in pods if p.resources and p.resources.intel_gpu]
        pods_to_preempt: list[PodDescriptor] = []
        while pods and resources.any:
            logger.debug("Pods left: %d", len(pods))
            logger.debug("Resources left: %s", resources)
            # max distance for a single resource is 1, 4 resources total
            best_dist = 5.0
            best_resources: Resources = pods[0].resources  # type: ignore
            best_pod_index = 0
            for i, pod in enumerate(pods):
                assert pod.resources
                dist = cls._distance(resources, pod)
                # Select min distance. If distances are equal select pod with min
                # resources.
                if dist < best_dist or (
                    dist == best_dist
                    and cls._has_less_resources(pod.resources, best_resources)
                ):
                    logger.debug(
                        "New best pod selected: name=%s, dist=%f", pods[i].name, dist
                    )
                    best_dist = dist
                    best_resources = pod.resources
                    best_pod_index = i
            resources = cls._subtract_resources(resources, best_resources)
            pods_to_preempt.append(pods[best_pod_index])
            del pods[best_pod_index]
        logger.debug(
            "Resources left: cpu=%fm, memory=%dMi, gpu=%d",
            resources.cpu_mcores,
            resources.memory,
            resources.nvidia_gpu or 0,
        )
        if resources.any:
            logger.debug("Pods to preempt: []")
            return []
        logger.debug("Pods to preempt: %s", [p.name for p in pods_to_preempt])
        return pods_to_preempt

    @classmethod
    def _subtract_resources(cls, r1: NodeResources, r2: Resources) -> NodeResources:
        return replace(
            r1,
            cpu=max(0, (r1.cpu_mcores - r2.cpu_mcores) / 1000),
            memory=max(0, r1.memory - r2.memory),
            nvidia_gpu=max(0, (r1.nvidia_gpu or 0) - (r2.nvidia_gpu or 0)),
            amd_gpu=max(0, (r1.amd_gpu or 0) - (r2.amd_gpu or 0)),
            intel_gpu=max(0, (r1.intel_gpu or 0) - (r2.intel_gpu or 0)),
        )

    @classmethod
    def _distance(cls, resources: NodeResources, pod: PodDescriptor) -> float:
        def _dist(resource: int, pod_resource: int) -> float:
            try:
                return (max(0, resource - pod_resource) / resource) ** 2
            except ZeroDivisionError:
                return 0

        assert pod.resources
        dist = 0.0
        dist += _dist(resources.cpu_mcores, pod.resources.cpu_mcores)
        dist += _dist(resources.memory, pod.resources.memory)
        if resources.nvidia_gpu:
            dist += _dist(resources.nvidia_gpu or 0, pod.resources.nvidia_gpu or 0)
        if resources.amd_gpu:
            dist += _dist(resources.amd_gpu or 0, pod.resources.amd_gpu or 0)
        if resources.intel_gpu:
            dist += _dist(resources.intel_gpu or 0, pod.resources.intel_gpu or 0)
        return dist

    @classmethod
    def _has_less_resources(cls, r1: Resources, r2: Resources) -> bool:
        key1 = (
            (r1.nvidia_gpu or 0) + (r1.amd_gpu or 0) + (r1.intel_gpu or 0),
            r1.memory,
            r1.cpu_mcores,
        )
        key2 = (
            (r2.nvidia_gpu or 0) + (r2.amd_gpu or 0) + (r2.intel_gpu or 0),
            r2.memory,
            r2.cpu_mcores,
        )
        return key1 < key2


# New API helpers
async def wrap_job_error(coro: Coroutine[Any, Any, V1Pod]) -> V1Pod:
    try:
        return await coro
    except ApoloResourceNotFound:
        raise JobNotFoundException("job was not found")
    except ApoloResourceExists:
        raise JobAlreadyExistsException("job already exists")
    except ResourceInvalid:
        raise JobError("cant create job")
    except KubeClientException:
        raise JobError("unexpected error")


async def get_pod(client_proxy: KubeClientProxy, pod_name: str) -> PodDescriptor:
    pod = await wrap_job_error(client_proxy.core_v1.pod.get(pod_name))
    return PodDescriptor.from_model(pod)


async def get_pod_status(client_proxy: KubeClientProxy, pod_name: str) -> PodStatus:
    pod = await get_pod(client_proxy, pod_name)
    if pod.status is None:
        raise ValueError("Missing pod status")
    return pod.status


async def create_pod(
    client_proxy: KubeClientProxy, descriptor: PodDescriptor
) -> PodDescriptor:
    model = descriptor.to_model()
    pod = await wrap_job_error(client_proxy.core_v1.pod.create(model))
    return PodDescriptor.from_model(pod)


async def delete_pod(
    client_proxy: KubeClientProxy, pod_name: str, *, force: bool = False
) -> PodStatus:
    payload: dict[str, Any] | None = None
    if force:
        payload = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 0,
        }
    model = await wrap_job_error(
        client_proxy.core_v1.pod.delete(pod_name, payload=payload)
    )
    pod = PodDescriptor.from_model(model)
    return pod.status  # type: ignore


async def create_service(client_proxy: KubeClientProxy, service: Service) -> Service:
    model = service.to_model()
    created = await client_proxy.core_v1.service.create(model)
    return Service.from_model(created)


async def get_service(client_proxy: KubeClientProxy, name: str) -> Service:
    service = await client_proxy.core_v1.service.get(name)
    return Service.from_model(service)


async def list_services(
    client_proxy: KubeClientProxy, labels: dict[str, str] | None = None
) -> list[Service]:
    label_selector = None
    if labels:
        label_selector = ",".join([f"{k}={v}" for k, v in labels.items()])
    svc_list = await client_proxy.core_v1.service.get_list(
        label_selector=label_selector
    )
    return [Service.from_model(item) for item in (svc_list.items or [])]


async def delete_service(
    client_proxy: KubeClientProxy,
    name: str,
    *,
    uid: str | None = None,
) -> None:
    payload: dict[str, Any] | None = None
    if uid:
        payload = {"preconditions": {"uid": uid}}
    await client_proxy.core_v1.service.delete(name, payload=payload)


async def get_ingress(client_proxy: KubeClientProxy, ingress_name: str) -> Ingress:
    ingress = await client_proxy.networking_k8s_io_v1.ingress.get(name=ingress_name)
    return Ingress.from_model(ingress)


async def create_ingress(client_proxy: KubeClientProxy, ingress: Ingress) -> Ingress:
    model = ingress.to_model()
    ingress_out = await client_proxy.networking_k8s_io_v1.ingress.create(model)
    return Ingress.from_model(ingress_out)


async def delete_ingress(client_proxy: KubeClientProxy, name: str) -> None:
    await client_proxy.networking_k8s_io_v1.ingress.delete(name)


async def delete_all_ingresses(
    client_proxy: KubeClientProxy, labels: dict[str, str]
) -> None:
    label_selector = ",".join([f"{k}={v}" for k, v in labels.items()])
    lst = await client_proxy.networking_k8s_io_v1.ingress.get_list(
        label_selector=label_selector
    )
    for item in lst.items or []:
        assert item.metadata.name is not None
        await client_proxy.networking_k8s_io_v1.ingress.delete(item.metadata.name)


async def update_docker_secret(
    client_proxy: KubeClientProxy, secret: DockerRegistrySecret
) -> None:
    model = secret.to_model()
    await client_proxy.core_v1.secret.create_or_update(model)


async def get_raw_secret(
    client_proxy: KubeClientProxy, secret_name: str
) -> dict[str, Any]:
    secret = await client_proxy.core_v1.secret.get(secret_name)
    return secret.model_dump()


async def get_pod_events(
    client_proxy: KubeClientProxy,
    pod_id: str,
) -> list[KubernetesEvent]:
    events = await client_proxy.core_v1.event.get_list(
        field_selector=(
            "involvedObject.kind=Pod"
            f",involvedObject.namespace={client_proxy._namespace}"
            f",involvedObject.name={pod_id}"
        )
    )
    return [KubernetesEvent.from_model(event) for event in events.items]


async def wait_pod_is_running(
    client_proxy: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    async with timeout(timeout_s):
        while True:
            pod_status = await get_pod_status(client_proxy, pod_name)
            if not pod_status.is_waiting:
                return
            await asyncio.sleep(interval_s)


async def wait_pod_is_terminated(
    client_proxy: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    async with timeout(timeout_s):
        while True:
            pod_status = await get_pod_status(client_proxy, pod_name)
            if pod_status.is_terminated:
                return
            await asyncio.sleep(interval_s)


async def wait_pod_is_deleted(
    client_proxy: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    async with timeout(timeout_s):
        while True:
            try:
                await get_pod(client_proxy, pod_name)
                await asyncio.sleep(interval_s)
            except JobNotFoundException:
                return


async def wait_pod_is_finished(
    client_proxy: KubeClientProxy,
    pod_name: str,
    timeout_s: float = 10.0 * 60,
    interval_s: float = 1.0,
) -> None:
    async with timeout(timeout_s):
        while True:
            pod_status = await get_pod_status(client_proxy, pod_name)
            if pod_status.phase in ("Succeeded", "Failed"):
                return
            await asyncio.sleep(interval_s)
