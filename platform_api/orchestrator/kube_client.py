import abc
import asyncio
import enum
import json
import logging
from asyncio import timeout
from base64 import b64encode
from collections.abc import (
    AsyncIterator,
    Callable,
    Coroutine,
    Mapping,
    Sequence,
)
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from pathlib import PurePath
from typing import Any, ClassVar, NoReturn, Optional, Self

import aiohttp
import iso8601
from apolo_kube_client import (
    KubeClientProxy,
    ResourceExists as ApoloResourceExists,
    ResourceInvalid,
    ResourceNotFound as ApoloResourceNotFound,
)
from kubernetes.client import CoreV1Event, V1Taint
from kubernetes.client.models import (
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
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
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
    V1Toleration,
    V1Volume,
    V1VolumeMount,
    V1WeightedPodAffinityTerm,
)
from yarl import URL

from platform_api.old_kube_client.client import KubeClient as ApoloKubeClient
from platform_api.old_kube_client.errors import (
    KubeClientException,
    KubeClientUnauthorized,
    ResourceExists,
    ResourceGone,
    ResourceNotFound,
)

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
from .kube_config import KubeClientAuthType

logger = logging.getLogger(__name__)


class ServiceType(str, enum.Enum):
    CLUSTER_IP = "ClusterIP"
    NODE_PORT = "NodePort"
    LOAD_BALANCER = "LoadBalancer"


def _raise_status_job_exception(pod: dict[str, Any], job_id: str | None) -> NoReturn:
    if pod["code"] == 409:
        raise ResourceExists(pod.get("reason", "job already exists"))
    if pod["code"] == 404:
        raise JobNotFoundException(f"job {job_id} was not found")
    if pod["code"] == 422:
        raise JobError(f"cant create job with id {job_id}")
    raise JobError(f"unexpected payload: {pod}")


class GroupVersion(str, Enum):
    NETWORKING_V1 = "networking.k8s.io/v1"


@dataclass(frozen=True)
class APIResource:
    group_version: str
    resources: Sequence[str]

    @property
    def has_ingress(self) -> bool:
        return self.has_resource("ingresses")

    def has_resource(self, resource_name: str) -> bool:
        return resource_name in self.resources

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "APIResource":
        return cls(
            group_version=payload["groupVersion"],
            resources=[p["name"] for p in payload["resources"]],
        )


class APIResources(dict[str, APIResource]):
    group_versions: list[str] = [GroupVersion.NETWORKING_V1.value]

    @property
    def networking_v1(self) -> APIResource | None:
        return self.get(GroupVersion.NETWORKING_V1)

    @property
    def has_networking_v1_ingress(self) -> bool:
        return self.networking_v1 is not None and self.networking_v1.has_ingress


@dataclass(frozen=True)
class Volume(metaclass=abc.ABCMeta):
    name: str

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: PurePath | None = None,
    ) -> "VolumeMount":
        raise NotImplementedError("Cannot create mount for abstract Volume type.")

    def to_primitive(self) -> dict[str, Any]:
        raise NotImplementedError

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
    def to_primitive(self) -> dict[str, Any]:
        return {"name": self.name, "emptyDir": {"medium": "Memory"}}

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "valueFrom": {
                "secretKeyRef": {
                    "name": self.secret.k8s_secret_name,
                    "key": self.secret.secret_key,
                }
            },
        }

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

    def to_primitive(self) -> dict[str, Any]:
        sub_path = str(self.sub_path)
        raw = {
            "name": self.volume.name,
            "mountPath": str(self.mount_path),
            "readOnly": self.read_only,
        }
        if sub_path:
            raw["subPath"] = sub_path
        return raw

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "secret": {"secretName": self.k8s_secret_name, "defaultMode": 0o400},
        }

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "persistentVolumeClaim": {"claimName": self.claim_name},
        }

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

    def to_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "requests": {"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
            "limits": {"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
        }
        if self.nvidia_gpu:
            payload["requests"][self.nvidia_gpu_key] = self.nvidia_gpu
            payload["limits"][self.nvidia_gpu_key] = self.nvidia_gpu
        if self.nvidia_migs:
            for key, value in self.nvidia_migs.items():
                payload["requests"][self.nvidia_mig_key_prefix + key] = value
                payload["limits"][self.nvidia_mig_key_prefix + key] = value
        if self.amd_gpu:
            payload["requests"][self.amd_gpu_key] = self.amd_gpu
            payload["limits"][self.amd_gpu_key] = self.amd_gpu
        if self.intel_gpu:
            payload["requests"][self.intel_gpu_key] = self.intel_gpu
            payload["limits"][self.intel_gpu_key] = self.intel_gpu
        if self.tpu_version:
            payload["requests"][self.tpu_key] = self.tpu_cores
            payload["limits"][self.tpu_key] = self.tpu_cores
        if self.memory_request:
            payload["requests"]["memory"] = self.memory_request_str
        return payload

    def to_model(self) -> V1ResourceRequirements:
        ret = V1ResourceRequirements(
            requests={"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
            limits={"cpu": f"{self.cpu_mcores}m", "memory": self.memory_str},
        )
        if self.nvidia_gpu:
            ret.requests[self.nvidia_gpu_key] = self.nvidia_gpu
            ret.limits[self.nvidia_gpu_key] = self.nvidia_gpu
        if self.nvidia_migs:
            for key, value in self.nvidia_migs.items():
                ret.requests[self.nvidia_mig_key_prefix + key] = value
                ret.limits[self.nvidia_mig_key_prefix + key] = value
        if self.amd_gpu:
            ret.requests[self.amd_gpu_key] = self.amd_gpu
            ret.limits[self.amd_gpu_key] = self.amd_gpu
        if self.intel_gpu:
            ret.requests[self.intel_gpu_key] = self.intel_gpu
            ret.limits[self.intel_gpu_key] = self.intel_gpu
        if self.tpu_version:
            ret.requests[self.tpu_key] = self.tpu_cores
            ret.limits[self.tpu_key] = self.tpu_cores
        if self.memory_request:
            ret.requests["memory"] = self.memory_request_str
        return ret

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "Resources":
        requests = payload.get("requests", {})
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
    namespace: str
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

    def to_primitive(self) -> dict[str, Any]:
        service_descriptor: dict[str, Any] = {
            "metadata": {
                "namespace": self.namespace,
                "name": self.name,
            },
            "spec": {
                "type": self.service_type.value,
                "ports": [],
                "selector": self.selector,
            },
        }

        if self.cluster_ip:
            service_descriptor["spec"]["clusterIP"] = self.cluster_ip
        if self.labels:
            service_descriptor["metadata"]["labels"] = self.labels.copy()

        self._add_port_map(
            self.port, self.target_port, "http", service_descriptor["spec"]["ports"]
        )
        return service_descriptor

    def to_model(self) -> V1Service:
        ports: list[V1ServicePort] = []
        if self.target_port:
            ports.append(
                V1ServicePort(name="http", port=self.port, target_port=self.target_port)
            )
        spec = V1ServiceSpec(
            type=self.service_type.value,
            selector=self.selector or None,
            ports=ports or None,
        )
        if self.cluster_ip is not None:
            spec.cluster_ip = self.cluster_ip
        metadata = V1ObjectMeta(name=self.name, namespace=self.namespace)
        if self.labels:
            metadata.labels = self.labels.copy()
        return V1Service(metadata=metadata, spec=spec)

    @classmethod
    def create_for_pod(cls, namespace: str, pod: "PodDescriptor") -> "Service":
        return cls(
            namespace=namespace,
            name=pod.name,
            selector=pod.labels,
            target_port=pod.port,
            labels=pod.labels,
        )

    @classmethod
    def create_headless_for_pod(cls, namespace: str, pod: "PodDescriptor") -> "Service":
        http_port = pod.port or cls.port
        return cls(
            namespace=namespace,
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
    def from_primitive(cls, payload: dict[str, Any]) -> "Service":
        http_payload = cls._find_port_by_name("http", payload["spec"]["ports"])
        service_type = payload["spec"].get("type", Service.service_type.value)
        return cls(
            namespace=payload["metadata"]["namespace"],
            name=payload["metadata"]["name"],
            uid=payload["metadata"]["uid"],
            selector=payload["spec"].get("selector", {}),
            target_port=http_payload.get("targetPort", None),
            port=http_payload.get("port", Service.port),
            service_type=ServiceType(service_type),
            cluster_ip=payload["spec"].get("clusterIP"),
            labels=payload["metadata"].get("labels", {}),
        )

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
            namespace=(model.metadata.namespace if model.metadata else "default"),
            name=(model.metadata.name if model.metadata else ""),
            uid=(model.metadata.uid if model.metadata else None),
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
    def from_v1beta1_primitive(cls, payload: dict[str, Any]) -> "IngressRule":
        http_paths = payload.get("http", {}).get("paths", [])
        http_path = http_paths[0] if http_paths else {}
        backend = http_path.get("backend", {})
        service_name = backend.get("serviceName")
        service_port = backend.get("servicePort")
        return cls(
            host=payload.get("host", ""),
            service_name=service_name,
            service_port=service_port,
        )

    @classmethod
    def from_v1_primitive(cls, payload: dict[str, Any]) -> "IngressRule":
        http_paths = payload.get("http", {}).get("paths", [])
        http_path = http_paths[0] if http_paths else {}
        service = http_path.get("backend", {}).get("service", {})
        service_name = service.get("name")
        service_port = service.get("port", {}).get("number")
        return cls(
            host=payload.get("host", ""),
            service_name=service_name,
            service_port=service_port,
        )

    def to_v1beta1_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"host": self.host}
        if self.service_name:
            payload["http"] = {
                "paths": [
                    {
                        "backend": {
                            "serviceName": self.service_name,
                            "servicePort": self.service_port,
                        }
                    }
                ]
            }
        return payload

    def to_v1_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"host": self.host}
        if self.service_name:
            payload["http"] = {
                "paths": [
                    {
                        "pathType": "ImplementationSpecific",
                        "backend": {
                            "service": {
                                "name": self.service_name,
                                "port": {"number": self.service_port},
                            }
                        },
                    }
                ]
            }
        return payload

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

    def to_v1beta1_primitive(self) -> dict[str, Any]:
        rules: list[Any] = [rule.to_v1beta1_primitive() for rule in self.rules] or [
            None
        ]
        annotations = self.annotations.copy()
        if self.ingress_class:
            annotations["kubernetes.io/ingress.class"] = self.ingress_class
        metadata = {"name": self.name, "annotations": annotations}
        if self.labels:
            metadata["labels"] = self.labels.copy()
        return {"metadata": metadata, "spec": {"rules": rules}}

    def to_v1_primitive(self) -> dict[str, Any]:
        rules: list[Any] = [rule.to_v1_primitive() for rule in self.rules] or [None]
        annotations = self.annotations.copy()
        metadata = {"name": self.name, "annotations": annotations}
        spec: dict[str, Any] = {"rules": rules}
        primitive: dict[str, Any] = {"metadata": metadata, "spec": spec}
        if self.ingress_class:
            annotations.pop(
                "kubernetes.io/ingress.class", None
            )  # deprecated and has conflict with ingressClassName
            spec["ingressClassName"] = self.ingress_class
        if self.labels:
            metadata["labels"] = self.labels.copy()
        return primitive

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
        spec.rules = [rule.to_model() for rule in self.rules] or None
        return V1Ingress(metadata=metadata, spec=spec)

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "Ingress":
        if payload["apiVersion"] == GroupVersion.NETWORKING_V1:
            return cls._from_v1_primitive(payload)
        return cls._from_v1beta1_primitive(payload)

    @classmethod
    def _from_v1beta1_primitive(cls, payload: dict[str, Any]) -> "Ingress":
        metadata = payload["metadata"]
        spec = payload["spec"]
        annotations = metadata.get("annotations", {})
        rules = [IngressRule.from_v1beta1_primitive(rule) for rule in spec["rules"]]
        return cls(
            name=metadata["name"],
            ingress_class=annotations.get("kubernetes.io/ingress.class"),
            rules=rules,
            annotations=metadata.get("annotations", {}),
            labels=metadata.get("labels", {}),
        )

    @classmethod
    def _from_v1_primitive(cls, payload: dict[str, Any]) -> "Ingress":
        metadata = payload["metadata"]
        spec = payload["spec"]
        annotations = metadata.get("annotations", {})
        rules = [IngressRule.from_v1_primitive(rule) for rule in spec["rules"]]
        return cls(
            name=metadata["name"],
            ingress_class=spec.get("ingressClassName")
            or annotations.get(
                "kubernetes.io/ingress.class"
            ),  # for backward compatibility with old ingresses
            rules=rules,
            annotations=annotations,
            labels=metadata.get("labels", {}),
        )

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
    namespace: str

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.name, "namespace": self.namespace},
            "data": {".dockerconfigjson": self._build_json()},
            "type": self.type,
        }

    def to_model(self) -> V1Secret:
        metadata = V1ObjectMeta(name=self.name, namespace=self.namespace)
        secret = V1Secret(metadata=metadata, type=self.type)
        # The client library expects .data as dict[str, str] base64 values
        secret.data = {".dockerconfigjson": self._build_json()}
        return secret


@dataclass(frozen=True)
class SecretRef:
    name: str

    def to_primitive(self) -> dict[str, str]:
        return {"name": self.name}

    def to_model(self) -> V1LocalObjectReference:
        return V1LocalObjectReference(name=self.name)

    @classmethod
    def from_primitive(cls, payload: dict[str, str]) -> "SecretRef":
        return cls(**payload)

    @classmethod
    def from_model(cls, model: V1LocalObjectReference) -> "SecretRef":
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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "operator": self.operator,
            "value": self.value,
            "effect": self.effect,
        }

    def to_model(self) -> V1Toleration:
        return V1Toleration(
            key=self.key,
            operator=self.operator,
            value=self.value,
            effect=self.effect,
        )


class SelectorOperator(str, Enum):
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
    def create_in(cls, key: str, *values: str) -> "LabelSelectorMatchExpression":
        return cls(key=key, operator=SelectorOperator.IN, values=[*values])

    @classmethod
    def create_exists(cls, key: str) -> "LabelSelectorMatchExpression":
        return cls(key=key, operator=SelectorOperator.EXISTS)

    @classmethod
    def create_does_not_exist(cls, key: str) -> "LabelSelectorMatchExpression":
        return cls(key=key, operator=SelectorOperator.DOES_NOT_EXIST)

    @classmethod
    def create_gt(cls, key: str, value: int) -> "LabelSelectorMatchExpression":
        return cls(key=key, operator=SelectorOperator.GT, values=[str(value)])

    @classmethod
    def create_lt(cls, key: str, value: int) -> "LabelSelectorMatchExpression":
        return cls(key=key, operator=SelectorOperator.LT, values=[str(value)])

    def is_satisfied(self, node_labels: dict[str, str]) -> bool:
        label_value = node_labels.get(self.key)
        return self.operator.apply(label_value, self.values)

    def to_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"key": self.key, "operator": self.operator.value}
        if self.values:
            payload["values"] = self.values.copy()
        return payload

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "matchExpressions": [expr.to_primitive() for expr in self.match_expressions]
        }

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

    def to_primitive(self) -> dict[str, Any]:
        return {"preference": self.preference.to_primitive(), "weight": self.weight}

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

    def to_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.required:
            payload["requiredDuringSchedulingIgnoredDuringExecution"] = {
                "nodeSelectorTerms": [term.to_primitive() for term in self.required]
            }
        if self.preferred:
            payload["preferredDuringSchedulingIgnoredDuringExecution"] = [
                term.to_primitive() for term in self.preferred
            ]
        return payload

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

    def to_primitive(self) -> dict[str, Any]:
        result = {
            "labelSelector": self.label_selector.to_primitive(),
            "topologyKey": self.topologyKey,
        }
        if self.namespaces:
            result["namespaces"] = self.namespaces.copy()
        return result

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

    def to_primitive(self) -> dict[str, Any]:
        return {
            "podAffinityTerm": self.pod_affinity_term.to_primitive(),
            "weight": self.weight,
        }

    def to_model(self) -> V1WeightedPodAffinityTerm:
        return V1WeightedPodAffinityTerm(
            pod_affinity_term=self.pod_affinity_term.to_model(),
            weight=self.weight,
        )


@dataclass(frozen=True)
class PodAffinity:
    preferred: list[PodPreferredSchedulingTerm] = field(default_factory=list)

    def to_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.preferred:
            payload["preferredDuringSchedulingIgnoredDuringExecution"] = [
                term.to_primitive() for term in self.preferred
            ]
        return payload

    def to_model(self) -> V1PodAffinity:
        ret = V1PodAffinity()
        if self.preferred:
            ret.preferred_during_scheduling_ignored_during_execution = [
                term.to_model() for term in self.preferred
            ]
        return ret


@enum.unique
class PodRestartPolicy(str, enum.Enum):
    ALWAYS = "Always"
    ON_FAILURE = "OnFailure"
    NEVER = "Never"

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.__str__().__repr__()


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
            priority_class_name=priority_class_name,
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
        env_list = self.env_list + [env.to_model() for env in self.secret_env_list]

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
        if self.labels:
            model.metadata.labels = self.labels
        if self.annotations:
            model.metadata.annotations = self.annotations.copy()
        if self.node_selector:
            model.spec.nodeSelector = self.node_selector.copy()
        if self.node_affinity or self.pod_affinity:
            model.spec.affinity = V1Affinity()
        if self.node_affinity:
            model.spec.affinity.node_affinity = self.node_affinity.to_model()
        if self.pod_affinity:
            model.spec.affinity.pod_affinity = self.pod_affinity.to_model()
        if self.priority_class_name:
            model.spec.priority_class_name = self.priority_class_name
        return model

    def to_primitive(self) -> dict[str, Any]:
        volume_mounts = [mount.to_primitive() for mount in self.volume_mounts]
        volumes = [volume.to_primitive() for volume in self.volumes]
        env_list = self.env_list + [env.to_primitive() for env in self.secret_env_list]

        container_payload: dict[str, Any] = {
            "name": f"{self.name}",
            "image": f"{self.image}",
            "imagePullPolicy": "Always",
            "env": env_list,
            "volumeMounts": volume_mounts,
            "terminationMessagePolicy": "FallbackToLogsOnError",
        }
        if self.command:
            container_payload["command"] = self.command
        if self.args:
            container_payload["args"] = self.args
        if self.resources:
            container_payload["resources"] = self.resources.to_primitive()
        if self.tty:
            container_payload["tty"] = True
        container_payload["stdin"] = True
        if self.working_dir is not None:
            container_payload["workingDir"] = self.working_dir
        if self.privileged:
            container_payload["securityContext"] = {
                "privileged": self.privileged,
            }

        ports = self._to_primitive_ports()
        if ports:
            container_payload["ports"] = ports
        readiness_probe = self._to_primitive_readiness_probe()
        if readiness_probe:
            container_payload["readinessProbe"] = readiness_probe

        payload: dict[str, Any] = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": self.name},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [container_payload],
                "volumes": volumes,
                "restartPolicy": str(self.restart_policy),
                "imagePullSecrets": [
                    secret.to_primitive() for secret in self.image_pull_secrets
                ],
                "tolerations": [
                    toleration.to_primitive() for toleration in self.tolerations
                ],
            },
        }
        if self.labels:
            payload["metadata"]["labels"] = self.labels
        if self.annotations:
            payload["metadata"]["annotations"] = self.annotations.copy()
        if self.node_selector:
            payload["spec"]["nodeSelector"] = self.node_selector.copy()
        if self.node_affinity or self.pod_affinity:
            payload["spec"]["affinity"] = {}
        if self.node_affinity:
            # fmt: off
            payload["spec"]["affinity"]["nodeAffinity"] \
                = self.node_affinity.to_primitive()
            # fmt: on
        if self.pod_affinity:
            # fmt: off
            payload["spec"]["affinity"]["podAffinity"] \
                = self.pod_affinity.to_primitive()
            # fmt: on
        return payload

    def _to_primitive_ports(self) -> list[dict[str, int]]:
        ports = []
        if self.port:
            ports.append({"containerPort": self.port})
        return ports

    def _to_model_ports(self) -> list[V1ContainerPort]:
        ports = []
        if self.port:
            ports.append(V1ContainerPort(container_port=self.port))
        return ports

    def _to_primitive_readiness_probe(self) -> dict[str, Any]:
        if not self.readiness_probe:
            return {}

        if self.port:
            return {
                "httpGet": {"port": self.port, "path": self.health_check_path},
                "initialDelaySeconds": 1,
                "periodSeconds": 1,
            }

        return {}

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
    def from_primitive(cls, payload: dict[str, Any]) -> "PodDescriptor":
        cls._assert_resource_kind(expected_kind="Pod", payload=payload)

        metadata = payload["metadata"]
        container_payload = payload["spec"]["containers"][0]
        # TODO (R Zubairov 09/13/18): remove medium emptyDir
        # TODO (A Danshyn 06/19/18): set rest of attributes
        status = None
        if "status" in payload:
            status = PodStatus.from_primitive(payload["status"])
        if "imagePullSecrets" in payload["spec"]:
            secrets = [
                SecretRef.from_primitive(secret)
                for secret in payload["spec"]["imagePullSecrets"]
            ]
        else:
            secrets = []
        tolerations = [
            Toleration(
                key=t.get("key", ""),
                operator=t.get("operator", Toleration.operator),
                value=t.get("value", Toleration.value),
                effect=t.get("effect", Toleration.effect),
            )
            for t in payload["spec"].get("tolerations", ())
        ]
        return cls(
            name=metadata["name"],
            created_at=iso8601.parse_date(metadata["creationTimestamp"]),
            image=container_payload["image"],
            status=status,
            image_pull_secrets=secrets,
            node_name=payload["spec"].get("nodeName"),
            command=container_payload.get("command"),
            args=container_payload.get("args"),
            tty=container_payload.get("tty", False),
            tolerations=tolerations,
            labels=metadata.get("labels", {}),
            restart_policy=PodRestartPolicy(
                payload["spec"].get("restartPolicy", str(cls.restart_policy))
            ),
            working_dir=container_payload.get("workingDir"),
            resources=Resources.from_primitive(container_payload.get("resources", {})),
        )

    @classmethod
    def from_model(cls, model: V1Pod) -> "PodDescriptor":
        metadata = model.metadata
        container = model.spec.containers[0]
        # TODO (R Zubairov 09/13/18): remove medium emptyDir
        # TODO (A Danshyn 06/19/18): set rest of attributes
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
            resources=Resources.from_model(container.resources or {}),
        )


@dataclass(frozen=True)
class ContainerStateRunning:
    started_at: datetime


class ContainerStatus:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}

    @property
    def _state(self) -> dict[str, Any]:
        return self._payload.get("state", {})

    @property
    def is_waiting(self) -> bool:
        return not self._state or (
            "waiting" in self._state and self._state["waiting"] is not None
        )

    @property
    def is_terminated(self) -> bool:
        return (
            bool(self._state)
            and "terminated" in self._state
            and self._state["terminated"] is not None
        )

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
        for state in self._state.values():
            if not state:
                continue
            return state.get("reason")
        return None

    @property
    def message(self) -> str | None:
        for state in self._state.values():
            if not state:
                continue
            return state.get("message")
        return None

    @property
    def exit_code(self) -> int | None:
        assert self.is_terminated
        termination_state = self._state["terminated"]
        for key in ("exitCode", "exit_code"):
            if key in termination_state:
                return termination_state[key]
        return None

    @property
    def is_creating(self) -> bool:
        # TODO (A Danshyn 07/20/18): handle PodInitializing
        # TODO (A Danshyn 07/20/18): consider handling other reasons
        # https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43
        return self.is_waiting and self.reason in (None, "ContainerCreating")

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> Self:
        return cls(payload)

    @classmethod
    def from_model(cls, model: V1ContainerStatus) -> Self:
        return cls(model.to_dict())


class PodConditionType(enum.Enum):
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
    def from_primitive(cls, payload: dict[str, Any]) -> Self:
        if "lastTransitionTime" in payload:
            last_transition_time = iso8601.parse_date(payload["lastTransitionTime"])
        else:
            last_transition_time = payload["last_transition_time"]

        raw_status = payload["status"]

        match raw_status:
            case "Unknown":
                status = None
            case "True":
                status = True
            case "False":
                status = False
            case _:
                raise ValueError(f"Invalid status {raw_status!r}")

        try:
            type = PodConditionType(payload["type"])
        except (KeyError, ValueError):
            type = PodConditionType.UNKNOWN

        return cls(
            last_transition_time=last_transition_time,
            reason=payload.get("reason", ""),
            message=payload.get("message", ""),
            status=status,
            type=type,
        )

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
        count: int,
        reason: str | None,
        message: str | None,
        creation_timestamp: datetime,
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
    def from_primitive(cls, payload: dict[str, Any]) -> Self:
        first_timestamp = last_timestamp = event_time = None

        raw_first_timestamp = payload.get("firstTimestamp") or payload.get(
            "deprecatedFirstTimestamp"
        )
        if raw_first_timestamp:
            first_timestamp = iso8601.parse_date(raw_first_timestamp)

        raw_last_timestamp = payload.get("lastTimestamp") or payload.get(
            "deprecatedLastTimestamp"
        )
        if raw_last_timestamp:
            last_timestamp = iso8601.parse_date(raw_last_timestamp)

        raw_event_time = payload.get("eventTime")
        if raw_event_time:
            event_time = iso8601.parse_date(raw_event_time)

        return cls(
            involved_object=payload["involvedObject"],
            count=payload["count"],
            reason=payload.get("reason", None),
            message=payload.get("message", None),
            creation_timestamp=iso8601.parse_date(
                payload["metadata"]["creationTimestamp"]
            ),
            first_timestamp=first_timestamp,
            event_time=event_time,
            last_timestamp=last_timestamp,
        )

    @classmethod
    def from_model(cls, event: CoreV1Event) -> Self:
        return cls(
            involved_object=event.involved_object,
            count=event.count,
            reason=event.reason,
            message=event.message,
            creation_timestamp=event.metadata.creation_timestamp,
            first_timestamp=event.first_timestamp,
            event_time=event.event_time,
            last_timestamp=event.last_timestamp,
        )

    @property
    def timestamp(self) -> datetime:
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
    def from_primitive(cls, payload: dict[str, Any]) -> "PodStatus":
        if "containerStatuses" in payload and payload["containerStatuses"]:
            container_statuses = tuple(
                ContainerStatus.from_primitive(s) for s in payload["containerStatuses"]
            )
        else:
            container_statuses = (ContainerStatus(),)
        return cls(
            phase=payload["phase"],
            container_statuses=container_statuses,
            reason=payload.get("reason"),
            conditions=[
                PodCondition.from_primitive(c) for c in payload.get("conditions", [])
            ],
        )

    @classmethod
    def from_model(cls, model: V1PodStatus) -> "PodStatus":
        if model.container_statuses:
            container_statuses = tuple(
                ContainerStatus.from_model(s) for s in model.container_statuses or []
            )
        else:
            container_statuses = (ContainerStatus(),)
        return cls(
            phase=model.phase,
            container_statuses=container_statuses,
            reason=model.reason,
            conditions=[PodCondition.from_model(c) for c in model.conditions or []],
        )


@dataclass(frozen=True)
class NodeTaint:
    key: str
    value: str
    effect: str = "NoSchedule"

    def to_primitive(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value, "effect": self.effect}

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
    def from_primitive(cls, payload: dict[str, Any]) -> "NodeResources":
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


class NodeConditionType(enum.Enum):
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
    def from_primitive(cls, payload: dict[str, Any]) -> "NodeCondition":
        return cls(
            type=NodeConditionType.parse(payload["type"]),
            status=cls._parse_status(payload["status"]),
            message=payload.get("message", ""),
            reason=payload.get("reason", ""),
            transition_time=(
                iso8601.parse_date(t)
                if (t := payload.get("lastTransitionTime"))
                else None
            ),
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
    def from_primitive(cls, payload: dict[str, Any]) -> "NodeStatus":
        return cls(
            allocatable_resources=NodeResources.from_primitive(payload["allocatable"]),
            conditions=[
                NodeCondition.from_primitive(p) for p in payload.get("conditions", ())
            ],
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
    def from_primitive(cls, payload: dict[str, Any]) -> "Node":
        metadata = payload["metadata"]
        return cls(
            name=metadata["name"],
            labels=metadata.get("labels", {}),
            status=NodeStatus.from_primitive(payload["status"]),
        )

    def get_free_resources(self, resource_requests: NodeResources) -> NodeResources:
        return self.status.allocatable_resources - resource_requests


@dataclass(frozen=True)
class ListResult:
    resource_version: str
    items: list[dict[str, Any]]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "ListResult":
        return cls(
            resource_version=payload["metadata"]["resourceVersion"],
            items=payload["items"],
        )


class WatchEventType(str, Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"
    ERROR = "ERROR"


@dataclass(frozen=True)
class WatchBookmarkEvent:
    resource_version: str

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "WatchBookmarkEvent":
        return cls(
            resource_version=payload["object"]["metadata"]["resourceVersion"],
        )

    @classmethod
    def is_bookmark(cls, payload: dict[str, Any]) -> bool:
        return "BOOKMARK" == payload["type"].upper()


@dataclass(frozen=True)
class WatchEvent:
    type: WatchEventType
    resource: dict[str, Any]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "WatchEvent":
        return cls(type=WatchEventType(payload["type"]), resource=payload["object"])

    @classmethod
    def is_error(cls, payload: dict[str, Any]) -> bool:
        return WatchEventType.ERROR == payload["type"].upper()

    @classmethod
    def create_added(cls, resource: dict[str, Any]) -> "WatchEvent":
        return cls(type=WatchEventType.ADDED, resource=resource)

    @classmethod
    def create_modified(cls, resource: dict[str, Any]) -> "WatchEvent":
        return cls(type=WatchEventType.MODIFIED, resource=resource)

    @classmethod
    def create_deleted(cls, resource: dict[str, Any]) -> "WatchEvent":
        return cls(type=WatchEventType.DELETED, resource=resource)


class KubeClient(ApoloKubeClient):
    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._api_resources: APIResources = APIResources()

    async def init_api_resources(self) -> None:
        for gv in APIResources.group_versions:
            try:
                self._api_resources[gv] = await self.get_api_resource(gv)
            except aiohttp.ClientResponseError as ex:
                if ex.status != 404:
                    raise

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        await self.init_api_resources()
        return self

    @property
    def _all_pods_url(self) -> str:
        return f"{self.api_v1_url}/pods"

    def _generate_pod_url(self, namespace: str, pod_id: str) -> str:
        pods_url = self._generate_pods_url(namespace)
        return f"{pods_url}/{pod_id}"

    def _generate_pods_url(
        self, namespace: str | None = None, all_namespaces: bool = False
    ) -> str:
        if all_namespaces or not namespace:
            return self._all_pods_url
        namespace_url = self.generate_namespace_url(namespace)
        return f"{namespace_url}/pods"

    @property
    def _nodes_url(self) -> str:
        return f"{self.api_v1_url}/nodes"

    def _generate_node_url(self, name: str) -> str:
        return f"{self._nodes_url}/{name}"

    def _generate_all_secrets_url(self, namespace_name: str | None = None) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = self.generate_namespace_url(namespace_name)
        return f"{namespace_url}/secrets"

    def _generate_secret_url(
        self, secret_name: str, namespace_name: str | None = None
    ) -> str:
        all_secrets_url = self._generate_all_secrets_url(namespace_name)
        return f"{all_secrets_url}/{secret_name}"

    def _create_headers(self, headers: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = dict(headers) if headers else {}
        if self._auth_type == KubeClientAuthType.TOKEN and self._token:
            headers["Authorization"] = "Bearer " + self._token
        return headers

    async def _watch(
        self,
        url: str,
        params: dict[str, str] | None = None,
        resource_version: str | None = None,
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        params = params or {}
        params.update(watch="true", allowWatchBookmarks="true")
        if resource_version:
            params["resourceVersion"] = resource_version
        assert self._client
        async with self._client.get(
            url,
            params=params,
            headers=self._create_headers(),
            timeout=aiohttp.ClientTimeout(),
        ) as response:
            if response.status == 410:
                raise ResourceGone
            async for line in response.content:
                payload = json.loads(line)
                self._raise_for_status(payload)
                if WatchEvent.is_error(payload):
                    self._raise_for_status(payload["object"])
                if WatchBookmarkEvent.is_bookmark(payload):
                    yield WatchBookmarkEvent.from_primitive(payload)
                else:
                    yield WatchEvent.from_primitive(payload)

    async def get_api_resource(self, group_version: str) -> APIResource:
        url = f"{self._base_url}/apis/{group_version}"
        payload = await self.get(url=url, raise_for_status=True)
        return APIResource.from_primitive(payload)

    async def _delete_resource_url(self, url: str, uid: str | None = None) -> None:
        request_payload = None
        if uid:
            request_payload = {"preconditions": {"uid": uid}}
        try:
            await self.delete(url=url, json=request_payload)
        except ResourceExists as e:
            # This might happen if we try to remove resource by it's name, but
            # different UID, see https://github.com/neuro-inc/platform-api/pull/1525
            raise ResourceNotFound(str(e))

    async def create_node(
        self,
        name: str,
        capacity: dict[str, Any],
        labels: dict[str, str] | None = None,
        taints: Sequence[NodeTaint] | None = None,
    ) -> None:
        taints = taints or []
        payload = {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name, "labels": labels or {}},
            "spec": {"taints": [taint.to_primitive() for taint in taints]},
            "status": {
                # TODO (ajuszkowski, 29-0-2019) add enum for capacity
                "capacity": capacity,
                "conditions": [{"status": "True", "type": "Ready"}],
            },
        }
        url = self._nodes_url
        await self.post(url=url, json=payload)

    async def delete_node(self, name: str) -> None:
        url = self._generate_node_url(name)
        await self._delete_resource_url(url)

    async def get_nodes(self) -> Sequence[Node]:
        payload = await self.get(url=self._nodes_url)
        assert payload["kind"] == "NodeList"
        nodes = []
        for item in payload["items"]:
            nodes.append(Node.from_primitive(item))
        return nodes

    async def get_node(self, name: str) -> Node:
        payload = await self.get(url=self._generate_node_url(name))
        return Node.from_primitive(payload)

    async def get_raw_nodes(self, labels: dict[str, str] | None = None) -> ListResult:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        payload = await self.get(url=self._nodes_url, params=params)
        return ListResult.from_primitive(payload)

    async def watch_nodes(
        self,
        labels: dict[str, str] | None = None,
        resource_version: str | None = None,
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        async for event in self._watch(self._nodes_url, params, resource_version):
            yield event

    async def create_pod(
        self, namespace: str, descriptor: PodDescriptor
    ) -> PodDescriptor:
        url = self._generate_pods_url(namespace)
        payload = await self.post(
            url=url,
            json=descriptor.to_primitive(),
            raise_for_status=False,
        )
        return PodDescriptor.from_primitive(payload)

    async def set_raw_pod_status(
        self,
        namespace: str,
        name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        url = self._generate_pod_url(namespace, name) + "/status"
        return await self.put(url=url, json=payload)

    async def get_pod(self, namespace: str, pod_name: str) -> PodDescriptor:
        url = self._generate_pod_url(namespace, pod_name)
        payload = await self.get(
            url=url,
            raise_for_status=False,
        )
        return PodDescriptor.from_primitive(payload)

    async def get_raw_pod(self, namespace: str, name: str) -> dict[str, Any]:
        url = self._generate_pod_url(namespace, name)
        return await self.get(url=url)

    async def get_raw_pods(self, all_namespaces: bool = False) -> ListResult:
        url = self._generate_pods_url(all_namespaces=all_namespaces)
        payload = await self.get(url=url)
        assert payload["kind"] == "PodList"
        return ListResult.from_primitive(payload)

    async def watch_pods(
        self, all_namespaces: bool = False, resource_version: str | None = None
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        url = self._generate_pods_url(all_namespaces=all_namespaces)
        async for event in self._watch(url, resource_version=resource_version):
            yield event

    async def get_pod_status(
        self,
        namespace: str,
        pod_id: str,
    ) -> PodStatus:
        pod = await self.get_pod(namespace, pod_id)
        if pod.status is None:
            raise ValueError("Missing pod status")
        return pod.status

    async def delete_pod(
        self, namespace: str, pod_name: str, *, force: bool = False
    ) -> PodStatus:
        url = self._generate_pod_url(namespace, pod_name)
        request_payload = None
        if force:
            request_payload = {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "gracePeriodSeconds": 0,
            }
        payload = await self.delete(
            url=url,
            json=request_payload,
            raise_for_status=False,
        )
        pod = PodDescriptor.from_primitive(payload)
        return pod.status  # type: ignore

    async def create_docker_secret(self, secret: DockerRegistrySecret) -> None:
        url = self._generate_all_secrets_url(secret.namespace)
        await self.post(url=url, json=secret.to_primitive())

    async def update_docker_secret(
        self, secret: DockerRegistrySecret, create_non_existent: bool = False
    ) -> None:
        try:
            url = self._generate_secret_url(secret.name, secret.namespace)
            await self.put(url=url, json=secret.to_primitive())
        except KubeClientException as e:
            if isinstance(e, ResourceNotFound) and create_non_existent:
                await self.create_docker_secret(secret)
            else:
                raise e

    async def delete_secret(
        self, secret_name: str, namespace_name: str | None = None
    ) -> None:
        url = self._generate_secret_url(secret_name, namespace_name)
        await self._delete_resource_url(url)

    async def wait_pod_is_running(
        self,
        namespace: str,
        pod_name: str,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> None:
        """Wait until the pod transitions from the waiting state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(namespace, pod_name)
                if not pod_status.is_waiting:
                    return
                await asyncio.sleep(interval_s)

    async def wait_pod_is_terminated(
        self,
        namespace: str,
        pod_name: str,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> None:
        """Wait until the pod transitions to the terminated state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(namespace, pod_name)
                if pod_status.is_terminated:
                    return
                await asyncio.sleep(interval_s)


class EventHandler:
    @abc.abstractmethod
    async def init(self, resources: list[dict[str, Any]]) -> None:
        pass

    @abc.abstractmethod
    async def handle(self, event: WatchEvent) -> None:
        pass


class Watcher(abc.ABC):
    resource_version: str

    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._handlers: list[EventHandler] = []
        self._watcher_task: asyncio.Task[None] | None = None

    def subscribe(self, handler: EventHandler) -> None:
        if self._watcher_task is not None:
            raise Exception("Subscription is not possible after watcher start")
        self._handlers.append(handler)

    async def __aenter__(self) -> "Watcher":
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

        self.resource_version = result.resource_version

    async def _watch(self) -> None:
        while True:
            try:
                async for event in self.watch(self.resource_version):
                    if isinstance(event, WatchBookmarkEvent):
                        self.resource_version = event.resource_version
                        continue
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

    @abc.abstractmethod
    async def list(self) -> ListResult:
        pass

    @abc.abstractmethod
    async def watch(
        self, resource_version: str
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        yield  # type: ignore


class NodeWatcher(Watcher):
    def __init__(
        self, kube_client: KubeClient, labels: dict[str, str] | None = None
    ) -> None:
        super().__init__(kube_client)
        self._kwargs = {"labels": labels}

    async def list(self) -> ListResult:
        return await self._kube_client.get_raw_nodes(**self._kwargs)

    async def watch(
        self, resource_version: str
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        async for event in self._kube_client.watch_nodes(
            resource_version=resource_version, **self._kwargs
        ):
            yield event


class PodWatcher(Watcher):
    def __init__(self, kube_client: KubeClient, all_namespaces: bool = True) -> None:
        super().__init__(kube_client)
        self._kwargs = {"all_namespaces": all_namespaces}

    async def list(self) -> ListResult:
        return await self._kube_client.get_raw_pods(**self._kwargs)

    async def watch(
        self, resource_version: str
    ) -> AsyncIterator[WatchEvent | WatchBookmarkEvent]:
        async for event in self._kube_client.watch_pods(
            resource_version=resource_version, **self._kwargs
        ):
            yield event


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


async def set_raw_pod_status(
    namespace: str,
    name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {}
    # url = self._generate_pod_url(namespace, name) + "/status"
    # return await self.put(url=url, json=payload)


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
    ingress = await client_proxy.networking_k8s_io_v1.ingress.create(model)
    return Ingress.from_model(ingress)


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
    return secret.to_dict()


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
