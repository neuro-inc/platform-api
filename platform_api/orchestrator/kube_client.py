import abc
import asyncio
import enum
import json
import logging
from asyncio import timeout
from base64 import b64encode
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from pathlib import PurePath
from typing import Any, ClassVar, NoReturn, Optional

import aiohttp
import iso8601
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


@dataclass(frozen=True)
class SecretRef:
    name: str

    def to_primitive(self) -> dict[str, str]:
        return {"name": self.name}

    @classmethod
    def from_primitive(cls, payload: dict[str, str]) -> "SecretRef":
        return cls(**payload)


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


@dataclass(frozen=True)
class NodePreferredSchedulingTerm:
    preference: LabelSelectorTerm
    weight: int = 100

    def to_primitive(self) -> dict[str, Any]:
        return {"preference": self.preference.to_primitive(), "weight": self.weight}


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


@dataclass(frozen=True)
class PodPreferredSchedulingTerm:
    pod_affinity_term: PodAffinityTerm
    weight: int = 100

    def to_primitive(self) -> dict[str, Any]:
        return {
            "podAffinityTerm": self.pod_affinity_term.to_primitive(),
            "weight": self.weight,
        }


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
        priority_class_name: str | None = None,
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
        if self.priority_class_name:
            payload["spec"]["priorityClassName"] = self.priority_class_name
        return payload

    def _to_primitive_ports(self) -> list[dict[str, int]]:
        ports = []
        if self.port:
            ports.append({"containerPort": self.port})
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
            priority_class_name=payload["spec"].get("priorityClassName"),
            restart_policy=PodRestartPolicy(
                payload["spec"].get("restartPolicy", str(cls.restart_policy))
            ),
            working_dir=container_payload.get("workingDir"),
            resources=Resources.from_primitive(container_payload.get("resources", {})),
        )


class ContainerStatus:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}

    @property
    def _state(self) -> dict[str, Any]:
        return self._payload.get("state", {})

    @property
    def is_waiting(self) -> bool:
        return not self._state or "waiting" in self._state

    @property
    def is_terminated(self) -> bool:
        return bool(self._state) and "terminated" in self._state

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
            return state.get("reason")
        return None

    @property
    def message(self) -> str | None:
        for state in self._state.values():
            return state.get("message")
        return None

    @property
    def exit_code(self) -> int | None:
        assert self.is_terminated
        return self._state["terminated"]["exitCode"]

    @property
    def is_creating(self) -> bool:
        # TODO (A Danshyn 07/20/18): handle PodInitializing
        # TODO (A Danshyn 07/20/18): consider handling other reasons
        # https://github.com/kubernetes/kubernetes/blob/886e04f1fffbb04faf8a9f9ee141143b2684ae68/pkg/kubelet/images/types.go#L25-L43
        return self.is_waiting and self.reason in (None, "ContainerCreating")


class PodConditionType(enum.Enum):
    UNKNOWN = "Unknown"
    POD_SCHEDULED = "PodScheduled"
    READY = "Ready"
    INITIALIZED = "Initialized"
    CONTAINERS_READY = "ContainersReady"


class PodCondition:
    # https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    @property
    def transition_time(self) -> datetime:
        return iso8601.parse_date(self._payload["lastTransitionTime"])

    @property
    def reason(self) -> str:
        return self._payload.get("reason", "")

    @property
    def message(self) -> str:
        return self._payload.get("message", "")

    @property
    def status(self) -> bool | None:
        val = self._payload["status"]
        if val == "Unknown":
            return None
        if val == "True":
            return True
        if val == "False":
            return False
        raise ValueError(f"Invalid status {val!r}")

    @property
    def type(self) -> PodConditionType:
        try:
            return PodConditionType(self._payload["type"])
        except (KeyError, ValueError):
            return PodConditionType.UNKNOWN


class KubernetesEvent:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload or {}

    @property
    def involved_object(self) -> dict[str, str]:
        return self._payload["involvedObject"]

    @property
    def reason(self) -> str | None:
        return self._payload.get("reason", None)

    @property
    def message(self) -> str | None:
        return self._payload.get("message", None)

    @property
    def first_timestamp(self) -> datetime | None:
        event_time = self._payload.get("firstTimestamp") or self._payload.get(
            "deprecatedFirstTimestamp"
        )
        if event_time:
            return iso8601.parse_date(event_time)
        return None

    @property
    def event_time(self) -> datetime | None:
        event_time = self._payload.get("eventTime")
        if event_time:
            return iso8601.parse_date(event_time)
        return None

    @property
    def last_timestamp(self) -> datetime | None:
        event_time = self._payload.get("lastTimestamp") or self._payload.get(
            "deprecatedLastTimestamp"
        )
        if event_time:
            return iso8601.parse_date(event_time)
        return None

    @property
    def creation_timestamp(self) -> datetime:
        event_time = self._payload["metadata"]["lastTimestamp"]
        return iso8601.parse_date(event_time)

    @property
    def timestamp(self) -> datetime:
        return (
            self.last_timestamp
            or self.event_time
            or self.first_timestamp
            or self.creation_timestamp
        )

    @property
    def count(self) -> int:
        return self._payload["count"]


class PodStatus:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self._container_statuses = self._create_container_status()

    def _create_container_status(self) -> Sequence[ContainerStatus]:
        container_statuses = self._payload.get("containerStatuses")
        if not container_statuses:
            return (ContainerStatus(),)
        return tuple(ContainerStatus(p) for p in container_statuses)

    @property
    def phase(self) -> str:
        """
        "Pending", "Running", "Succeeded", "Failed", "Unknown"
        """
        return self._payload["phase"]

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
        return self._payload.get("reason") or self.container_status.reason

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
        return [PodCondition(val) for val in self._payload.get("conditions", [])]

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "PodStatus":
        return cls(payload)


@dataclass(frozen=True)
class NodeTaint:
    key: str
    value: str
    effect: str = "NoSchedule"

    def to_primitive(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value, "effect": self.effect}


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

    def _generate_network_policy_url(self, name: str, namespace_name: str) -> str:
        all_nps_url = self.generate_network_policy_url(namespace_name)
        return f"{all_nps_url}/{name}"

    def _generate_endpoint_url(self, name: str, namespace: str) -> str:
        return f"{self.generate_namespace_url(namespace)}/endpoints/{name}"

    @property
    def _nodes_url(self) -> str:
        return f"{self.api_v1_url}/nodes"

    def _generate_node_url(self, name: str) -> str:
        return f"{self._nodes_url}/{name}"

    def _generate_ingress_url(self, namespace: str, ingress_name: str) -> str:
        url = self._generate_ingresses_url(namespace)
        return f"{url}/{ingress_name}"

    def _generate_services_url(self, namespace: str) -> str:
        url = self.generate_namespace_url(namespace)
        return f"{url}/services"

    def _generate_service_url(self, namespace: str, service_name: str) -> str:
        url = self._generate_services_url(namespace)
        return f"{url}/{service_name}"

    def _generate_networking_v1_namespace_url(self, namespace: str) -> str:
        return f"{self._base_url}/apis/networking.k8s.io/v1/namespaces/{namespace}"

    def _generate_networking_v1beta1_namespace_url(self, namespace: str) -> str:
        return f"{self._base_url}/apis/networking.k8s.io/v1beta1/namespaces/{namespace}"

    def _generate_ingresses_url(self, namespace: str) -> str:
        if self._api_resources.has_networking_v1_ingress:
            url = self._generate_networking_v1_namespace_url(namespace)
        else:
            url = self._generate_networking_v1beta1_namespace_url(namespace)
        return f"{url}/ingresses"

    def _generate_all_secrets_url(self, namespace_name: str | None = None) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = self.generate_namespace_url(namespace_name)
        return f"{namespace_url}/secrets"

    def _generate_all_pvcs_url(self, namespace_name: str | None = None) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = self.generate_namespace_url(namespace_name)
        return f"{namespace_url}/persistentvolumeclaims"

    def _generate_secret_url(
        self, secret_name: str, namespace_name: str | None = None
    ) -> str:
        all_secrets_url = self._generate_all_secrets_url(namespace_name)
        return f"{all_secrets_url}/{secret_name}"

    def _generate_pvc_url(
        self, pvc_name: str, namespace_name: str | None = None
    ) -> str:
        all_pvcs_url = self._generate_all_pvcs_url(namespace_name)
        return f"{all_pvcs_url}/{pvc_name}"

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

    async def get_endpoint(
        self, name: str, namespace: str | None = None
    ) -> dict[str, Any]:
        url = self._generate_endpoint_url(name, namespace or self._namespace)
        return await self.get(url=url)

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

    async def delete_all_pods(self, namespace: str, *, labels: dict[str, str]) -> None:
        url = self._generate_pods_url(namespace)
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self.delete(url=url, params=params)

    async def create_ingress(
        self,
        name: str,
        namespace: str,
        ingress_class: str | None = None,
        rules: list[IngressRule] | None = None,
        annotations: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> Ingress:
        rules = rules or []
        annotations = annotations or {}
        labels = labels or {}
        ingress = Ingress(
            name=name,
            ingress_class=ingress_class,
            rules=rules,
            annotations=annotations,
            labels=labels,
        )
        if self._api_resources.has_networking_v1_ingress:
            payload = ingress.to_v1_primitive()
        else:
            payload = ingress.to_v1beta1_primitive()

        url = self._generate_ingresses_url(namespace)
        payload = await self.post(url=url, json=payload)
        return Ingress.from_primitive(payload)

    async def get_ingress(self, namespace: str, name: str) -> Ingress:
        url = self._generate_ingress_url(namespace, name)
        payload = await self.get(url=url)
        return Ingress.from_primitive(payload)

    async def delete_all_ingresses(
        self, namespace: str, *, labels: dict[str, str] | None = None
    ) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        url = self._generate_ingresses_url(namespace)
        await self.delete(url=url, params=params)

    async def delete_ingress(self, namespace: str, name: str) -> None:
        url = self._generate_ingress_url(namespace, name)
        await self.delete(url=url)

    async def add_ingress_rule(
        self, namespace: str, name: str, rule: IngressRule
    ) -> Ingress:
        # TODO (A Danshyn 06/13/18): test if does not exist already
        url = self._generate_ingress_url(namespace, name)
        headers = {"Content-Type": "application/json-patch+json"}
        if self._api_resources.has_networking_v1_ingress:
            rule_payload = rule.to_v1_primitive()
        else:
            rule_payload = rule.to_v1beta1_primitive()
        patches = [{"op": "add", "path": "/spec/rules/-", "value": rule_payload}]
        payload = await self.patch(url=url, headers=headers, json=patches)
        return Ingress.from_primitive(payload)

    async def remove_ingress_rule(
        self, namespace: str, name: str, host: str
    ) -> Ingress:
        # TODO (A Danshyn 06/13/18): this one should have a retry in case of
        # a race condition
        ingress = await self.get_ingress(namespace, name)
        rule_index = ingress.find_rule_index_by_host(host)
        if rule_index < 0:
            raise KubeClientException("NotFound")
        url = self._generate_ingress_url(namespace, name)
        rule = [
            {"op": "test", "path": f"/spec/rules/{rule_index}/host", "value": host},
            {"op": "remove", "path": f"/spec/rules/{rule_index}"},
        ]
        headers = {"Content-Type": "application/json-patch+json"}
        payload = await self.patch(url=url, headers=headers, json=rule)
        return Ingress.from_primitive(payload)

    async def create_service(self, namespace: str, service: Service) -> Service:
        url = self._generate_services_url(namespace)
        payload = await self.post(url=url, json=service.to_primitive())
        return Service.from_primitive(payload)

    async def get_service(self, namespace: str, name: str) -> Service:
        url = self._generate_service_url(namespace, name)
        payload = await self.get(url=url)
        return Service.from_primitive(payload)

    async def list_services(
        self, namespace: str, labels: dict[str, str]
    ) -> list[Service]:
        url = self._generate_services_url(namespace)
        label_selector = ",".join(f"{label}={value}" for label, value in labels.items())
        payload = await self.get(url=url, params={"labelSelector": label_selector})
        return [Service.from_primitive(item) for item in payload["items"]]

    async def delete_service(
        self, namespace: str, name: str, uid: str | None = None
    ) -> None:
        url = self._generate_service_url(namespace, name)
        await self._delete_resource_url(url, uid)

    async def delete_all_services(
        self, namespace: str, *, labels: dict[str, str] | None = None
    ) -> None:
        url = self._generate_services_url(namespace)
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self.delete(url=url, params=params)

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

    async def get_raw_secret(
        self, secret_name: str, namespace_name: str | None = None
    ) -> dict[str, Any]:
        url = self._generate_secret_url(secret_name, namespace_name)
        return await self.get(url=url)

    async def delete_secret(
        self, secret_name: str, namespace_name: str | None = None
    ) -> None:
        url = self._generate_secret_url(secret_name, namespace_name)
        await self._delete_resource_url(url)

    async def get_raw_pvc(
        self, pvc_name: str, namespace_name: str | None = None
    ) -> dict[str, Any]:
        url = self._generate_pvc_url(pvc_name, namespace_name)
        return await self.get(url=url)

    async def get_pod_events(
        self, pod_id: str, namespace: str
    ) -> list[KubernetesEvent]:
        params = {
            "fieldSelector": (
                "involvedObject.kind=Pod"
                f",involvedObject.namespace={namespace}"
                f",involvedObject.name={pod_id}"
            )
        }
        url = f"{self.api_v1_url}/namespaces/{namespace}/events"
        payload = await self.get(url=url, params=params)
        return [KubernetesEvent(item) for item in payload.get("items", [])]

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

    async def wait_pod_is_finished(
        self,
        namespace: str,
        pod_name: str,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> None:
        """Wait until the pod is finished.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(namespace, pod_name)
                if pod_status.phase in ("Succeeded", "Failed"):
                    return
                await asyncio.sleep(interval_s)

    async def wait_pod_is_deleted(
        self,
        namespace: str,
        pod_name: str,
        timeout_s: float = 10.0 * 60,
        interval_s: float = 1.0,
    ) -> None:
        async with timeout(timeout_s):
            while True:
                try:
                    await self.get_pod(namespace, pod_name)
                    await asyncio.sleep(interval_s)
                except JobNotFoundException:
                    return

    async def create_default_network_policy(
        self,
        namespace: str,
        name: str,
        pod_labels: dict[str, str],
        org_project_labels: dict[str, str],
    ) -> dict[str, Any]:
        assert pod_labels
        # https://tools.ietf.org/html/rfc1918#section-3
        rules: list[dict[str, Any]] = [
            # allowing pods to connect to public networks only
            {
                "to": [
                    {
                        "ipBlock": {
                            "cidr": "0.0.0.0/0",
                            "except": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"],
                        }
                    }
                ]
            },
            # allowing labeled pods to make DNS queries in our private
            # networks, because pods' /etc/resolv.conf files still
            # point to the internal DNS
            {
                "to": [
                    {"ipBlock": {"cidr": "10.0.0.0/8"}},
                    {"ipBlock": {"cidr": "172.16.0.0/12"}},
                    {"ipBlock": {"cidr": "192.168.0.0/16"}},
                ],
                "ports": [
                    {"port": 53, "protocol": "UDP"},
                    {"port": 53, "protocol": "TCP"},
                ],
            },
            {
                "to": [
                    # allowing labeled pods to connect to each other
                    {"podSelector": {"matchLabels": pod_labels}},
                    # allow connect to namespaces with same project labels
                    {"namespaceSelector": {"matchLabels": org_project_labels}},
                ]
            },
        ]
        return await self.create_egress_network_policy(
            namespace, name, pod_labels=pod_labels, rules=rules
        )

    async def create_egress_network_policy(
        self,
        namespace: str,
        name: str,
        *,
        pod_labels: dict[str, str],
        rules: list[dict[str, Any]],
        labels: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        assert pod_labels
        assert rules
        labels = labels or {}
        request_payload = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                # applying the rules below to labeled pods
                "podSelector": {"matchLabels": pod_labels},
                "policyTypes": ["Egress"],
                "egress": rules,
            },
        }
        url = self.generate_network_policy_url(namespace)
        return await self.post(url=url, json=request_payload)

    async def get_network_policy(
        self,
        namespace: str,
        name: str,
    ) -> dict[str, Any]:
        url = self._generate_network_policy_url(name, namespace)
        return await self.get(url=url)

    async def delete_network_policy(
        self,
        namespace: str,
        name: str,
    ) -> None:
        url = self._generate_network_policy_url(name, namespace)
        await self._delete_resource_url(url)

    async def delete_all_network_policies(
        self,
        namespace: str,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self.delete(
            url=self.generate_network_policy_url(namespace),
            params=params,
        )


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
