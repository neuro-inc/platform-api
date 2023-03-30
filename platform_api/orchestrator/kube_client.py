import abc
import asyncio
import enum
import json
import logging
import re
import ssl
from base64 import b64encode
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from pathlib import Path, PurePath
from types import TracebackType
from typing import Any, ClassVar, NoReturn, Optional, Union
from urllib.parse import urlsplit

import aiohttp
import iso8601
from aiohttp import WSMsgType
from async_timeout import timeout
from multidict import MultiDict
from yarl import URL

from platform_api.utils.stream import Stream

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


class KubeClientException(Exception):
    pass


class StatusException(KubeClientException):
    pass


class AlreadyExistsException(StatusException):
    pass


class NotFoundException(StatusException):
    pass


class ResourceGoneException(KubeClientException):
    pass


class KubeClientUnauthorizedException(KubeClientException):
    pass


def _raise_status_job_exception(pod: dict[str, Any], job_id: Optional[str]) -> NoReturn:
    if pod["code"] == 409:
        raise AlreadyExistsException(pod.get("reason", "job already exists"))
    elif pod["code"] == 404:
        raise JobNotFoundException(f"job {job_id} was not found")
    elif pod["code"] == 422:
        raise JobError(f"cant create job with id {job_id}")
    else:
        raise JobError("unexpected")


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
    group_versions: list[str] = [GroupVersion.NETWORKING_V1]

    @property
    def networking_v1(self) -> Optional[APIResource]:
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
        mount_sub_path: Optional[PurePath] = None,
    ) -> "VolumeMount":
        raise NotImplementedError("Cannot create mount for abstract Volume type.")

    def to_primitive(self) -> dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class PathVolume(Volume):
    # None for cluster storage.
    # /org for organization/additional storage.
    path: Optional[PurePath]

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: Optional[PurePath] = None,
    ) -> "VolumeMount":
        try:
            sub_path = container_volume.src_path.relative_to(
                "/" if self.path is None else str(self.path)
            )
        except ValueError:
            sub_path = container_volume.src_path.relative_to("/")
        mount_sub_path = mount_sub_path or PurePath("")
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path / mount_sub_path,
            sub_path=sub_path,
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class HostVolume(PathVolume):
    host_path: PurePath

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "hostPath": {"path": str(self.host_path), "type": "Directory"},
        }


@dataclass(frozen=True)
class SharedMemoryVolume(Volume):
    def to_primitive(self) -> dict[str, Any]:
        return {"name": self.name, "emptyDir": {"medium": "Memory"}}

    def create_mount(
        self,
        container_volume: ContainerVolume,
        mount_sub_path: Optional[PurePath] = None,
    ) -> "VolumeMount":
        mount_sub_path = mount_sub_path or PurePath("")
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path / mount_sub_path,
            sub_path=PurePath(""),
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class NfsVolume(PathVolume):
    server: str
    export_path: PurePath

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "nfs": {"server": self.server, "path": str(self.export_path)},
        }


@dataclass(frozen=True)
class PVCVolume(PathVolume):
    claim_name: str

    def to_primitive(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "persistentVolumeClaim": {"claimName": self.claim_name},
        }


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
    sub_path: PurePath = PurePath("")
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
    memory_request: Optional[int] = None
    gpu: Optional[int] = None
    shm: Optional[bool] = None
    tpu_version: Optional[str] = None
    tpu_cores: Optional[int] = None

    gpu_key: ClassVar[str] = "nvidia.com/gpu"
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
        if self.gpu:
            payload["requests"][self.gpu_key] = self.gpu
            payload["limits"][self.gpu_key] = self.gpu
        if self.tpu_version:
            payload["requests"][self.tpu_key] = self.tpu_cores
            payload["limits"][self.tpu_key] = self.tpu_cores
        if self.memory_request:
            payload["requests"]["memory"] = self.memory_request_str
        return payload

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "Resources":
        requests = payload.get("requests", {})
        gpu = None
        if cls.gpu_key in requests:
            gpu = int(requests[cls.gpu_key])
        tpu_version, tpu_cores = cls._parse_tpu(requests)
        return cls(
            cpu=cls.parse_cpu(requests.get("cpu", "0")),
            memory=cls.parse_memory(requests.get("memory", "0Mi")),
            gpu=gpu,
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
            else:
                raise ValueError(f"{memory!r} memory format is not supported")
        return memory_b

    @classmethod
    def _parse_tpu(cls, payload: dict[str, Any]) -> tuple[Optional[str], Optional[int]]:
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
            gpu=resources.gpu,
            shm=resources.shm,
            **kwargs,
        )


@dataclass(frozen=True)
class Service:
    name: str
    target_port: Optional[int]
    uid: Optional[str] = None
    selector: dict[str, str] = field(default_factory=dict)
    port: int = 80
    service_type: ServiceType = ServiceType.CLUSTER_IP
    cluster_ip: Optional[str] = None
    labels: dict[str, str] = field(default_factory=dict)

    def _add_port_map(
        self,
        port: Optional[int],
        target_port: Optional[int],
        port_name: str,
        ports: list[dict[str, Any]],
    ) -> None:
        if target_port:
            ports.append({"port": port, "targetPort": target_port, "name": port_name})

    def to_primitive(self) -> dict[str, Any]:
        service_descriptor: dict[str, Any] = {
            "metadata": {"name": self.name},
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
    def from_primitive(cls, payload: dict[str, Any]) -> "Service":
        http_payload = cls._find_port_by_name("http", payload["spec"]["ports"])
        service_type = payload["spec"].get("type", Service.service_type.value)
        return cls(
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
    service_name: Optional[str] = None
    service_port: Optional[int] = None

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
    ingress_class: Optional[str] = None
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
        primitive = {"metadata": metadata, "spec": {"rules": rules}}
        return primitive

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
        # TODO (A Danshyn 06/13/18): should be refactored along with PodStatus
        kind = payload["kind"]
        if kind == "Ingress":
            if payload["apiVersion"] == GroupVersion.NETWORKING_V1:
                return cls._from_v1_primitive(payload)
            return cls._from_v1beta1_primitive(payload)
        elif kind == "Status":
            # TODO (A.Yushkovskiy, 28-Jun-2019) patch this method to raise a proper
            #  error, not always `JobNotFoundException` (see issue #792)
            _raise_status_job_exception(payload, job_id=None)
        else:
            raise ValueError(f"unknown kind: {kind}")

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


class NodeSelectorOperator(str, Enum):
    DOES_NOT_EXIST = "DoesNotExist"
    EXISTS = "Exists"
    IN = "In"
    NOT_IN = "NotIn"
    GT = "Gt"
    LT = "Lt"

    @property
    def requires_no_values(self) -> bool:
        return self in (self.DOES_NOT_EXIST, self.EXISTS)

    def apply(self, label_value: Optional[str], values: list[str]) -> bool:
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
class NodeSelectorRequirement:
    key: str
    operator: NodeSelectorOperator
    values: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("blank key")
        if self.operator.requires_no_values and self.values:
            raise ValueError("values must be empty")

    @classmethod
    def create_in(cls, key: str, *values: str) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.IN, values=[*values])

    @classmethod
    def create_exists(cls, key: str) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.EXISTS)

    @classmethod
    def create_does_not_exist(cls, key: str) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.DOES_NOT_EXIST)

    @classmethod
    def create_gt(cls, key: str, value: int) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.GT, values=[str(value)])

    @classmethod
    def create_lt(cls, key: str, value: int) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.LT, values=[str(value)])

    def is_satisfied(self, node_labels: dict[str, str]) -> bool:
        label_value = node_labels.get(self.key)
        return self.operator.apply(label_value, self.values)

    def to_primitive(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"key": self.key, "operator": self.operator.value}
        if self.values:
            payload["values"] = self.values.copy()
        return payload


@dataclass(frozen=True)
class NodeSelectorTerm:
    match_expressions: list[NodeSelectorRequirement]

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
    preference: NodeSelectorTerm
    weight: int = 100

    def to_primitive(self) -> dict[str, Any]:
        return {"preference": self.preference.to_primitive(), "weight": self.weight}


@dataclass(frozen=True)
class NodeAffinity:
    required: list[NodeSelectorTerm] = field(default_factory=list)
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
    working_dir: Optional[str] = None
    env: dict[str, str] = field(default_factory=dict)
    # TODO (artem): create base type `EnvVar` and merge `env` and `secret_env`
    secret_env_list: list[SecretEnvVar] = field(default_factory=list)
    volume_mounts: list[VolumeMount] = field(default_factory=list)
    volumes: list[Volume] = field(default_factory=list)
    resources: Optional[Resources] = None
    node_selector: dict[str, str] = field(default_factory=dict)
    tolerations: list[Toleration] = field(default_factory=list)
    node_affinity: Optional[NodeAffinity] = None
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)

    port: Optional[int] = None
    health_check_path: str = "/"
    tty: bool = False

    status: Optional["PodStatus"] = None

    image_pull_secrets: list[SecretRef] = field(default_factory=list)

    # TODO (A Danshyn 12/09/2018): expose readiness probe properly
    readiness_probe: bool = False

    node_name: Optional[str] = None
    priority_class_name: Optional[str] = None

    created_at: Optional[datetime] = None

    tpu_version_annotation_key: ClassVar[str] = "tf-version.cloud-tpus.google.com"

    restart_policy: PodRestartPolicy = PodRestartPolicy.NEVER

    privileged: bool = False

    @classmethod
    def _process_storage_volumes(
        cls,
        container: Container,
        storage_volume_factory: Optional[
            Callable[[ContainerVolume], Sequence[PathVolume]]
        ] = None,
        storage_volume_mount_factory: Optional[
            Callable[[ContainerVolume, Sequence[PathVolume]], Sequence[VolumeMount]]
        ] = None,
    ) -> tuple[list[PathVolume], list[VolumeMount]]:
        if not storage_volume_factory or not storage_volume_mount_factory:
            return [], []

        result_volumes = []
        result_volume_mounts = []

        for container_volume in container.volumes:
            volumes = storage_volume_factory(container_volume)
            for v in storage_volume_factory(container_volume):
                if v not in result_volumes:
                    result_volumes.append(v)
            volume_mounts = storage_volume_mount_factory(container_volume, volumes)
            for vm in volume_mounts:
                if vm not in result_volume_mounts:
                    result_volume_mounts.append(vm)

        return result_volumes, result_volume_mounts

    @classmethod
    def _process_secret_volumes(
        cls,
        container: Container,
        secret_volume_factory: Optional[Callable[[str], SecretVolume]] = None,
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
        storage_volume_factory: Optional[
            Callable[[ContainerVolume], Sequence[PathVolume]]
        ] = None,
        storage_volume_mount_factory: Optional[
            Callable[[ContainerVolume, Sequence[PathVolume]], Sequence[VolumeMount]]
        ] = None,
        secret_volume_factory: Optional[Callable[[str], SecretVolume]] = None,
        image_pull_secret_names: Optional[list[str]] = None,
        node_selector: Optional[dict[str, str]] = None,
        tolerations: Optional[list[Toleration]] = None,
        node_affinity: Optional[NodeAffinity] = None,
        labels: Optional[dict[str, str]] = None,
        priority_class_name: Optional[str] = None,
        restart_policy: PodRestartPolicy = PodRestartPolicy.NEVER,
        meta_env: Optional[dict[str, str]] = None,
        privileged: bool = False,
    ) -> "PodDescriptor":
        container = job_request.container

        storage_volumes, storage_volume_mounts = cls._process_storage_volumes(
            container, storage_volume_factory, storage_volume_mount_factory
        )
        secret_volumes, secret_volume_mounts = cls._process_secret_volumes(
            container, secret_volume_factory
        )
        disk_volumes, disk_volume_mounts = cls._process_disk_volumes(
            container.disk_volumes
        )

        volumes = [*storage_volumes, *secret_volumes, *disk_volumes]
        volume_mounts = [
            *storage_volume_mounts,
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
            annotations[
                cls.tpu_version_annotation_key
            ] = container.resources.tpu.software_version

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
            labels=labels or {},
            annotations=annotations,
            priority_class_name=priority_class_name,
            restart_policy=restart_policy,
            privileged=privileged,
        )

    @property
    def env_list(self) -> list[dict[str, str]]:
        return [dict(name=name, value=value) for name, value in self.env.items()]

    def can_be_scheduled(self, node_labels: dict[str, str]) -> bool:
        affinities: list[NodeAffinity] = []
        if self.node_selector:
            requirements = [
                NodeSelectorRequirement.create_in(k, v)
                for k, v in self.node_selector.items()
            ]
            affinities.append(NodeAffinity(required=[NodeSelectorTerm(requirements)]))
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

        tolerations = self.tolerations.copy()
        if self.resources and self.resources.gpu:
            tolerations.append(
                Toleration(
                    key=self.resources.gpu_key, operator="Exists", effect="NoSchedule"
                )
            )

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
                    toleration.to_primitive() for toleration in tolerations
                ],
            },
        }
        if self.labels:
            payload["metadata"]["labels"] = self.labels
        if self.annotations:
            payload["metadata"]["annotations"] = self.annotations.copy()
        if self.node_selector:
            payload["spec"]["nodeSelector"] = self.node_selector.copy()
        if self.node_affinity:
            payload["spec"]["affinity"] = {
                "nodeAffinity": self.node_affinity.to_primitive()
            }
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
    def __init__(self, payload: Optional[dict[str, Any]] = None) -> None:
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
    def reason(self) -> Optional[str]:
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
    def message(self) -> Optional[str]:
        for state in self._state.values():
            return state.get("message")
        return None

    @property
    def exit_code(self) -> Optional[int]:
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
    def status(self) -> Optional[bool]:
        val = self._payload["status"]
        if val == "Unknown":
            return None
        elif val == "True":
            return True
        elif val == "False":
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
    def reason(self) -> Optional[str]:
        return self._payload.get("reason", None)

    @property
    def message(self) -> Optional[str]:
        return self._payload.get("message", None)

    @property
    def first_timestamp(self) -> Optional[datetime]:
        event_time = self._payload.get("firstTimestamp") or self._payload.get(
            "deprecatedFirstTimestamp"
        )
        if event_time:
            return iso8601.parse_date(event_time)
        return None

    @property
    def event_time(self) -> Optional[datetime]:
        event_time = self._payload.get("eventTime")
        if event_time:
            return iso8601.parse_date(event_time)
        return None

    @property
    def last_timestamp(self) -> Optional[datetime]:
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
    def reason(self) -> Optional[str]:
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


class ExecChannel(int, enum.Enum):
    STDIN = 0
    STDOUT = 1
    STDERR = 2
    ERROR = 3
    RESIZE = 4


class PodExec:
    RE_EXIT = re.compile(
        rb"^command terminated with non-zero exit code: "
        rb"Error executing in Docker Container: (\d+)$"
    )

    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws
        self._channels: defaultdict[ExecChannel, Stream] = defaultdict(Stream)
        loop = asyncio.get_event_loop()
        self._reader_task = loop.create_task(self._read_data())
        self._exit_code = loop.create_future()

    async def _read_data(self) -> None:
        try:
            async for msg in self._ws:
                if msg.type != WSMsgType.BINARY:
                    # looks weird, but the official client doesn't distinguish TEXT and
                    # BINARY WS messages
                    logger.warning("Unknown pod exec mgs type %r", msg)
                    continue
                data = msg.data
                if isinstance(data, str):
                    bdata = data.encode()
                else:
                    bdata = data
                if not bdata:
                    # an empty WS message. Have no idea how it can happen.
                    continue
                channel = ExecChannel(bdata[0])
                bdata = bdata[1:]
                if channel == ExecChannel.ERROR:
                    match = self.RE_EXIT.match(bdata)
                    if match is not None:
                        # exit code received
                        if not self._exit_code.done():
                            self._exit_code.set_result(int(match.group(1)))
                        continue
                    else:
                        # redirect internal error channel into stderr
                        channel = ExecChannel.STDERR
                stream = self._channels[channel]
                await stream.feed(bdata)

            await self.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("PodExec._read_data")
            await self.close()

    async def close(self) -> None:
        if not self._exit_code.done():
            # Don't have exit status yet, assume a normal termination
            self._exit_code.set_result(0)
        if not self._reader_task.done():
            self._reader_task.cancel()
            for stream in self._channels.values():
                await stream.close()
            with suppress(asyncio.CancelledError):
                await self._reader_task
        await self._ws.close()

    async def wait(self) -> int:
        return await self._exit_code

    async def __aenter__(self) -> "PodExec":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def write_stdin(self, data: bytes) -> None:
        msg = bytes((ExecChannel.STDIN,)) + data
        await self._ws.send_bytes(msg)

    async def read_stdout(self) -> bytes:
        return await self._channels[ExecChannel.STDOUT].read()

    async def read_stderr(self) -> bytes:
        return await self._channels[ExecChannel.STDERR].read()

    async def read_error(self) -> bytes:
        return await self._channels[ExecChannel.ERROR].read()


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
    gpu: int = 0

    gpu_key: ClassVar[str] = "nvidia.com/gpu"

    def __post_init__(self) -> None:
        if self.cpu < 0:
            raise ValueError("Invalid cpu")
        if self.memory < 0:
            raise ValueError("Invalid memory")
        if self.gpu < 0:
            raise ValueError("Invalid gpu")

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "NodeResources":
        return cls(
            cpu=Resources.parse_cpu(payload.get("cpu", "0")),
            memory=Resources.parse_memory(payload.get("memory", "0Mi")),
            gpu=int(payload.get(cls.gpu_key, 0)),
        )

    @property
    def any(self) -> bool:
        return self.cpu_mcores > 0 or self.memory > 0 or self.gpu > 0

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
            and self.gpu >= (r.gpu or 0)
        )

    def __add__(self, other: "NodeResources") -> "NodeResources":
        return self.__class__(
            cpu=self.cpu + other.cpu,
            memory=self.memory + other.memory,
            gpu=self.gpu + other.gpu,
        )

    def __sub__(self, other: "NodeResources") -> "NodeResources":
        return self.__class__(
            cpu=self.cpu - other.cpu,
            memory=self.memory - other.memory,
            gpu=self.gpu - other.gpu,
        )

    def __str__(self) -> str:
        return f"cpu={self.cpu_mcores}m, memory={self.memory}Mi, gpu={self.gpu}"


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
    status: Optional[bool]
    transition_time: datetime
    message: str = ""
    reason: str = ""

    @classmethod
    def from_primitive(cls, payload: dict[str, Any]) -> "NodeCondition":
        return cls(
            type=NodeConditionType.parse(payload["type"]),
            status=cls._parse_status(payload["status"]),
            message=payload.get("message", ""),
            reason=payload.get("reason", ""),
            transition_time=iso8601.parse_date(payload["lastTransitionTime"]),
        )

    @classmethod
    def _parse_status(cls, value: str) -> Optional[bool]:
        if value == "Unknown":
            return None
        elif value == "True":
            return True
        elif value == "False":
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


class KubeClient:
    def __init__(
        self,
        *,
        base_url: str,
        namespace: str,
        cert_authority_path: Optional[str] = None,
        cert_authority_data_pem: Optional[str] = None,
        auth_type: KubeClientAuthType = KubeClientAuthType.CERTIFICATE,
        auth_cert_path: Optional[str] = None,
        auth_cert_key_path: Optional[str] = None,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        conn_timeout_s: int = 300,
        read_timeout_s: int = 100,
        conn_pool_size: int = 100,
        trace_configs: Optional[list[aiohttp.TraceConfig]] = None,
    ) -> None:
        self._base_url = base_url
        self._namespace = namespace

        self._api_resources: APIResources = APIResources()

        self._cert_authority_data_pem = cert_authority_data_pem
        self._cert_authority_path = cert_authority_path

        self._auth_type = auth_type
        self._auth_cert_path = auth_cert_path
        self._auth_cert_key_path = auth_cert_key_path
        self._token = token
        self._token_path = token_path

        self._conn_timeout_s = conn_timeout_s
        self._read_timeout_s = read_timeout_s
        self._conn_pool_size = conn_pool_size
        self._trace_configs = trace_configs

        self._client: Optional[aiohttp.ClientSession] = None

        self._kubelet_port = 10255

    @property
    def _is_ssl(self) -> bool:
        return urlsplit(self._base_url).scheme == "https"

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self._is_ssl:
            return None
        ssl_context = ssl.create_default_context(
            cafile=self._cert_authority_path, cadata=self._cert_authority_data_pem
        )
        if self._auth_type == KubeClientAuthType.CERTIFICATE:
            ssl_context.load_cert_chain(
                self._auth_cert_path,  # type: ignore
                self._auth_cert_key_path,
            )
        return ssl_context

    async def init(self) -> None:
        if self._client:
            return
        connector = aiohttp.TCPConnector(
            limit=self._conn_pool_size, ssl=self._create_ssl_context()
        )
        if self._auth_type == KubeClientAuthType.TOKEN:
            token = self._token
            if not token:
                assert self._token_path is not None
                token = Path(self._token_path).read_text()
            headers = {"Authorization": "Bearer " + token}
        else:
            headers = {}
        timeout = aiohttp.ClientTimeout(
            connect=self._conn_timeout_s, total=self._read_timeout_s
        )
        self._client = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
            trace_configs=self._trace_configs,
        )

    async def init_api_resources(self) -> None:
        assert self._client
        for gv in APIResources.group_versions:
            try:
                self._api_resources[gv] = await self.get_api_resource(gv)
            except aiohttp.ClientResponseError as ex:
                if ex.status != 404:
                    raise

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        await self.init_api_resources()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _reload_http_client(self) -> None:
        await self.close()
        self._token = None
        await self.init()

    @property
    def _api_v1_url(self) -> str:
        return f"{self._base_url}/api/v1"

    @property
    def _apis_networking_v1_url(self) -> str:
        return f"{self._base_url}/apis/networking.k8s.io/v1"

    def _generate_namespace_url(self, namespace_name: str) -> str:
        return f"{self._api_v1_url}/namespaces/{namespace_name}"

    @property
    def _namespace_url(self) -> str:
        return self._generate_namespace_url(self._namespace)

    @property
    def _pods_url(self) -> str:
        return f"{self._namespace_url}/pods"

    @property
    def _all_pods_url(self) -> str:
        return f"{self._api_v1_url}/pods"

    def _generate_pod_url(self, pod_id: str) -> str:
        return f"{self._pods_url}/{pod_id}"

    def _generate_pods_url(self, all_namespaces: bool = False) -> str:
        return self._all_pods_url if all_namespaces else self._pods_url

    def _generate_all_network_policies_url(
        self, namespace_name: Optional[str] = None
    ) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = f"{self._apis_networking_v1_url}/namespaces/{namespace_name}"
        return f"{namespace_url}/networkpolicies"

    def _generate_network_policy_url(
        self, name: str, namespace_name: Optional[str] = None
    ) -> str:
        all_nps_url = self._generate_all_network_policies_url(namespace_name)
        return f"{all_nps_url}/{name}"

    def _generate_endpoint_url(self, name: str, namespace: str) -> str:
        return f"{self._generate_namespace_url(namespace)}/endpoints/{name}"

    @property
    def _nodes_url(self) -> str:
        return f"{self._api_v1_url}/nodes"

    def _generate_node_url(self, name: str) -> str:
        return f"{self._nodes_url}/{name}"

    @property
    def _networking_v1beta1_namespace_url(self) -> str:
        return (
            f"{self._base_url}/apis/networking.k8s.io/v1beta1"
            f"/namespaces/{self._namespace}"
        )

    @property
    def _networking_v1_namespace_url(self) -> str:
        return (
            f"{self._base_url}/apis/networking.k8s.io/v1"
            f"/namespaces/{self._namespace}"
        )

    @property
    def _ingresses_url(self) -> str:
        if self._api_resources.has_networking_v1_ingress:
            return f"{self._networking_v1_namespace_url}/ingresses"
        return f"{self._networking_v1beta1_namespace_url}/ingresses"

    def _generate_ingress_url(self, ingress_name: str) -> str:
        return f"{self._ingresses_url}/{ingress_name}"

    @property
    def _services_url(self) -> str:
        return f"{self._namespace_url}/services"

    def _generate_service_url(self, service_name: str) -> str:
        return f"{self._services_url}/{service_name}"

    def _generate_pod_log_url(self, pod_name: str, container_name: str) -> str:
        return (
            f"{self._generate_pod_url(pod_name)}/log"
            f"?container={pod_name}&follow=true"
        )

    def _generate_all_secrets_url(self, namespace_name: Optional[str] = None) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = self._generate_namespace_url(namespace_name)
        return f"{namespace_url}/secrets"

    def _generate_all_pvcs_url(self, namespace_name: Optional[str] = None) -> str:
        namespace_name = namespace_name or self._namespace
        namespace_url = self._generate_namespace_url(namespace_name)
        return f"{namespace_url}/persistentvolumeclaims"

    def _generate_secret_url(
        self, secret_name: str, namespace_name: Optional[str] = None
    ) -> str:
        all_secrets_url = self._generate_all_secrets_url(namespace_name)
        return f"{all_secrets_url}/{secret_name}"

    def _generate_pvc_url(
        self, pvc_name: str, namespace_name: Optional[str] = None
    ) -> str:
        all_pvcs_url = self._generate_all_pvcs_url(namespace_name)
        return f"{all_pvcs_url}/{pvc_name}"

    async def _request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        assert self._client
        doing_retry = kwargs.pop("doing_retry", False)

        async with self._client.request(*args, **kwargs) as response:
            payload = await response.json()
            logger.debug("k8s response payload: %s", payload)
        try:
            self._check_status_payload(payload)
        except KubeClientUnauthorizedException:
            if doing_retry:
                raise
            await self._reload_http_client()
            kwargs["doing_retry"] = True
            payload = await self._request(*args, **kwargs)
        return payload

    async def _watch(
        self,
        url: str,
        params: Optional[dict[str, str]] = None,
        resource_version: Optional[str] = None,
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
        params = params or {}
        params.update(watch="true", allowWatchBookmarks="true")
        if resource_version:
            params["resourceVersion"] = resource_version
        assert self._client
        async with self._client.get(
            url, params=params, timeout=aiohttp.ClientTimeout()
        ) as response:
            if response.status == 410:
                raise ResourceGoneException
            async for line in response.content:
                payload = json.loads(line)
                if WatchEvent.is_error(payload):
                    self._check_status_payload(payload["object"])
                if WatchBookmarkEvent.is_bookmark(payload):
                    yield WatchBookmarkEvent.from_primitive(payload)
                else:
                    yield WatchEvent.from_primitive(payload)

    async def get_api_resource(self, group_version: str) -> APIResource:
        url = f"{self._base_url}/apis/{group_version}"
        payload = await self._request(method="GET", url=url, raise_for_status=True)
        return APIResource.from_primitive(payload)

    async def _delete_resource_url(self, url: str, uid: Optional[str] = None) -> None:
        request_payload = None
        if uid:
            request_payload = {"preconditions": {"uid": uid}}
        payload = await self._request(method="DELETE", url=url, json=request_payload)
        if (
            uid
            and payload["kind"] == "Status"
            and payload["status"] == "Failure"
            and payload["reason"] == "Conflict"
        ):
            raise NotFoundException(payload["reason"])

    async def get_endpoint(
        self, name: str, namespace: Optional[str] = None
    ) -> dict[str, Any]:
        url = self._generate_endpoint_url(name, namespace or self._namespace)
        return await self._request(method="GET", url=url)

    async def create_node(
        self,
        name: str,
        capacity: dict[str, Any],
        labels: Optional[dict[str, str]] = None,
        taints: Optional[Sequence[NodeTaint]] = None,
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
        await self._request(method="POST", url=url, json=payload)

    async def delete_node(self, name: str) -> None:
        url = self._generate_node_url(name)
        await self._delete_resource_url(url)

    async def get_nodes(self) -> Sequence[Node]:
        payload = await self._request(method="GET", url=self._nodes_url)
        assert payload["kind"] == "NodeList"
        nodes = []
        for item in payload["items"]:
            nodes.append(Node.from_primitive(item))
        return nodes

    async def get_node(self, name: str) -> Node:
        payload = await self._request(method="GET", url=self._generate_node_url(name))
        assert payload["kind"] == "Node"
        return Node.from_primitive(payload)

    async def get_raw_nodes(
        self, labels: Optional[dict[str, str]] = None
    ) -> ListResult:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        payload = await self._request(method="GET", url=self._nodes_url, params=params)
        assert payload["kind"] == "NodeList"
        return ListResult.from_primitive(payload)

    async def watch_nodes(
        self,
        labels: Optional[dict[str, str]] = None,
        resource_version: Optional[str] = None,
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        async for event in self._watch(self._nodes_url, params, resource_version):
            yield event

    async def create_pod(self, descriptor: PodDescriptor) -> PodDescriptor:
        payload = await self._request(
            method="POST", url=self._pods_url, json=descriptor.to_primitive()
        )
        pod = PodDescriptor.from_primitive(payload)
        return pod

    async def set_raw_pod_status(
        self, name: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        url = self._generate_pod_url(name) + "/status"
        return await self._request(method="PUT", url=url, json=payload)

    async def get_pod(self, pod_name: str) -> PodDescriptor:
        url = self._generate_pod_url(pod_name)
        payload = await self._request(method="GET", url=url)
        return PodDescriptor.from_primitive(payload)

    async def get_raw_pod(self, name: str) -> dict[str, Any]:
        url = self._generate_pod_url(name)
        return await self._request(method="GET", url=url)

    async def get_raw_pods(self, all_namespaces: bool = False) -> ListResult:
        url = self._generate_pods_url(all_namespaces)
        payload = await self._request(method="GET", url=url)
        assert payload["kind"] == "PodList"
        return ListResult.from_primitive(payload)

    async def watch_pods(
        self, all_namespaces: bool = False, resource_version: Optional[str] = None
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
        url = self._generate_pods_url(all_namespaces)
        async for event in self._watch(url, resource_version=resource_version):
            yield event

    async def get_pod_status(self, pod_id: str) -> PodStatus:
        pod = await self.get_pod(pod_id)
        if pod.status is None:
            raise ValueError("Missing pod status")
        return pod.status

    async def delete_pod(self, pod_name: str, force: bool = False) -> PodStatus:
        url = self._generate_pod_url(pod_name)
        request_payload = None
        if force:
            request_payload = {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "gracePeriodSeconds": 0,
            }
        payload = await self._request(method="DELETE", url=url, json=request_payload)
        pod = PodDescriptor.from_primitive(payload)
        return pod.status  # type: ignore

    async def delete_all_pods(self, *, labels: dict[str, str]) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self._request(method="DELETE", url=self._pods_url, params=params)

    async def create_ingress(
        self,
        name: str,
        ingress_class: Optional[str] = None,
        rules: Optional[list[IngressRule]] = None,
        annotations: Optional[dict[str, str]] = None,
        labels: Optional[dict[str, str]] = None,
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
        payload = await self._request(
            method="POST", url=self._ingresses_url, json=payload
        )
        return Ingress.from_primitive(payload)

    async def get_ingress(self, name: str) -> Ingress:
        url = self._generate_ingress_url(name)
        payload = await self._request(method="GET", url=url)
        return Ingress.from_primitive(payload)

    async def delete_all_ingresses(
        self, *, labels: Optional[dict[str, str]] = None
    ) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self._request(method="DELETE", url=self._ingresses_url, params=params)

    async def delete_ingress(self, name: str) -> None:
        url = self._generate_ingress_url(name)
        await self._request(method="DELETE", url=url)

    def _check_status_payload(self, payload: dict[str, Any]) -> None:
        if payload.get("kind") == "Status":
            if payload["status"] == "Failure":
                reason = payload.get("reason")
                if reason == "AlreadyExists":
                    raise AlreadyExistsException(payload["reason"])
                if reason == "NotFound":
                    raise NotFoundException(payload["reason"])
                if reason == "Gone":
                    raise ResourceGoneException(payload["reason"])
                if reason == "Unauthorized":
                    raise KubeClientUnauthorizedException(payload["reason"])
                raise StatusException(payload["reason"])

    async def add_ingress_rule(self, name: str, rule: IngressRule) -> Ingress:
        # TODO (A Danshyn 06/13/18): test if does not exist already
        url = self._generate_ingress_url(name)
        headers = {"Content-Type": "application/json-patch+json"}
        if self._api_resources.has_networking_v1_ingress:
            rule_payload = rule.to_v1_primitive()
        else:
            rule_payload = rule.to_v1beta1_primitive()
        patches = [{"op": "add", "path": "/spec/rules/-", "value": rule_payload}]
        payload = await self._request(
            method="PATCH", url=url, headers=headers, json=patches
        )
        return Ingress.from_primitive(payload)

    async def remove_ingress_rule(self, name: str, host: str) -> Ingress:
        # TODO (A Danshyn 06/13/18): this one should have a retry in case of
        # a race condition
        ingress = await self.get_ingress(name)
        rule_index = ingress.find_rule_index_by_host(host)
        if rule_index < 0:
            raise StatusException("NotFound")
        url = self._generate_ingress_url(name)
        rule = [
            {"op": "test", "path": f"/spec/rules/{rule_index}/host", "value": host},
            {"op": "remove", "path": f"/spec/rules/{rule_index}"},
        ]
        headers = {"Content-Type": "application/json-patch+json"}
        payload = await self._request(
            method="PATCH", url=url, headers=headers, json=rule
        )
        return Ingress.from_primitive(payload)

    async def create_service(self, service: Service) -> Service:
        url = self._services_url
        payload = await self._request(
            method="POST", url=url, json=service.to_primitive()
        )
        return Service.from_primitive(payload)

    async def get_service(self, name: str) -> Service:
        url = self._generate_service_url(name)
        payload = await self._request(method="GET", url=url)
        return Service.from_primitive(payload)

    async def list_services(self, labels: dict[str, str]) -> list[Service]:
        url = self._services_url
        labelSelector = ",".join(f"{label}={value}" for label, value in labels.items())
        payload = await self._request(
            method="GET", url=url, params={"labelSelector": labelSelector}
        )
        return [Service.from_primitive(item) for item in payload["items"]]

    async def delete_service(self, name: str, uid: Optional[str] = None) -> None:
        url = self._generate_service_url(name)
        await self._delete_resource_url(url, uid)

    async def delete_all_services(
        self, *, labels: Optional[dict[str, str]] = None
    ) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self._request(method="DELETE", url=self._services_url, params=params)

    async def create_docker_secret(self, secret: DockerRegistrySecret) -> None:
        url = self._generate_all_secrets_url(secret.namespace)
        await self._request(method="POST", url=url, json=secret.to_primitive())

    async def update_docker_secret(
        self, secret: DockerRegistrySecret, create_non_existent: bool = False
    ) -> None:
        try:
            url = self._generate_secret_url(secret.name, secret.namespace)
            await self._request(method="PUT", url=url, json=secret.to_primitive())
        except StatusException as exc:
            if exc.args[0] != "NotFound" or not create_non_existent:
                raise

            await self.create_docker_secret(secret)

    async def get_raw_secret(
        self, secret_name: str, namespace_name: Optional[str] = None
    ) -> dict[str, Any]:
        url = self._generate_secret_url(secret_name, namespace_name)
        return await self._request(method="GET", url=url)

    async def delete_secret(
        self, secret_name: str, namespace_name: Optional[str] = None
    ) -> None:
        url = self._generate_secret_url(secret_name, namespace_name)
        await self._delete_resource_url(url)

    async def get_raw_pvc(
        self, pvc_name: str, namespace_name: Optional[str] = None
    ) -> dict[str, Any]:
        url = self._generate_pvc_url(pvc_name, namespace_name)
        return await self._request(method="GET", url=url)

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
        url = f"{self._api_v1_url}/namespaces/{namespace}/events"
        payload = await self._request(method="GET", url=url, params=params)
        return [KubernetesEvent(item) for item in payload.get("items", [])]

    async def exec_pod(
        self, pod_id: str, command: Union[str, Iterable[str]], *, tty: bool
    ) -> PodExec:
        url = URL(self._generate_pod_url(pod_id)) / "exec"
        s_tty = str(int(tty))  # 0 or 1
        args = MultiDict(
            {
                "container": pod_id,
                "tty": s_tty,
                "stdin": "1",
                "stdout": "1",
                "stderr": "1",
            }
        )
        if isinstance(command, str):
            args["command"] = command
        else:
            for part in command:
                args.add("command", part)

        url = url.with_query(args)
        ws = await self._client.ws_connect(url, method="POST")  # type: ignore
        return PodExec(ws)

    async def wait_pod_is_running(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        """Wait until the pod transitions from the waiting state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(pod_name)
                if not pod_status.is_waiting:
                    return
                await asyncio.sleep(interval_s)

    async def wait_pod_is_terminated(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        """Wait until the pod transitions to the terminated state.

        Raise JobError if there is no such pod.
        Raise asyncio.TimeoutError if it takes too long for the pod.
        """
        async with timeout(timeout_s):
            while True:
                pod_status = await self.get_pod_status(pod_name)
                if pod_status.is_terminated:
                    return
                await asyncio.sleep(interval_s)

    async def wait_pod_is_deleted(
        self, pod_name: str, timeout_s: float = 10.0 * 60, interval_s: float = 1.0
    ) -> None:
        async with timeout(timeout_s):
            while True:
                try:
                    await self.get_pod(pod_name)
                    await asyncio.sleep(interval_s)
                except JobNotFoundException:
                    return

    async def create_default_network_policy(
        self,
        name: str,
        pod_labels: dict[str, str],
        namespace_name: Optional[str] = None,
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
            # allowing labeled pods to connect to each other
            {"to": [{"podSelector": {"matchLabels": pod_labels}}]},
        ]
        return await self.create_egress_network_policy(
            name, pod_labels=pod_labels, rules=rules, namespace_name=namespace_name
        )

    async def create_egress_network_policy(
        self,
        name: str,
        *,
        pod_labels: dict[str, str],
        rules: list[dict[str, Any]],
        namespace_name: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
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
        url = self._generate_all_network_policies_url(namespace_name)
        return await self._request(method="POST", url=url, json=request_payload)

    async def get_network_policy(
        self, name: str, namespace_name: Optional[str] = None
    ) -> dict[str, Any]:
        url = self._generate_network_policy_url(name, namespace_name)
        return await self._request(method="GET", url=url)

    async def delete_network_policy(
        self, name: str, namespace_name: Optional[str] = None
    ) -> None:
        url = self._generate_network_policy_url(name, namespace_name)
        await self._delete_resource_url(url)

    async def delete_all_network_policies(
        self,
        *,
        namespace_name: Optional[str] = None,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        params: dict[str, str] = {}
        if labels:
            params["labelSelector"] = ",".join(
                "=".join(item) for item in labels.items()
            )
        await self._request(
            method="DELETE",
            url=self._generate_all_network_policies_url(namespace_name),
            params=params,
        )

    def _generate_node_proxy_url(self, name: str, port: int) -> str:
        return f"{self._api_v1_url}/nodes/{name}:{port}/proxy"

    def _generate_node_stats_summary_url(self, name: str) -> str:
        proxy_url = self._generate_node_proxy_url(name, self._kubelet_port)
        return f"{proxy_url}/stats/summary"


class EventHandler:
    @abc.abstractmethod
    async def init(self, resources: list[dict[str, Any]]) -> None:
        pass

    @abc.abstractmethod
    async def handle(self, event: WatchEvent) -> None:
        pass


class Watcher(abc.ABC):
    def __init__(self, kube_client: KubeClient) -> None:
        self._kube_client = kube_client
        self._handlers: list[EventHandler] = []
        self._watcher_task: Optional[asyncio.Task[None]] = None

    def subscribe(self, handler: EventHandler) -> None:
        if self._watcher_task is not None:
            raise Exception("Subscription is not possible after watcher start")
        self._handlers.append(handler)

    async def __aenter__(self) -> "Watcher":
        await self.start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        result = await self.list()
        for handler in self._handlers:
            await handler.init(result.items)
        self._watcher_task = asyncio.create_task(self._run(result.resource_version))

    async def stop(self) -> None:
        if self._watcher_task is None:
            return
        self._watcher_task.cancel()
        with suppress(asyncio.CancelledError):
            await self._watcher_task
        self._watcher_task = None
        self._handlers.clear()

    async def _run(self, resource_version: str) -> None:
        while True:
            try:
                async for event in self.watch(resource_version):
                    if isinstance(event, WatchBookmarkEvent):
                        resource_version = event.resource_version
                        continue
                    for handler in self._handlers:
                        await handler.handle(event)
            except ResourceGoneException as exc:
                logger.warning("Resource gone", exc_info=exc)
            except aiohttp.ClientError as exc:
                logger.warning("Watch client error", exc_info=exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Unhandled error", exc_info=exc)

    @abc.abstractmethod
    async def list(self) -> ListResult:
        pass

    @abc.abstractmethod
    async def watch(
        self, resource_version: str
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
        yield  # type: ignore


class NodeWatcher(Watcher):
    def __init__(
        self, kube_client: KubeClient, labels: Optional[dict[str, str]] = None
    ) -> None:
        super().__init__(kube_client)
        self._kwargs = {"labels": labels}

    async def list(self) -> ListResult:
        return await self._kube_client.get_raw_nodes(**self._kwargs)

    async def watch(
        self, resource_version: str
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
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
    ) -> AsyncIterator[Union[WatchEvent, WatchBookmarkEvent]]:
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
        if resources.gpu:
            pods = [p for p in pods if p.resources and p.resources.gpu]
        pods_to_preempt: list[PodDescriptor] = []
        while pods and resources.any:
            logger.debug("Pods left: %d", len(pods))
            logger.debug("Resources left: %s", resources)
            #  max distance for a single resource is 1, 3 resources total
            best_dist = 4.0
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
                        "Chose new best pod: name=%s, dist=%f", pods[i].name, dist
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
            resources.gpu or 0,
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
            gpu=max(0, (r1.gpu or 0) - (r2.gpu or 0)),
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
        if resources.gpu:
            dist += _dist(resources.gpu or 0, pod.resources.gpu or 0)
        return dist

    @classmethod
    def _has_less_resources(cls, r1: Resources, r2: Resources) -> bool:
        return ((r1.gpu or 0), r1.memory, r1.cpu_mcores) < (
            (r2.gpu or 0),
            r2.memory,
            r2.cpu_mcores,
        )
