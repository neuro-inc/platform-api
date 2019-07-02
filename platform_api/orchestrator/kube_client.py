import abc
import asyncio
import enum
import json
import logging
import re
import ssl
from base64 import b64encode
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path, PurePath
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    DefaultDict,
    Dict,
    Iterable,
    List,
    NoReturn,
    Optional,
    Sequence,
    Type,
    Union,
)
from urllib.parse import urlsplit

import aiohttp
import iso8601
from aiohttp import ContentTypeError, WSMsgType
from async_generator import asynccontextmanager
from async_timeout import timeout
from multidict import MultiDict
from yarl import URL

from platform_api.utils.stream import Stream

from .job_request import (
    ContainerResources,
    ContainerVolume,
    JobError,
    JobNotFoundException,
    JobRequest,
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


def _raise_status_job_exception(pod: Dict[str, Any], job_id: Optional[str]) -> NoReturn:
    if pod["code"] == 409:
        raise JobError(f"job with {job_id} already exist")
    elif pod["code"] == 404:
        raise JobNotFoundException(f"job {job_id} was not found")
    elif pod["code"] == 422:
        raise JobError(f"cant create job with id {job_id}")
    else:
        raise JobError("unexpected")


@dataclass(frozen=True)
class Volume(metaclass=abc.ABCMeta):
    name: str

    def create_mount(self, container_volume: ContainerVolume) -> "VolumeMount":
        raise NotImplementedError("Cannot create mount for abstract Volume type.")

    def to_primitive(self) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True)
class PathVolume(Volume):
    path: PurePath

    def create_mount(self, container_volume: ContainerVolume) -> "VolumeMount":
        sub_path = container_volume.src_path.relative_to(self.path)
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path,
            sub_path=sub_path,
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class HostVolume(PathVolume):
    def to_primitive(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "hostPath": {"path": str(self.path), "type": "Directory"},
        }


@dataclass(frozen=True)
class SharedMemoryVolume(Volume):
    def to_primitive(self) -> Dict[str, Any]:
        return {"name": self.name, "emptyDir": {"medium": "Memory"}}

    def create_mount(self, container_volume: ContainerVolume) -> "VolumeMount":
        return VolumeMount(
            volume=self,
            mount_path=container_volume.dst_path,
            sub_path=PurePath(""),
            read_only=container_volume.read_only,
        )


@dataclass(frozen=True)
class NfsVolume(PathVolume):
    server: str

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "nfs": {"server": self.server, "path": str(self.path)},
        }


@dataclass(frozen=True)
class VolumeMount:
    volume: Volume
    mount_path: PurePath
    sub_path: PurePath = PurePath("")
    read_only: bool = False

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "name": self.volume.name,
            "mountPath": str(self.mount_path),
            "readOnly": self.read_only,
            "subPath": str(self.sub_path),
        }


@dataclass(frozen=True)
class Resources:
    cpu: float
    memory: int
    gpu: Optional[int] = None
    shm: Optional[bool] = None

    gpu_key: ClassVar[str] = "nvidia.com/gpu"

    @property
    def cpu_mcores(self) -> str:
        mcores = int(self.cpu * 1000)
        return f"{mcores}m"

    @property
    def memory_mib(self) -> str:
        return f"{self.memory}Mi"

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "limits": {"cpu": self.cpu_mcores, "memory": self.memory_mib}
        }
        if self.gpu:
            payload["limits"][self.gpu_key] = self.gpu
        return payload

    @classmethod
    def from_container_resources(cls, resources: ContainerResources) -> "Resources":
        return cls(
            cpu=resources.cpu,
            memory=resources.memory_mb,
            gpu=resources.gpu,
            shm=resources.shm,
        )


@dataclass(frozen=True)
class Service:
    name: str
    target_port: Optional[int]
    ssh_target_port: Optional[int] = None

    port: int = 80
    ssh_port: int = 22

    service_type: ServiceType = ServiceType.CLUSTER_IP

    cluster_ip: Optional[str] = None

    def _add_port_map(
        self,
        port: Optional[int],
        target_port: Optional[int],
        port_name: str,
        ports: List[Dict[str, Any]],
    ) -> None:
        if target_port:
            ports.append({"port": port, "targetPort": target_port, "name": port_name})

    def to_primitive(self) -> Dict[str, Any]:
        service_descriptor = {
            "metadata": {"name": self.name},
            "spec": {
                "type": self.service_type.value,
                "ports": [],
                "selector": {"job": self.name},
            },
        }

        if self.cluster_ip:
            service_descriptor["spec"]["clusterIP"] = self.cluster_ip

        self._add_port_map(
            self.port, self.target_port, "http", service_descriptor["spec"]["ports"]
        )
        self._add_port_map(
            self.ssh_port,
            self.ssh_target_port,
            "ssh",
            service_descriptor["spec"]["ports"],
        )
        return service_descriptor

    @classmethod
    def create_for_pod(cls, pod: "PodDescriptor") -> "Service":
        return cls(pod.name, target_port=pod.port, ssh_target_port=pod.ssh_port)

    @classmethod
    def create_headless_for_pod(cls, pod: "PodDescriptor") -> "Service":
        http_port = pod.port or cls.port
        return cls(
            name=pod.name,
            cluster_ip="None",
            target_port=http_port,
            ssh_target_port=pod.ssh_port,
        )

    @classmethod
    def _find_port_by_name(
        cls, name: str, port_mappings: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        for port_mapping in port_mappings:
            if port_mapping.get("name", None) == name:
                return port_mapping
        return {}

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "Service":
        http_payload = cls._find_port_by_name("http", payload["spec"]["ports"])
        ssh_payload = cls._find_port_by_name("ssh", payload["spec"]["ports"])
        service_type = payload["spec"].get("type", Service.service_type.value)
        return cls(
            name=payload["metadata"]["name"],
            target_port=http_payload.get("targetPort", None),
            port=http_payload.get("port", Service.port),
            ssh_target_port=ssh_payload.get("targetPort", None),
            ssh_port=ssh_payload.get("port", Service.ssh_port),
            service_type=ServiceType(service_type),
            cluster_ip=payload["spec"].get("clusterIP"),
        )


@dataclass(frozen=True)
class IngressRule:
    host: str
    service_name: Optional[str] = None
    service_port: Optional[int] = None

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "IngressRule":
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

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"host": self.host}
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

    @classmethod
    def from_service(cls, host: str, service: Service) -> "IngressRule":
        return cls(host=host, service_name=service.name, service_port=service.port)


@dataclass(frozen=True)
class Ingress:
    name: str
    rules: List[IngressRule] = field(default_factory=list)

    def to_primitive(self) -> Dict[str, Any]:
        rules: List[Any] = [rule.to_primitive() for rule in self.rules] or [None]
        return {"metadata": {"name": self.name}, "spec": {"rules": rules}}

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "Ingress":
        # TODO (A Danshyn 06/13/18): should be refactored along with PodStatus
        kind = payload["kind"]
        if kind == "Ingress":
            rules = [
                IngressRule.from_primitive(rule) for rule in payload["spec"]["rules"]
            ]
            return cls(name=payload["metadata"]["name"], rules=rules)
        elif kind == "Status":
            _raise_status_job_exception(payload, job_id=None)
        else:
            raise ValueError(f"unknown kind: {kind}")

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

    def to_primitive(self) -> Dict[str, Any]:
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

    def to_primitive(self) -> Dict[str, str]:
        return {"name": self.name}

    @classmethod
    def from_primitive(cls, payload: Dict[str, str]) -> "SecretRef":
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

    def to_primitive(self) -> Dict[str, Any]:
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


@dataclass(frozen=True)
class NodeSelectorRequirement:
    key: str
    operator: NodeSelectorOperator
    values: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("blank key")
        if self.operator.requires_no_values and self.values:
            raise ValueError("values must be empty")

    @classmethod
    def create_exists(cls, key: str) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.EXISTS)

    @classmethod
    def create_does_not_exist(cls, key: str) -> "NodeSelectorRequirement":
        return cls(key=key, operator=NodeSelectorOperator.DOES_NOT_EXIST)

    def to_primitive(self) -> Dict[str, Any]:
        payload = {"key": self.key, "operator": self.operator.value}
        if self.values:
            payload["values"] = self.values.copy()
        return payload


@dataclass(frozen=True)
class NodeSelectorTerm:
    match_expressions: List[NodeSelectorRequirement]

    def __post_init__(self) -> None:
        if not self.match_expressions:
            raise ValueError("no expressions")

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "matchExpressions": [expr.to_primitive() for expr in self.match_expressions]
        }


@dataclass(frozen=True)
class NodePreferredSchedulingTerm:
    preference: NodeSelectorTerm
    weight: int = 100

    def to_primitive(self) -> Dict[str, Any]:
        return {"preference": self.preference.to_primitive(), "weight": self.weight}


@dataclass(frozen=True)
class NodeAffinity:
    required: List[NodeSelectorTerm] = field(default_factory=list)
    preferred: List[NodePreferredSchedulingTerm] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.required and not self.preferred:
            raise ValueError("no terms")

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
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
class PodDescriptor:
    name: str
    image: str
    args: List[str] = field(default_factory=list)
    env: Dict[str, str] = field(default_factory=dict)
    volume_mounts: List[VolumeMount] = field(default_factory=list)
    volumes: List[Volume] = field(default_factory=list)
    resources: Optional[Resources] = None
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Toleration] = field(default_factory=list)
    node_affinity: Optional[NodeAffinity] = None
    labels: Dict[str, str] = field(default_factory=dict)

    port: Optional[int] = None
    ssh_port: Optional[int] = None
    health_check_path: str = "/"

    status: Optional["PodStatus"] = None

    image_pull_secrets: List[SecretRef] = field(default_factory=list)

    # TODO (A Danshyn 12/09/2018): expose readiness probe properly
    readiness_probe: bool = False

    node_name: Optional[str] = None

    created_at: Optional[datetime] = None

    @classmethod
    def from_job_request(
        cls,
        volume: Volume,
        job_request: JobRequest,
        secret_names: Optional[List[str]] = None,
        node_selector: Optional[Dict[str, str]] = None,
        tolerations: Optional[List[Toleration]] = None,
        node_affinity: Optional[NodeAffinity] = None,
        labels: Optional[Dict[str, str]] = None,
    ) -> "PodDescriptor":
        container = job_request.container
        volume_mounts = [
            volume.create_mount(container_volume)
            for container_volume in container.volumes
        ]
        volumes = [volume]

        if job_request.container.resources.shm:
            dev_shm_volume = SharedMemoryVolume(name="dshm")
            container_volume = ContainerVolume(
                URL(""),
                dst_path=PurePath("/dev/shm"),
                src_path=PurePath(""),
                read_only=False,
            )
            volume_mounts.append(dev_shm_volume.create_mount(container_volume))
            volumes.append(dev_shm_volume)

        resources = Resources.from_container_resources(container.resources)
        if secret_names is not None:
            image_pull_secrets = [SecretRef(name) for name in secret_names]
        else:
            image_pull_secrets = []
        return cls(
            name=job_request.job_id,
            image=container.image,
            args=container.command_list,
            env=container.env.copy(),
            volume_mounts=volume_mounts,
            volumes=volumes,
            resources=resources,
            port=container.port,
            ssh_port=container.ssh_port,
            health_check_path=container.health_check_path,
            image_pull_secrets=image_pull_secrets,
            node_selector=node_selector or {},
            tolerations=tolerations or [],
            node_affinity=node_affinity,
            labels=labels or {},
        )

    @property
    def env_list(self) -> List[Dict[str, str]]:
        return [dict(name=name, value=value) for name, value in self.env.items()]

    def to_primitive(self) -> Dict[str, Any]:
        volume_mounts = [mount.to_primitive() for mount in self.volume_mounts]
        volumes = [volume.to_primitive() for volume in self.volumes]

        container_payload = {
            "name": f"{self.name}",
            "image": f"{self.image}",
            "env": self.env_list,
            "volumeMounts": volume_mounts,
            "terminationMessagePolicy": "FallbackToLogsOnError",
        }
        if self.args:
            container_payload["args"] = self.args
        if self.resources:
            container_payload["resources"] = self.resources.to_primitive()

        ports = self._to_primitive_ports()
        if ports:
            container_payload["ports"] = ports
        readiness_probe = self._to_primitive_readiness_probe()
        if readiness_probe:
            container_payload["readinessProbe"] = readiness_probe

        labels = self.labels.copy()
        # TODO (A Danshyn 12/04/18): the job is left for backward
        # compatibility
        labels["job"] = self.name

        tolerations = self.tolerations.copy()
        if self.resources and self.resources.gpu:
            tolerations.append(
                Toleration(
                    key=self.resources.gpu_key, operator="Exists", effect="NoSchedule"
                )
            )

        payload: Dict[str, Any] = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {"name": self.name, "labels": labels},
            "spec": {
                "automountServiceAccountToken": False,
                "containers": [container_payload],
                "volumes": volumes,
                "restartPolicy": "Never",
                "imagePullSecrets": [
                    secret.to_primitive() for secret in self.image_pull_secrets
                ],
                "tolerations": [
                    toleration.to_primitive() for toleration in tolerations
                ],
            },
        }
        if self.node_selector:
            payload["spec"]["nodeSelector"] = self.node_selector.copy()
        if self.node_affinity:
            payload["spec"]["affinity"] = {
                "nodeAffinity": self.node_affinity.to_primitive()
            }
        return payload

    def _to_primitive_ports(self) -> List[Dict[str, int]]:
        ports = []
        if self.port:
            ports.append({"containerPort": self.port})
        if self.ssh_port:
            ports.append({"containerPort": self.ssh_port})
        return ports

    def _to_primitive_readiness_probe(self) -> Dict[str, Any]:
        if not self.readiness_probe:
            return {}

        if self.port:
            return {
                "httpGet": {"port": self.port, "path": self.health_check_path},
                "initialDelaySeconds": 1,
                "periodSeconds": 1,
            }

        if self.ssh_port:
            return {
                "tcpSocket": {"port": self.ssh_port},
                "initialDelaySeconds": 1,
                "periodSeconds": 1,
            }

        return {}

    @classmethod
    def _assert_resource_kind(cls, expected_kind: str, payload: Dict[str, Any]) -> None:
        kind = payload["kind"]
        if kind == "Status":
            _raise_status_job_exception(payload, job_id="")
        elif kind != expected_kind:
            raise ValueError(f"unknown kind: {kind}")

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PodDescriptor":
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
        return cls(
            name=metadata["name"],
            created_at=iso8601.parse_date(metadata["creationTimestamp"]),
            image=container_payload["image"],
            status=status,
            image_pull_secrets=secrets,
            node_name=payload["spec"].get("nodeName"),
        )


class ContainerStatus:
    def __init__(self, payload: Optional[Dict[str, Any]] = None) -> None:
        self._payload = payload or {}

    @property
    def _state(self) -> Dict[str, Any]:
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

    def __init__(self, payload: Dict[str, Any]) -> None:
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
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload or {}

    @property
    def involved_object(self) -> Dict[str, str]:
        return self._payload["involvedObject"]

    @property
    def reason(self) -> Optional[str]:
        return self._payload.get("reason", None)

    @property
    def first_timestamp(self) -> datetime:
        return iso8601.parse_date(self._payload["firstTimestamp"])

    @property
    def last_timestamp(self) -> datetime:
        return iso8601.parse_date(self._payload["lastTimestamp"])

    @property
    def count(self) -> int:
        return self._payload["count"]


class PodStatus:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload
        self._container_status = self._init_container_status()

    def _init_container_status(self) -> ContainerStatus:
        payload = None
        if "containerStatuses" in self._payload:
            payload = self._payload["containerStatuses"][0]
        return ContainerStatus(payload=payload)

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
    def reason(self) -> Optional[str]:
        """

        If kubelet decides to evict the pod, it sets the "Failed" phase along with
        the "Evicted" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/kubelet/eviction/eviction_manager.go#L543-L566
        If a node the pod scheduled on fails, node lifecycle controller sets
        the "NodeList" reason.
        https://github.com/kubernetes/kubernetes/blob/a3ccea9d8743f2ff82e41b6c2af6dc2c41dc7b10/pkg/controller/util/node/controller_utils.go#L109-L126
        """
        # the pod status reason has a greater priority
        return self._payload.get("reason") or self._container_status.reason

    @property
    def message(self) -> Optional[str]:
        return self._payload.get("message") or self._container_status.message

    @property
    def container_status(self) -> ContainerStatus:
        return self._container_status

    @property
    def is_container_creating(self) -> bool:
        return self._container_status.is_creating

    @property
    def is_node_lost(self) -> bool:
        return self.reason == "NodeLost"

    @property
    def conditions(self) -> List[PodCondition]:
        return [PodCondition(val) for val in self._payload.get("conditions", [])]

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PodStatus":
        return cls(payload)


class ExecChannel(int, enum.Enum):
    STDIN = 0
    STDOUT = 1
    STDERR = 2
    ERROR = 3
    RESIZE = 4


class PodExec:
    RE_EXIT = re.compile(
        br"^command terminated with non-zero exit code: "
        br"Error executing in Docker Container: (\d+)$"
    )

    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws
        self._channels: DefaultDict[ExecChannel, Stream] = DefaultDict(Stream)
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
        exc_type: Optional[Type[BaseException]],
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

    def to_primitive(self) -> Dict[str, Any]:
        return {"key": self.key, "value": self.value, "effect": self.effect}


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
    ) -> None:
        self._base_url = base_url
        self._namespace = namespace

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
            ssl_context.load_cert_chain(  # type: ignore
                self._auth_cert_path, self._auth_cert_key_path
            )
        return ssl_context

    async def init(self) -> None:
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
            connector=connector, timeout=timeout, headers=headers
        )

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "KubeClient":
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

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

    def _generate_pod_url(self, pod_id: str) -> str:
        return f"{self._pods_url}/{pod_id}"

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
    def _v1beta1_namespace_url(self) -> str:
        return (
            f"{self._base_url}/apis/extensions/v1beta1" f"/namespaces/{self._namespace}"
        )

    @property
    def _ingresses_url(self) -> str:
        return f"{self._v1beta1_namespace_url}/ingresses"

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

    def _generate_secret_url(
        self, secret_name: str, namespace_name: Optional[str] = None
    ) -> str:
        all_secrets_url = self._generate_all_secrets_url(namespace_name)
        return f"{all_secrets_url}/{secret_name}"

    async def _request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        assert self._client
        async with self._client.request(*args, **kwargs) as response:
            # TODO (A Danshyn 05/21/18): check status code etc
            payload = await response.json()
            logging.debug("k8s response payload: %s", payload)
            return payload

    async def get_endpoint(
        self, name: str, namespace: Optional[str] = None
    ) -> Dict[str, Any]:
        url = self._generate_endpoint_url(name, namespace or self._namespace)
        return await self._request(method="GET", url=url)

    async def create_node(
        self,
        name: str,
        capacity: Dict[str, Any],
        labels: Optional[Dict[str, str]] = None,
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
        result = await self._request(method="POST", url=url, json=payload)
        self._check_status_payload(result)

    async def delete_node(self, name: str) -> None:
        url = self._generate_node_url(name)
        result = await self._request(method="DELETE", url=url)
        self._check_status_payload(result)

    async def create_pod(self, descriptor: PodDescriptor) -> PodDescriptor:
        payload = await self._request(
            method="POST", url=self._pods_url, json=descriptor.to_primitive()
        )
        pod = PodDescriptor.from_primitive(payload)
        return pod

    async def set_raw_pod_status(
        self, name: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = self._generate_pod_url(name) + "/status"
        return await self._request(method="PUT", url=url, json=payload)

    async def get_pod(self, pod_name: str) -> PodDescriptor:
        url = self._generate_pod_url(pod_name)
        payload = await self._request(method="GET", url=url)
        return PodDescriptor.from_primitive(payload)

    async def get_raw_pod(self, name: str) -> Dict[str, Any]:
        url = self._generate_pod_url(name)
        return await self._request(method="GET", url=url)

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

    async def create_ingress(self, name: str) -> Ingress:
        ingress = Ingress(name=name)
        payload = await self._request(
            method="POST", url=self._ingresses_url, json=ingress.to_primitive()
        )
        return Ingress.from_primitive(payload)

    async def get_ingress(self, name: str) -> Ingress:
        url = self._generate_ingress_url(name)
        payload = await self._request(method="GET", url=url)
        return Ingress.from_primitive(payload)

    async def delete_ingress(self, name: str) -> None:
        url = self._generate_ingress_url(name)
        payload = await self._request(method="DELETE", url=url)
        self._check_status_payload(payload)

    def _check_status_payload(self, payload: Dict[str, Any]) -> None:
        if payload["kind"] == "Status":
            if payload["status"] == "Failure":
                if payload.get("reason") == "AlreadyExists":
                    raise AlreadyExistsException(payload["reason"])
                raise StatusException(payload["reason"])

    async def add_ingress_rule(self, name: str, rule: IngressRule) -> Ingress:
        # TODO (A Danshyn 06/13/18): test if does not exist already
        url = self._generate_ingress_url(name)
        headers = {"Content-Type": "application/json-patch+json"}
        patches = [{"op": "add", "path": "/spec/rules/-", "value": rule.to_primitive()}]
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

    async def delete_service(self, name: str) -> None:
        url = self._generate_service_url(name)
        payload = await self._request(method="DELETE", url=url)
        self._check_status_payload(payload)

    async def create_docker_secret(self, secret: DockerRegistrySecret) -> None:
        url = self._generate_all_secrets_url(secret.namespace)
        payload = await self._request(
            method="POST", url=url, json=secret.to_primitive()
        )
        self._check_status_payload(payload)

    async def update_docker_secret(
        self, secret: DockerRegistrySecret, create_non_existent: bool = False
    ) -> None:
        try:
            url = self._generate_secret_url(secret.name, secret.namespace)
            payload = await self._request(
                method="PUT", url=url, json=secret.to_primitive()
            )
            self._check_status_payload(payload)
        except StatusException as exc:
            if exc.args[0] != "NotFound" or not create_non_existent:
                raise

            await self.create_docker_secret(secret)

    async def delete_secret(
        self, secret_name: str, namespace_name: Optional[str] = None
    ) -> None:
        url = self._generate_secret_url(secret_name, namespace_name)
        payload = await self._request(method="DELETE", url=url)
        self._check_status_payload(payload)

    async def get_pod_events(
        self, pod_id: str, namespace: str
    ) -> List[KubernetesEvent]:
        params = {
            "fieldSelector": (
                "involvedObject.kind=Pod"
                f",involvedObject.namespace={namespace}"
                f",involvedObject.name={pod_id}"
            )
        }
        url = f"{self._api_v1_url}/namespaces/{namespace}/events"
        payload = await self._request(method="GET", url=url, params=params)
        self._check_status_payload(payload)
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
                if not pod_status.container_status.is_waiting:
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
                if pod_status.container_status.is_terminated:
                    return
                await asyncio.sleep(interval_s)

    @asynccontextmanager
    async def create_pod_container_logs_stream(
        self,
        pod_name: str,
        container_name: str,
        conn_timeout_s: float = 60 * 5,
        read_timeout_s: float = 60 * 30,
    ) -> AsyncIterator[aiohttp.StreamReader]:
        url = self._generate_pod_log_url(pod_name, container_name)
        client_timeout = aiohttp.ClientTimeout(
            connect=conn_timeout_s, sock_read=read_timeout_s
        )
        async with self._client.get(  # type: ignore
            url, timeout=client_timeout
        ) as response:
            await self._check_response_status(response)
            yield response.content

    async def _check_response_status(self, response: aiohttp.ClientResponse) -> None:
        if response.status != 200:
            payload = await response.text()
            raise KubeClientException(payload)

    async def create_default_network_policy(
        self,
        name: str,
        pod_labels: Dict[str, str],
        namespace_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        assert pod_labels
        # https://tools.ietf.org/html/rfc1918#section-3
        request_payload = {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": name},
            "spec": {
                # applying the rules below to labeled pods
                "podSelector": {"matchLabels": pod_labels},
                "policyTypes": ["Egress"],
                "egress": [
                    # allowing pods to connect to public networks only
                    {
                        "to": [
                            {
                                "ipBlock": {
                                    "cidr": "0.0.0.0/0",
                                    "except": [
                                        "10.0.0.0/8",
                                        "172.16.0.0/12",
                                        "192.168.0.0/16",
                                    ],
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
                ],
            },
        }
        url = self._generate_all_network_policies_url(namespace_name)
        payload = await self._request(method="POST", url=url, json=request_payload)
        self._check_status_payload(payload)
        return payload

    async def get_network_policy(
        self, name: str, namespace_name: Optional[str] = None
    ) -> Dict[str, Any]:
        url = self._generate_network_policy_url(name, namespace_name)
        payload = await self._request(method="GET", url=url)
        self._check_status_payload(payload)
        return payload

    async def delete_network_policy(
        self, name: str, namespace_name: Optional[str] = None
    ) -> None:
        url = self._generate_network_policy_url(name, namespace_name)
        payload = await self._request(method="DELETE", url=url)
        self._check_status_payload(payload)

    def _generate_node_proxy_url(self, name: str, port: int) -> str:
        return f"{self._api_v1_url}/nodes/{name}:{port}/proxy"

    def _generate_node_stats_summary_url(self, name: str) -> str:
        proxy_url = self._generate_node_proxy_url(name, self._kubelet_port)
        return f"{proxy_url}/stats/summary"

    async def get_pod_container_stats(
        self, pod_name: str, container_name: str
    ) -> Optional["PodContainerStats"]:
        """
        https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/apis/stats/v1alpha1/types.go
        """
        pod = await self.get_pod(pod_name)
        if not pod.node_name:
            return None
        url = self._generate_node_stats_summary_url(pod.node_name)
        try:
            payload = await self._request(method="GET", url=url)
            summary = StatsSummary(payload)
            return summary.get_pod_container_stats(
                self._namespace, pod_name, container_name
            )
        except ContentTypeError:
            logger.info("Failed to parse response", exc_info=True)
            return None


@dataclass(frozen=True)
class PodContainerStats:
    cpu: float
    memory: float
    # TODO (A Danshyn): group into a single attribute
    gpu_duty_cycle: Optional[int] = None
    gpu_memory: Optional[float] = None

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "PodContainerStats":
        cpu = payload.get("cpu", {}).get("usageNanoCores", 0) / (10 ** 9)
        memory = payload.get("memory", {}).get("workingSetBytes", 0) / (2 ** 20)  # MB
        gpu_memory = None
        gpu_duty_cycle = None
        accelerators = payload.get("accelerators") or []
        if accelerators:
            gpu_memory = sum(acc["memoryUsed"] for acc in accelerators) / (
                2 ** 20
            )  # MB
            gpu_duty_cycle_total = sum(acc["dutyCycle"] for acc in accelerators)
            gpu_duty_cycle = int(gpu_duty_cycle_total / len(accelerators))  # %
        return cls(
            cpu=cpu, memory=memory, gpu_duty_cycle=gpu_duty_cycle, gpu_memory=gpu_memory
        )


class StatsSummary:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def _find_pod_in_stats_summary(
        self, stats_summary: Dict[str, Any], namespace_name: str, name: str
    ) -> Dict[str, Any]:
        for pod_stats in stats_summary["pods"]:
            ref = pod_stats["podRef"]
            if ref["namespace"] == namespace_name and ref["name"] == name:
                return pod_stats
        return {}

    def _find_container_in_pod_stats(
        self, pod_stats: Dict[str, Any], name: str
    ) -> Dict[str, Any]:
        containers = pod_stats.get("containers") or []
        for container_stats in containers:
            if container_stats["name"] == name:
                return container_stats
        return {}

    def get_pod_container_stats(
        self, namespace_name: str, pod_name: str, container_name: str
    ) -> Optional[PodContainerStats]:
        pod_stats = self._find_pod_in_stats_summary(
            self._payload, namespace_name, pod_name
        )
        if not pod_stats:
            return None

        container_stats = self._find_container_in_pod_stats(pod_stats, container_name)
        if not container_stats:
            return None

        return PodContainerStats.from_primitive(container_stats)
