import enum
import shlex
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence, Union

from yarl import URL

from platform_api.resource import Preset, ResourcePoolType


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


class JobAlreadyExistsException(JobException):
    pass


@dataclass(frozen=True)
class ContainerVolume:
    uri: URL
    dst_path: PurePath
    read_only: bool = False

    @property
    def src_path(self) -> PurePath:
        return PurePath(URL(self.uri).path)

    @classmethod
    def create(
        cls, uri: str, dst_path: PurePath, read_only: bool = False
    ) -> "ContainerVolume":
        return cls(uri=URL(uri), dst_path=dst_path, read_only=read_only)

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerVolume":
        return cls(
            uri=URL(payload.get("uri", "")),
            dst_path=PurePath(payload["dst_path"]),
            read_only=payload["read_only"],
        )

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = asdict(self)
        payload["uri"] = str(payload["uri"])
        payload["dst_path"] = str(payload["dst_path"])
        return payload


@dataclass(frozen=True)
class Disk:
    disk_id: str  # `disk-id` in `disk://cluster/user/disk-id`
    # `user` in `disk://cluster/user/disk-id`
    # `user/with/slash` in `disk://cluster/user/with/slash/disk-id`
    # `org/user` in `disk://cluster/org/user/disk-id
    path: str
    cluster_name: str  # `cluster` in `disk://cluster/user/disk-id`

    def to_uri(self) -> URL:
        return (
            URL.build(scheme="disk", host=self.cluster_name) / self.path / self.disk_id
        )

    @classmethod
    def create(cls, disk_uri: Union[str, URL]) -> "Disk":
        # Note: format of `disk_uri` is enforced by validators
        uri = URL(disk_uri)
        cluster_name = uri.host
        assert cluster_name, uri  # for lint
        path, _, disk_id = uri.path.lstrip("/").rpartition("/")
        return cls(disk_id=disk_id, cluster_name=cluster_name, path=path)


@dataclass(frozen=True)
class DiskContainerVolume:
    disk: Disk
    dst_path: PurePath
    read_only: bool = False

    def to_uri(self) -> URL:
        return self.disk.to_uri()

    @classmethod
    def create(
        cls, uri: str, dst_path: PurePath, read_only: bool = False
    ) -> "DiskContainerVolume":
        return cls(disk=Disk.create(uri), dst_path=dst_path, read_only=read_only)

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "DiskContainerVolume":
        return cls.create(
            uri=payload["src_disk_uri"],
            dst_path=PurePath(payload["dst_path"]),
            read_only=payload["read_only"],
        )

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "src_disk_uri": str(self.to_uri()),
            "dst_path": str(self.dst_path),
            "read_only": self.read_only,
        }


@dataclass(frozen=True)
class Secret:
    secret_key: str  # `sec` in `secret://cluster/user/sec`
    # `user` in `secret://cluster/user/sec`
    # `user/with/slash` in `secret://cluster/user/with/slash/sec`
    # `org/user` in `secret://cluster/org/user/sec
    path: str
    cluster_name: str  # `cluster` in `secret://cluster/user/sec`

    @property
    def k8s_secret_name(self) -> str:
        return f"user--{self.path.replace('/', '--')}--secrets"

    def to_uri(self) -> URL:
        return (
            URL.build(scheme="secret", host=self.cluster_name)
            / self.path
            / self.secret_key
        )

    @classmethod
    def create(cls, secret_uri: Union[str, URL]) -> "Secret":
        # Note: format of `secret_uri` is enforced by validators
        uri = URL(secret_uri)
        cluster_name = uri.host
        assert cluster_name, uri  # for lint
        path, _, secret_key = uri.path.lstrip("/").rpartition("/")
        return cls(secret_key=secret_key, cluster_name=cluster_name, path=path)


@dataclass(frozen=True)
class SecretContainerVolume:
    secret: Secret
    dst_path: PurePath

    def to_uri(self) -> URL:
        return self.secret.to_uri()

    @classmethod
    def create(cls, uri: str, dst_path: PurePath) -> "SecretContainerVolume":
        return cls(secret=Secret.create(uri), dst_path=dst_path)

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "SecretContainerVolume":
        return cls.create(
            uri=payload["src_secret_uri"], dst_path=PurePath(payload["dst_path"])
        )

    def to_primitive(self) -> Dict[str, Any]:
        return {
            "src_secret_uri": str(self.to_uri()),
            "dst_path": str(self.dst_path),
        }


@dataclass(frozen=True)
class ContainerTPUResource:
    type: str
    software_version: str

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerTPUResource":
        return cls(type=payload["type"], software_version=payload["software_version"])

    def to_primitive(self) -> Dict[str, Any]:
        return {"type": self.type, "software_version": self.software_version}


@dataclass(frozen=True)
class ContainerResources:
    cpu: float
    memory_mb: int
    gpu: Optional[int] = None
    gpu_model_id: Optional[str] = None
    shm: Optional[bool] = None
    tpu: Optional[ContainerTPUResource] = None

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerResources":
        tpu = None
        if payload.get("tpu"):
            tpu = ContainerTPUResource.from_primitive(payload["tpu"])
        return cls(
            cpu=payload["cpu"],
            memory_mb=payload["memory_mb"],
            gpu=payload.get("gpu"),
            gpu_model_id=payload.get("gpu_model_id"),
            shm=payload.get("shm"),
            tpu=tpu,
        )

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"cpu": self.cpu, "memory_mb": self.memory_mb}
        if self.gpu is not None:
            payload["gpu"] = self.gpu
            payload["gpu_model_id"] = self.gpu_model_id
        if self.shm is not None:
            payload["shm"] = self.shm
        if self.tpu:
            payload["tpu"] = self.tpu.to_primitive()
        return payload

    def check_fit_into_pool_type(self, pool_type: ResourcePoolType) -> bool:
        if not pool_type.available_cpu or not pool_type.available_memory_mb:
            return False
        return (
            self.cpu <= pool_type.available_cpu
            and self.memory_mb <= pool_type.available_memory_mb
            and self._check_gpu(pool_type)
            and self._check_tpu(pool_type)
        )

    def check_fit_into_preset(self, preset: Preset) -> bool:
        return (
            self.cpu <= preset.cpu
            and self.memory_mb <= preset.memory_mb
            and self._check_gpu(preset)
            and self._check_tpu_preset(preset)
        )

    def _check_gpu(self, entry: Union[ResourcePoolType, Preset]) -> bool:
        if not self.gpu:
            # container does not need GPU. we are good regardless of presence
            # of GPU in the pool type.
            return True

        # container needs GPU

        if not entry.gpu:
            return False

        if entry.gpu < self.gpu:
            return False

        if not self.gpu_model_id:
            # container needs any GPU model
            return True

        assert entry.gpu_model
        return self.gpu_model_id == entry.gpu_model

    def _check_tpu(self, pool_type: ResourcePoolType) -> bool:
        if not self.tpu:
            # container does not need TPU. we are good regardless of presence
            # of TPU in the pool type.
            return True

        # container needs TPU

        if not pool_type.tpu:
            return False

        return (
            self.tpu.type in pool_type.tpu.types
            and self.tpu.software_version in pool_type.tpu.software_versions
        )

    def _check_tpu_preset(self, preset: Preset) -> bool:
        if not self.tpu:
            # container does not need TPU. we are good regardless of presence
            # of TPU in the pool type.
            return True

        # container needs TPU

        if not preset.tpu:
            return False

        return (
            self.tpu.type == preset.tpu.type
            and self.tpu.software_version == preset.tpu.software_version
        )


@dataclass(frozen=True)
class ContainerHTTPServer:
    port: int
    health_check_path: str = "/"
    requires_auth: bool = False

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerHTTPServer":
        return cls(
            port=payload["port"],
            health_check_path=payload.get("health_check_path") or cls.health_check_path,
            requires_auth=payload.get("requires_auth", cls.requires_auth),
        )

    def to_primitive(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Container:
    image: str
    resources: ContainerResources
    entrypoint: Optional[str] = None
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)
    secret_env: Dict[str, Secret] = field(default_factory=dict)
    secret_volumes: List[SecretContainerVolume] = field(default_factory=list)
    disk_volumes: List[DiskContainerVolume] = field(default_factory=list)
    http_server: Optional[ContainerHTTPServer] = None
    tty: bool = False
    working_dir: Optional[str] = None

    def belongs_to_registry(self, registry_host: str) -> bool:
        prefix = f"{registry_host}/"
        return self.image.startswith(prefix)

    def to_image_uri(self, registry_host: str, cluster_name: str) -> URL:
        assert self.belongs_to_registry(registry_host), "Unknown registry"
        prefix = f"{registry_host}/"
        repo = self.image[len(prefix) :]
        path, *_ = repo.split(":", 1)
        assert cluster_name
        return URL.build(scheme="image", host=cluster_name) / path

    def get_secrets(self) -> List[Secret]:
        return list(
            {*self.secret_env.values(), *(v.secret for v in self.secret_volumes)}
        )

    def get_path_to_secrets(self) -> Dict[str, List[Secret]]:
        path_to_secrets: Dict[str, List[Secret]] = defaultdict(list)
        for secret in self.get_secrets():
            path_to_secrets[secret.path].append(secret)
        return path_to_secrets

    def get_path_to_secret_volumes(self) -> Dict[str, List[SecretContainerVolume]]:
        user_volumes: Dict[str, List[SecretContainerVolume]] = defaultdict(list)
        for volume in self.secret_volumes:
            user_volumes[volume.secret.path].append(volume)
        return user_volumes

    def get_secret_uris(self) -> Sequence[URL]:
        env_uris = [sec.to_uri() for sec in self.secret_env.values()]
        vol_uris = [vol.to_uri() for vol in self.secret_volumes]
        return list(set(env_uris + vol_uris))

    @property
    def port(self) -> Optional[int]:
        if self.http_server:
            return self.http_server.port
        return None

    @property
    def health_check_path(self) -> str:
        if self.http_server:
            return self.http_server.health_check_path
        return ContainerHTTPServer.health_check_path

    def _parse_command(self, command: str) -> List[str]:
        try:
            return shlex.split(command)
        except ValueError:
            raise JobError("invalid command format")

    @property
    def entrypoint_list(self) -> List[str]:
        if self.entrypoint:
            return self._parse_command(self.entrypoint)
        return []

    @property
    def command_list(self) -> List[str]:
        if self.command:
            return self._parse_command(self.command)
        return []

    @property
    def has_http_server_exposed(self) -> bool:
        return bool(self.http_server)

    @property
    def requires_http_auth(self) -> bool:
        return bool(self.http_server and self.http_server.requires_auth)

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "Container":
        kwargs = payload.copy()
        kwargs["resources"] = ContainerResources.from_primitive(kwargs["resources"])
        kwargs["volumes"] = [
            ContainerVolume.from_primitive(item) for item in kwargs["volumes"]
        ]
        kwargs["secret_volumes"] = [
            SecretContainerVolume.from_primitive(item)
            for item in kwargs.get("secret_volumes", [])
        ]
        kwargs["disk_volumes"] = [
            DiskContainerVolume.from_primitive(item)
            for item in kwargs.get("disk_volumes", [])
        ]
        kwargs["secret_env"] = {
            key: Secret.create(value)
            for key, value in kwargs.get("secret_env", {}).items()
        }

        if kwargs.get("http_server"):
            kwargs["http_server"] = ContainerHTTPServer.from_primitive(
                kwargs["http_server"]
            )
        elif kwargs.get("port") is not None:
            kwargs["http_server"] = ContainerHTTPServer.from_primitive(kwargs)

        kwargs.pop("port", None)
        kwargs.pop("health_check_path", None)

        # previous jobs still have ssh_server stored in database
        kwargs.pop("ssh_server", None)

        # NOTE: `entrypoint` is not not serialized if it's `None` (see issue #804)
        if "entrypoint" not in kwargs:
            kwargs["entrypoint"] = None

        return cls(**kwargs)

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = asdict(self)
        payload["resources"] = self.resources.to_primitive()
        payload["volumes"] = [volume.to_primitive() for volume in self.volumes]

        secret_volumes = [v.to_primitive() for v in self.secret_volumes]
        if secret_volumes:
            payload["secret_volumes"] = secret_volumes
        else:
            payload.pop("secret_volumes")

        disk_volumes = [v.to_primitive() for v in self.disk_volumes]
        if disk_volumes:
            payload["disk_volumes"] = disk_volumes
        else:
            payload.pop("disk_volumes")

        payload.pop("secret_env", None)
        if self.secret_env:
            payload["secret_env"] = {
                name: str(sec.to_uri()) for name, sec in self.secret_env.items()
            }

        if self.http_server:
            payload["http_server"] = self.http_server.to_primitive()

        # NOTE: not to serialize `entrypoint` if it's `None` (see issue #804)
        entrypoint = payload.get("entrypoint")
        if entrypoint is None:
            payload.pop("entrypoint", None)

        if payload["working_dir"] is None:
            del payload["working_dir"]

        return payload


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container
    description: Optional[str] = None

    @classmethod
    def create(
        cls, container: Container, description: Optional[str] = None
    ) -> "JobRequest":
        return cls(
            job_id=f"job-{uuid.uuid4()}", description=description, container=container
        )

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "JobRequest":
        kwargs = payload.copy()
        kwargs["container"] = Container.from_primitive(kwargs["container"])
        return cls(**kwargs)

    def to_primitive(self) -> Dict[str, Any]:
        result = {"job_id": self.job_id, "container": self.container.to_primitive()}
        if self.description:
            result["description"] = self.description
        return result


class JobStatus(str, enum.Enum):
    """An Enum subclass that represents job statuses.

    PENDING: a job is being created and scheduled. This includes finding (and
    possibly waiting for) sufficient amount of resources, pulling an image
    from a registry etc.
    RUNNING: a job is being run.
    SUCCEEDED: a job terminated with the 0 exit code.
    CANCELLED: a running job was manually terminated/deleted.
    FAILED: a job terminated with a non-0 exit code.
    """

    PENDING = "pending"
    SUSPENDED = "suspended"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @property
    def is_pending(self) -> bool:
        return self == self.PENDING

    @property
    def is_running(self) -> bool:
        return self == self.RUNNING

    @property
    def is_finished(self) -> bool:
        return self in (self.SUCCEEDED, self.FAILED, self.CANCELLED)

    @classmethod
    def values(cls) -> List[str]:
        return [item.value for item in cls]

    @classmethod
    def active_values(cls) -> List[str]:
        return [item.value for item in cls if not item.is_finished]

    @classmethod
    def finished_values(cls) -> List[str]:
        return [item.value for item in cls if item.is_finished]

    def __repr__(self) -> str:
        return f"JobStatus.{self.name}"

    def __str__(self) -> str:
        return self.value
