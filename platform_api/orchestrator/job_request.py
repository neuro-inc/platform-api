import enum
import shlex
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Sequence, Union
from urllib.parse import urlsplit

from yarl import URL

from platform_api.cluster_config import RegistryConfig
from platform_api.resource import ResourcePoolType


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
    src_path: PurePath
    dst_path: PurePath
    read_only: bool = False

    @staticmethod
    def create(uri: str, *args: Any, **kwargs: Any) -> "ContainerVolume":
        return ContainerVolumeFactory(uri, *args, **kwargs).create()

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerVolume":
        kwargs = payload.copy()
        # use dct.get() for backward compatibility
        # old DB records has no src_uri field
        kwargs["uri"] = URL(kwargs.get("uri", ""))
        kwargs["src_path"] = PurePath(kwargs["src_path"])
        kwargs["dst_path"] = PurePath(kwargs["dst_path"])
        return cls(**kwargs)

    def to_primitive(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = asdict(self)
        payload["uri"] = str(payload["uri"])
        payload["src_path"] = str(payload["src_path"])
        payload["dst_path"] = str(payload["dst_path"])
        return payload


@dataclass(frozen=True)
class Secret:
    secret_name: str  # `sec` in `secret://cluster/user/sec`
    user_name: str  # `user` in `secret://cluster/user/sec`
    cluster_name: str  # `cluster` in `secret://cluster/user/sec`

    @property
    def k8s_secret_name(self) -> str:
        return f"user--{self.user_name}--secrets"

    def to_uri(self) -> URL:
        return URL(f"secret://{self.cluster_name}/{self.user_name}/{self.secret_name}")

    @classmethod
    def create(cls, secret_uri: Union[str, URL]) -> "Secret":
        assert isinstance(secret_uri, (str, URL)), (type(secret_uri), secret_uri)
        uri = URL(secret_uri)
        cluster_name = uri.host
        assert cluster_name, f"invalid secret URI: no cluster name: '{uri}'"

        parts = PurePath(uri.path).parts
        assert len(parts) == 3, parts  # TODO: validation
        assert parts[0] == "/", parts
        user_name, secret_name = parts[1], parts[2]

        return cls(
            secret_name=secret_name, cluster_name=cluster_name, user_name=user_name
        )


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
            uri=payload["src_secret_uri"], dst_path=PurePath(payload["dst_path"]),
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
        return self._check_gpu(pool_type) and self._check_tpu(pool_type)

    def _check_gpu(self, pool_type: ResourcePoolType) -> bool:
        if not self.gpu:
            # container does not need GPU. we are good regardless of presence
            # of GPU in the pool type.
            return True

        # container needs GPU

        if not pool_type.gpu:
            return False

        if pool_type.gpu < self.gpu:
            return False

        if not self.gpu_model_id:
            # container needs any GPU model
            return True

        assert pool_type.gpu_model
        return self.gpu_model_id == pool_type.gpu_model

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
class ContainerSSHServer:
    port: int

    @classmethod
    def from_primitive(cls, payload: Dict[str, Any]) -> "ContainerSSHServer":
        return cls(port=payload["port"])

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
    http_server: Optional[ContainerHTTPServer] = None
    ssh_server: Optional[ContainerSSHServer] = None
    tty: bool = False

    def belongs_to_registry(self, registry_config: RegistryConfig) -> bool:
        prefix = f"{registry_config.host}/"
        return self.image.startswith(prefix)

    def to_image_uri(self, registry_config: RegistryConfig, cluster_name: str) -> URL:
        assert self.belongs_to_registry(registry_config), "Unknown registry"
        prefix = f"{registry_config.host}/"
        repo = self.image.replace(prefix, "", 1)
        path, *_ = repo.split(":", 1)
        assert cluster_name
        return URL(f"image://{cluster_name}/{path}")

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
    def ssh_port(self) -> Optional[int]:
        if self.ssh_server:
            return self.ssh_server.port
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

        if kwargs.get("ssh_server"):
            ssh_server_desc = kwargs["ssh_server"]
            container_desc = ContainerSSHServer.from_primitive(ssh_server_desc)
            kwargs["ssh_server"] = container_desc

        kwargs.pop("port", None)
        kwargs.pop("health_check_path", None)

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

        payload.pop("secret_env", None)
        if self.secret_env:
            payload["secret_env"] = {
                name: str(sec.to_uri()) for name, sec in self.secret_env.items()
            }

        if self.http_server:
            payload["http_server"] = self.http_server.to_primitive()
        if self.ssh_server:
            payload["ssh_server"] = self.ssh_server.to_primitive()

        # NOTE: not to serialize `entrypoint` if it's `None` (see issue #804)
        entrypoint = payload.get("entrypoint", None)
        if entrypoint is None:
            payload.pop("entrypoint", None)

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
    SUCCEEDED: a job terminated with the 0 exit code or a running job was
    manually terminated/deleted.
    FAILED: a job terminated with a non-0 exit code.
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_pending(self) -> bool:
        return self == self.PENDING

    @property
    def is_running(self) -> bool:
        return self == self.RUNNING

    @property
    def is_finished(self) -> bool:
        return self in (self.SUCCEEDED, self.FAILED)

    @classmethod
    def values(cls) -> List[str]:
        return [item.value for item in cls]

    def __repr__(self) -> str:
        return f"JobStatus.{self.name}"

    def __str__(self) -> str:
        return self.value


class ContainerVolumeFactory:
    def __init__(
        self,
        uri: str,
        *,
        src_mount_path: PurePath,
        dst_mount_path: PurePath,
        extend_dst_mount_path: bool = True,
        read_only: bool = False,
    ) -> None:
        """Check constructor parameters and initialize the factory instance.

        :param bool extend_dst_mount_path:
            If True, append the parsed path from the URI to `dst_mount_path`,
            otherwise use `dst_mount_path` as is. Defaults to True.
        """
        self._uri = uri
        path = PurePath(urlsplit(uri).path)
        if path.is_absolute():
            path = path.relative_to("/")
        self._path = path

        self._read_only = read_only

        self._src_mount_path: PurePath = src_mount_path
        self._dst_mount_path: PurePath = dst_mount_path
        self._extend_dst_mount_path = extend_dst_mount_path

    def create(self) -> ContainerVolume:
        src_path = self._src_mount_path / self._path
        dst_path = self._dst_mount_path
        if self._extend_dst_mount_path:
            dst_path /= self._path
        return ContainerVolume(
            uri=URL(self._uri),
            src_path=src_path,
            dst_path=dst_path,
            read_only=self._read_only,
        )
