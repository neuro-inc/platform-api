import enum
import shlex
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import PurePath
from typing import Dict, List, Optional
from urllib.parse import urlsplit

from yarl import URL

from platform_api.config import RegistryConfig
from platform_api.resource import ResourcePoolType


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


@dataclass(frozen=True)
class ContainerVolume:
    uri: URL
    src_path: PurePath
    dst_path: PurePath
    read_only: bool = False

    @staticmethod
    def create(*args, **kwargs) -> "ContainerVolume":
        return ContainerVolumeFactory(*args, **kwargs).create()

    @classmethod
    def from_primitive(cls, payload: Dict) -> "ContainerVolume":
        kwargs = payload.copy()
        # use dct.get() for backward compatibility
        # old DB records has no src_uri field
        kwargs["uri"] = URL(kwargs.get("uri", ""))
        kwargs["src_path"] = PurePath(kwargs["src_path"])
        kwargs["dst_path"] = PurePath(kwargs["dst_path"])
        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        payload: Dict = asdict(self)
        payload["uri"] = str(payload["uri"])
        payload["src_path"] = str(payload["src_path"])
        payload["dst_path"] = str(payload["dst_path"])
        return payload


@dataclass(frozen=True)
class ContainerResources:
    cpu: float
    memory_mb: int
    gpu: Optional[int] = None
    gpu_model_id: Optional[str] = None
    shm: Optional[bool] = None

    @classmethod
    def from_primitive(cls, payload: Dict) -> "ContainerResources":
        return cls(
            cpu=payload["cpu"],
            memory_mb=payload["memory_mb"],
            gpu=payload.get("gpu"),
            gpu_model_id=payload.get("gpu_model_id"),
            shm=payload.get("shm"),
        )  # type: ignore

    def to_primitive(self) -> Dict:
        return asdict(self)

    def check_fit_into_pool_type(self, pool_type: ResourcePoolType) -> bool:
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
        return self.gpu_model_id == pool_type.gpu_model.id


@dataclass(frozen=True)
class ContainerHTTPServer:
    port: int
    health_check_path: str = "/"

    @classmethod
    def from_primitive(cls, payload) -> "ContainerHTTPServer":
        return cls(  # type: ignore
            port=payload["port"],
            health_check_path=payload.get("health_check_path") or cls.health_check_path,
        )

    def to_primitive(self) -> Dict:
        return asdict(self)


@dataclass(frozen=True)
class ContainerSSHServer:
    port: int

    @classmethod
    def from_primitive(cls, payload) -> "ContainerSSHServer":
        return cls(port=payload["port"])  # type: ignore

    def to_primitive(self) -> Dict:
        return asdict(self)


@dataclass(frozen=True)
class Container:
    image: str
    resources: ContainerResources
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)
    http_server: Optional[ContainerHTTPServer] = None
    ssh_server: Optional[ContainerSSHServer] = None

    def belongs_to_registry(self, registry_config: RegistryConfig) -> bool:
        prefix = f"{registry_config.host}/"
        return self.image.startswith(prefix)

    def to_image_uri(self, registry_config: RegistryConfig) -> URL:
        assert self.belongs_to_registry(registry_config), "Unknown registry"
        prefix = f"{registry_config.host}/"
        repo = self.image.replace(prefix, "", 1)
        uri = URL(f"image://{repo}")
        path, *_ = uri.path.split(":", 1)
        return uri.with_path(path)

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

    @property
    def command_list(self) -> List[str]:
        if self.command:
            return shlex.split(self.command)
        return []

    @property
    def has_http_server_exposed(self) -> bool:
        return bool(self.http_server)

    @property
    def has_ssh_server_exposed(self) -> bool:
        return bool(self.ssh_server)

    @classmethod
    def from_primitive(cls, payload) -> "Container":
        kwargs = payload.copy()
        kwargs["resources"] = ContainerResources.from_primitive(kwargs["resources"])
        kwargs["volumes"] = [
            ContainerVolume.from_primitive(item) for item in kwargs["volumes"]
        ]

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

        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        payload: Dict = asdict(self)
        payload["resources"] = self.resources.to_primitive()
        payload["volumes"] = [volume.to_primitive() for volume in self.volumes]
        if self.http_server:
            payload["http_server"] = self.http_server.to_primitive()
        if self.ssh_server:
            payload["ssh_server"] = self.ssh_server.to_primitive()
        return payload


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container
    job_name: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def create(
        cls,
        container: Container,
        job_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> "JobRequest":
        return cls(
            job_id=f"job-{uuid.uuid4()}",
            job_name=job_name,
            description=description,
            container=container,
        )  # type: ignore

    @classmethod
    def from_primitive(cls, payload: Dict) -> "JobRequest":
        kwargs = payload.copy()
        kwargs["container"] = Container.from_primitive(kwargs["container"])
        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        result = {"job_id": self.job_id, "container": self.container.to_primitive()}
        if self.job_name:
            result["job_name"] = self.job_name
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
        return [item.value for item in cls]  # type: ignore


class ContainerVolumeFactory:
    """A factory class for :class:`ContainerVolume`.

    Responsible for parsing the specified storage URI and making sure
    that the resulting path is valid.
    """

    def __init__(
        self,
        uri: str,
        *,
        src_mount_path: PurePath,
        dst_mount_path: PurePath,
        extend_dst_mount_path: bool = True,
        read_only: bool = False,
        scheme: str = "storage",
    ) -> None:
        """Check constructor parameters and initialize the factory instance.

        :param bool extend_dst_mount_path:
            If True, append the parsed path from the URI to `dst_mount_path`,
            otherwise use `dst_mount_path` as is. Defaults to True.
        """
        self._uri = uri
        self._scheme = scheme
        self._path: PurePath = PurePath("")

        self._parse_uri()

        self._read_only = read_only

        self._check_mount_path(src_mount_path)
        self._check_mount_path(dst_mount_path)

        self._src_mount_path: PurePath = src_mount_path
        self._dst_mount_path: PurePath = dst_mount_path
        self._extend_dst_mount_path = extend_dst_mount_path

    def _parse_uri(self):
        url = urlsplit(self._uri)
        if url.scheme != self._scheme:
            raise ValueError(f"Invalid URI scheme: {self._uri}")

        path = PurePath(url.netloc + url.path)
        if path.is_absolute():
            path = path.relative_to("/")
        self._check_dots_in_path(path)

        self._path = path

    def _check_dots_in_path(self, path: PurePath) -> None:
        if ".." in path.parts:
            raise ValueError(f"Invalid path: {path}")

    def _check_mount_path(self, path: PurePath) -> None:
        if not path.is_absolute():
            raise ValueError(f"Mount path must be absolute: {path}")
        self._check_dots_in_path(path)

    def create(self) -> ContainerVolume:
        src_path = self._src_mount_path / self._path
        dst_path = self._dst_mount_path
        if self._extend_dst_mount_path:
            dst_path /= self._path
        return ContainerVolume(  # type: ignore
            uri=URL(self._uri),
            src_path=src_path,
            dst_path=dst_path,
            read_only=self._read_only,
        )
