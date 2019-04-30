import enum
import uuid
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit

from yarl import URL

from platform_api.orchestrator.kube_client import Container, ContainerVolume


class JobException(Exception):
    pass


class JobError(JobException):
    pass


class JobNotFoundException(JobException):
    pass


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

    def _parse_uri(self) -> None:
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
        return ContainerVolume(
            uri=URL(self._uri),
            src_path=src_path,
            dst_path=dst_path,
            read_only=self._read_only,
        )
