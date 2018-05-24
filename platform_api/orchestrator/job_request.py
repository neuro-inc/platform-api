import enum
from dataclasses import dataclass, field
from pathlib import PurePath
import shlex
from typing import Dict, Optional, List
import uuid
from urllib.parse import urlsplit


class JobError(Exception):
    pass


@dataclass(frozen=True)
class ContainerVolume:
    src_path: PurePath
    dst_path: PurePath
    read_only: bool = False

    @staticmethod
    def create(*args, **kwargs) -> 'ContainerVolume':
        return ContainerVolumeFactory(*args, **kwargs).create()


@dataclass(frozen=True)
class Container:
    image: str
    # TODO (A Danshyn 05/23/18): env is not integrated yet
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)

    @property
    def command_list(self) -> List[str]:
        if self.command:
            return shlex.split(self.command)
        return []


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container

    @classmethod
    def create(cls, container) -> 'JobRequest':
        job_id = str(uuid.uuid4())
        return cls(job_id, container)  # type: ignore


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'


class ContainerVolumeFactory:
    """A factory class responsible for parsing a storage URI and making sure
    that the resulting path is valid. Creates an instance of ContainerVolume.
    """
    def __init__(
            self, uri: str, *,
            src_mount_path: PurePath, dst_mount_path: PurePath,
            read_only: bool=False, scheme: str='storage'
            ) -> None:
        self._uri = uri
        self._scheme = scheme
        self._path: PurePath = PurePath('')

        self._parse_uri()

        self._read_only = read_only

        assert src_mount_path.is_absolute()
        assert dst_mount_path.is_absolute()

        self._src_mount_path: PurePath = src_mount_path
        self._dst_mount_path: PurePath = dst_mount_path

    def _parse_uri(self):
        url = urlsplit(self._uri)
        if url.scheme != self._scheme:
            raise ValueError(f'Invalid URI scheme: {self._uri}')

        path = PurePath(url.netloc + url.path)
        if path.is_absolute():
            path = path.relative_to('/')
        if '..' in path.parts:
            raise ValueError('Invalid URI path: {self._uri}')

        self._path = path

    def create(self) -> ContainerVolume:
        src_path = self._src_mount_path / self._path
        dst_path = self._dst_mount_path / self._path
        return ContainerVolume(  # type: ignore
            src_path=src_path, dst_path=dst_path,
            read_only=self._read_only)
