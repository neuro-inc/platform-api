import enum
from dataclasses import dataclass, field, asdict
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

    @classmethod
    def from_primitive(cls, payload: Dict) -> 'ContainerVolume':
        kwargs = payload.copy()
        kwargs['src_path'] = PurePath(kwargs['src_path'])
        kwargs['dst_path'] = PurePath(kwargs['dst_path'])
        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        payload = asdict(self)
        payload['src_path'] = str(payload['src_path'])
        payload['dst_path'] = str(payload['dst_path'])
        return payload


@dataclass(frozen=True)
class ContainerResources:
    cpu: float
    memory_mb: int
    gpu: Optional[int] = None

    @classmethod
    def from_primitive(cls, payload: Dict) -> 'ContainerResources':
        return cls(**payload)  # type: ignore

    def to_primitive(self) -> Dict:
        return asdict(self)


@dataclass(frozen=True)
class Container:
    image: str
    resources: ContainerResources
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)
    port: Optional[int] = None
    health_check_path: str = '/'

    @property
    def command_list(self) -> List[str]:
        if self.command:
            return shlex.split(self.command)
        return []

    @property
    def has_http_server_exposed(self) -> bool:
        return bool(self.port)

    @classmethod
    def from_primitive(cls, payload) -> 'Container':
        kwargs = payload.copy()
        kwargs['resources'] = ContainerResources.from_primitive(
            kwargs['resources'])
        kwargs['volumes'] = [
            ContainerVolume.from_primitive(item)
            for item in kwargs['volumes']]
        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        payload = asdict(self)
        payload['resources'] = self.resources.to_primitive()
        payload['volumes'] = [
            volume.to_primitive() for volume in self.volumes]
        return payload


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container

    @classmethod
    def create(cls, container) -> 'JobRequest':
        job_id = f'job-{uuid.uuid4()}'
        return cls(job_id, container)  # type: ignore

    @classmethod
    def from_primitive(cls, payload: Dict) -> 'JobRequest':
        kwargs = payload.copy()
        kwargs['container'] = Container.from_primitive(kwargs['container'])
        return cls(**kwargs)  # type: ignore

    def to_primitive(self) -> Dict:
        return {
            'job_id': self.job_id,
            'container': self.container.to_primitive(),
        }


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'

    @property
    def is_finished(self) -> bool:
        return self in (self.SUCCEEDED, self.FAILED)

    @classmethod
    def values(cls) -> List[str]:
        return [item.value for item in cls]


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
