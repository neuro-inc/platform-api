import enum
from dataclasses import dataclass, field
from typing import Dict, Optional, List


class JobError(Exception):
    pass


@dataclass(frozen=True)
class ContainerVolume:
    src_path: str
    dst_path: str
    read_only: bool = False


@dataclass(frozen=True)
class Container:
    image: str
    # TODO (A Danshyn 05/23/18): command and env are not integrated yet
    command: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    volumes: List[ContainerVolume] = field(default_factory=list)


@dataclass(frozen=True)
class JobRequest:
    job_id: str
    container: Container


class JobStatus(str, enum.Enum):
    PENDING = 'pending'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
